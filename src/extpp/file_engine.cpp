#include <extpp/file_engine.hpp>

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/block_index.hpp>
#include <extpp/math.hpp>

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <extpp/detail/rollback.hpp>

#include <exception>
#include <iosfwd>
#include <type_traits>

namespace extpp {

namespace {

struct block;
struct block_cache;
struct block_dirty_set;
struct block_map;
struct block_pool;

// TODO: Put these classes into a private header so they can be tested again?

/// A block instance represents a block from disk
/// that has been loaded into memory.
///
/// Blocks are reference counted: if their refcount
/// drops to zero, they will be flushed to disk.
/// Note that they will only be  written if they have been
/// marked as dirty prior to their destruction.
///
/// All instances with a refcount greater than zero
/// are stored in an (intrusive) hash map,
/// indexed by their block index. This enables us to always
/// return the same instance for the same index.
/// Recently used blocks are additionally kept in a LRU-Cache.
///
/// Block instances that have been "destroyed" (i.e. their refcount has become zero)
/// are linked to together in a free list and will be reused for
/// future allocations.
struct block final : public detail::block_handle_impl {
public:
    /// The engine this block belongs to.
    file_engine_impl* const m_engine;

    /// Number of references to this block.
    u32 m_refcount = 0;

    /// Used by the free list in block_pool.
    boost::intrusive::list_member_hook<> m_free_hook;

    /// Used by the block_cache.
    boost::intrusive::list_member_hook<> m_lru_hook;

    /// Used by the block_map.
    boost::intrusive::unordered_set_member_hook<> m_map_hook;

    /// Marks the block as dirty and links all dirty blocks together
    /// in a list. Used by the block_dirty_set.
    boost::intrusive::list_member_hook<> m_dirty_hook;

    explicit block(file_engine_impl* engine, u32 block_size)
        : m_engine(engine)
    {
        m_block_size = block_size;
        m_data = static_cast<byte*>(std::malloc(block_size));
        if (!m_data)
            throw std::bad_alloc();
    }

    ~block() {
        free(m_data);
    }

    /// Puts the block into a state where it can be reused.
    void reset() noexcept {
        // The block must not be in any of the containers
        // when it is being reset.
        EXTPP_ASSERT(!m_free_hook.is_linked(), "in free list");
        EXTPP_ASSERT(!m_lru_hook.is_linked(), "in lru list");
        EXTPP_ASSERT(!m_map_hook.is_linked(), "in block map");
        EXTPP_ASSERT(!m_dirty_hook.is_linked(), "in dirty list");

        m_index = 0;
        m_writable = false;
        // Not zeroing the data array because it will
        // be overwritten by a read() anyway.
    }

    void ref() noexcept {
        ++m_refcount;
        EXTPP_ASSERT(m_refcount >= 1, "invalid refcount");
    }

    inline void unref() noexcept;

    /// Marks this block as dirty.
    inline void set_dirty() noexcept;

    block(const block&) = delete;
    block& operator=(const block&) = delete;

public:
    // Interface implementation (detail::block_handle_impl)

    virtual block* copy() override {
        ref();
        return this;
    }

    virtual void destroy() override {
        unref();
    }

    virtual void make_writable() override {
        set_dirty();
        m_writable = true;
    }

    friend void intrusive_ptr_add_ref(block* b) {
        b->ref();
    }

    friend void intrusive_ptr_release(block* b) {
        b->unref();
    }
};

/// Keeps the N most recently used blocks in a linked list.
/// Membership in the cache counts as an additional reference,
/// keeping the block alive for further use.
struct block_cache {
private:
    using list_t = boost::intrusive::list<
        block,
        boost::intrusive::member_hook<
            block, boost::intrusive::list_member_hook<>, &block::m_lru_hook
        >
    >;

private:
    /// Maximum number of cached blocks.
    u32 m_max_size;

    /// Linked list of cached blocks (intrusive).
    /// The most recently used block is at the front.
    list_t m_list;

public:
    /// Construct a new cache with the given capacity.
    explicit block_cache(u32 max_size) noexcept;

    ~block_cache();

    void clear() noexcept;

    /// True if the block is in the cache.
    bool contains(block& blk) const noexcept {
        return blk.m_lru_hook.is_linked();
    }

    /// Marks the block as used, inserting it into the cache
    /// if necessary. The block will be at the head of the list.
    /// If the  cache is full, the least recently used block
    /// will be removed.
    void use(block& blk) noexcept;

    u32 max_size() const noexcept { return m_max_size; }

    u32 size() const noexcept { return static_cast<u32>(m_list.size()); }

    block_cache(const block_cache&) = delete;
    block_cache& operator=(const block_cache&) = delete;

private:
    void insert(block& blk) noexcept;

    static void dispose(block* blk) noexcept;
};

/// A hash map that indexes all used blocks.
/// Blocks can be retrieved by specifying their block index.
///
/// Once a block's reference count reaches zero,
/// it will be removed from this map.
/// Note: Membership does *not* count towards the ref count.
struct block_map {
    // Blocks are indexed by their block index.
    struct block_key {
        using type = u64;

        u64 operator()(const block& blk) const noexcept {
            return blk.index();
        }
    };

    struct block_index_hash {
        size_t operator()(u64 index) const noexcept {
            return boost::hash_value(index);
        }
    };

    using map_t = boost::intrusive::unordered_set<
        block,
        boost::intrusive::member_hook<
            block,
            boost::intrusive::unordered_set_member_hook<>,
            &block::m_map_hook
        >,
        boost::intrusive::power_2_buckets<true>,
        boost::intrusive::key_of_value<block_key>,
        boost::intrusive::hash<block_index_hash>
    >;

    using bucket_t = typename map_t::bucket_type;

private:
    /// Holds the buckets for the intrusive map.
    std::vector<bucket_t> m_buckets;

    /// Maps block indices to block instances.
    map_t m_map;

public:
    /// Constructs a block map with the given size hint.
    ///
    /// \param expected_load
    ///     The approximate expected worst case load.
    ///     This number is used to compute an appropriate
    ///     number of hash table buckets.
    explicit block_map(size_t expected_load);

    ~block_map();

    auto begin() noexcept { return m_map.begin(); }

    auto end() noexcept { return m_map.end(); }

    void clear() noexcept;

    /// Insert a block into the map.
    /// Note: does *not* affect the block's reference count.
    ///
    /// \pre The block must not be stored already.
    /// \pre The block's index must be unique.
    void insert(block& blk) noexcept;

    /// Removes a block from the map.
    /// Note: does *not* affect the block's reference count.
    ///
    /// \pre `contains(blk)`.
    void remove(block& blk) noexcept;

    /// Finds the block with the given index and returns it.
    /// Returns nullptr if no such block was found.
    block* find(u64 index) const noexcept;

    /// Returns true if the block is inside this map.
    bool contains(block& blk) const noexcept {
        return blk.m_map_hook.is_linked();
    }

    /// Returns the number of blocks in this map.
    u32 size() const noexcept { return static_cast<u32>(m_map.size()); }

    block_map(const block_map&) = delete;
    block_map& operator=(const block_map&) = delete;
};

/// Stores reuseable block instances.
class block_pool {
    using list_t = boost::intrusive::list<
        block,
        boost::intrusive::member_hook<
            block, boost::intrusive::list_member_hook<>, &block::m_free_hook
        >
    >;

private:
    /// List of free blocks.
    list_t m_list;

public:
    block_pool();

    ~block_pool();

    /// Add a block to the pool for future use.
    /// The pool takes ownership of this block.
    /// The block must not already be in this pool.
    ///
    /// \pre The block's refcount must be zero.
    void add(block& blk) noexcept;

    /// Removes a single block instance from the pool.
    /// The block is owned by the caller.
    /// Returns a nullptr if the pool is empty.
    block* remove() noexcept;

    /// The number of block instances in the pool
    u32 size() const noexcept { return static_cast<u32>(m_list.size()); }

    /// True iff the pool is empty.
    bool empty() const noexcept { return m_list.empty(); }

    void clear() noexcept;

    block_pool(const block_pool&) = delete;
    block_pool& operator=(const block_pool&) = delete;

private:
    static void dispose(block* blk) noexcept;
};

class block_dirty_set {
    using list_t = boost::intrusive::list<
        block,
        boost::intrusive::member_hook<
            block, boost::intrusive::list_member_hook<>, &block::m_dirty_hook
        >
    >;

private:
    /// List of all dirty blocks.
    list_t m_list;

public:
    block_dirty_set();

    ~block_dirty_set();

    auto begin() noexcept { return m_list.begin(); }

    auto end() noexcept { return m_list.end(); }

    /// Marks the block as dirty.
    void add(block& blk) noexcept;

    /// Returns true if the block has been marked as dirty.
    bool contains(block& blk) const noexcept { return blk.m_dirty_hook.is_linked(); }

    /// Marks the block as clean.
    /// \pre `contains(blk)`.
    void remove(block& blk) noexcept;

    void clear() noexcept;

    size_t size() const { return m_list.size(); }

    block_dirty_set(const block_dirty_set&) = delete;
    block_dirty_set& operator=(const block_dirty_set&) = delete;
};

} // namespace

class file_engine_impl {
private:
    /// Underlying I/O-object.
    file* m_file;

    /// Cache size + a bit of a safety buffer
    /// if the application needs to pin 1 or more blocks.
    u32 m_capacity;

    /// Size of a single block. Must be a power of two.
    u32 m_block_size;

    /// Contains previously allocated instances that
    /// can be reused for future blocks.
    block_pool m_pool;

    /// Contains all block instances that are currently in use.
    block_map m_blocks;

    /// The block cache.
    block_cache m_cache;

    /// Manages all dirty blocks.
    block_dirty_set m_dirty;

    /// Performance metrics.
    file_engine_stats m_stats;

    /// An exception from an earlier write operation
    /// that has to be rethrown in the next read or flush operation.
    /// These exceptions have to be stored here because they cannot
    /// be reported from the block handle's (noexcept) destructor.
    std::exception_ptr m_write_error;

public:
    explicit file_engine_impl(file& fd, u32 block_size, u32 cache_size);

    ~file_engine_impl();

    file& fd() const { return *m_file; }

    u32 block_size() const noexcept { return m_block_size; }

    const file_engine_stats& stats() const noexcept { return m_stats; }

    /// Reads the block at the given address and returns a handle to it.
    /// No actual I/O is performed if the block is already in memory.
    ///
    /// In-memory blocks are managed using automatic reference counting.
    /// A block will remain accessible for as long as there are handles
    /// that refer to it.
    ///
    /// Throws if an I/O error occurs.
    boost::intrusive_ptr<block> read(u64 index);

    /// Similar to `read()`, but no actual I/O is performed when the block
    /// is not already in memory. Instead, the block's content is zeroed.
    ///
    /// This is useful for "reading" a newly allocated block, where the caller
    /// already knows that the contents can be ignored.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// \note Blocks read using this function are dirty by default,
    /// as their content does not necessarily reflect the state on disk.
    ///
    /// Throws if an I/O error occurs (this can still happen if the new block
    /// evicts an old block from the cache).
    boost::intrusive_ptr<block> overwrite_zero(u64 index);

    /// Like `overwrite(index)`, but sets the content to that of `data`.
    /// Data must be at least `block_size()` bytes long.
    boost::intrusive_ptr<block> overwrite_with(u64 index, const byte* data);

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    ///
    /// \note Flushing does *not* perform a `sync()` on the underlying file,
    /// it merely writes all pending data to the file using `write()`.
    void flush();

    file_engine_impl(const file_engine_impl&) = delete;
    file_engine_impl& operator=(const file_engine_impl&) = delete;

private:
    friend block;

    template<typename ReadAction>
    boost::intrusive_ptr<block> read_impl(u64 index, ReadAction&& read);

    void set_dirty(block& blk) noexcept;

    bool is_dirty(block& blk) const noexcept { return m_dirty.contains(blk); }

    /// Write a single block back to disk (throws).
    /// Does nothing if the block isn't marked as dirty.
    void flush_block(block& blk);

    /// Called when the refcount of a block reaches zero.
    /// The block will be written to disk (if necessary).
    /// Blocks are reused for future allocations.
    void finalize_block(block& blk) noexcept;

    /// Returns a new block instance, possibly
    /// from the free list.
    block& allocate_block();

    /// Deallocates a block (or puts it into the free list
    /// for later use).
    void free_block(block& blk) noexcept;

    /// Rethrows a stored write error.
    /// Clears the error so it won't be thrown again.
    void rethrow_write_error();
};

inline void block::unref() noexcept {
    EXTPP_ASSERT(m_refcount >= 1, "invalid refcount");
    if (--m_refcount == 0) {
        m_engine->finalize_block(*this);
    }
}

inline void block::set_dirty() noexcept {
    if (!m_engine->is_dirty(*this)) {
        m_engine->set_dirty(*this);
    }
}

// --------------------------------
//
//   block cache
//   An lru cache for blocks read from disk.
//
// --------------------------------

block_cache::block_cache(u32 max_size) noexcept
    : m_max_size(max_size)
    , m_list()
{}

block_cache::~block_cache()
{
    clear();
}

void block_cache::clear() noexcept
{
    m_list.clear_and_dispose(dispose);
}

void block_cache::use(block& blk) noexcept
{
    if (!contains(blk)) {
        insert(blk);
        return;
    }

    // Move the block to the front of the list.
    auto iter = m_list.iterator_to(blk);
    m_list.splice(m_list.begin(), m_list, iter);
}

void block_cache::insert(block& blk) noexcept
{
    EXTPP_ASSERT(!contains(blk), "must not be stored in the cache.");
    EXTPP_ASSERT(m_list.size() <= m_max_size, "invalid cache size.");

    // Add the block at the front and increment its reference count.
    m_list.push_front(blk);
    blk.ref();

    // Remove the least recently used element if the cache is full.
    while (m_list.size() > m_max_size) {
        m_list.pop_back_and_dispose(dispose);
    }
}

void block_cache::dispose(block* blk) noexcept
{
    blk->unref();
}

// --------------------------------
//
//   block map
//   Maps block indices to block instances.
//
// --------------------------------

// TODO: Bucket array should be able to grow dynamically.
block_map::block_map(size_t expected_load)
    : m_buckets(round_towards_pow2(std::max(size_t(32), (expected_load * 4) / 3)))
    , m_map(map_t::bucket_traits(m_buckets.data(), m_buckets.size()))
{}

block_map::~block_map()
{
    clear();
}

void block_map::clear() noexcept
{
    m_map.clear();
}

void block_map::insert(block& blk) noexcept
{
    EXTPP_ASSERT(!contains(blk), "block is already stored in a map.");

    bool inserted;
    std::tie(std::ignore, inserted) = m_map.insert(blk);
    EXTPP_ASSERT(inserted, "a block with that index already exists.");
}

void block_map::remove(block& blk) noexcept
{
    EXTPP_ASSERT(contains(blk), "block not stored in map.");

    m_map.erase(m_map.iterator_to(blk));
}

block* block_map::find(u64 index) const noexcept
{
    auto iter = m_map.find(index);
    if (iter == m_map.end()) {
        return nullptr;
    }

    const block& blk = *iter;
    return const_cast<block*>(&blk);
}

// --------------------------------
//
//   block pool
//   Holds reusable block instances.
//
// --------------------------------

block_pool::block_pool()
{}

block_pool::~block_pool()
{
    EXTPP_ASSERT(m_list.empty(), "Blocks in the pool must not outlive the engine.");
}

void block_pool::clear() noexcept
{
    m_list.clear_and_dispose(dispose);
}

void block_pool::add(block& blk) noexcept
{
    EXTPP_ASSERT(blk.m_refcount == 0, "block must not be referenced.");
    EXTPP_ASSERT(!blk.m_free_hook.is_linked(), "block must not be in the pool.");
    m_list.push_back(blk);
}

block* block_pool::remove() noexcept
{
    if (m_list.empty())
        return nullptr;

    block* blk = &m_list.front();
    m_list.pop_front();
    return blk;
}

void block_pool::dispose(block* blk) noexcept
{
    delete blk;
}

// --------------------------------
//
//   block dirty set
//
// --------------------------------

block_dirty_set::block_dirty_set()
{}

block_dirty_set::~block_dirty_set()
{
    clear();
}

void block_dirty_set::clear() noexcept
{
    m_list.clear();
}

void block_dirty_set::add(block& blk) noexcept
{
    if (!contains(blk)) {
        m_list.push_back(blk);
    }
}

void block_dirty_set::remove(block& blk) noexcept
{
    EXTPP_ASSERT(contains(blk), "block is not dirty");
    m_list.erase(m_list.iterator_to(blk));
}

// --------------------------------
//
//   block engine
//   Reads and writes blocks from a disk file
//   and caches them in memory.
//
// --------------------------------

file_engine_impl::file_engine_impl(file& fd, u32 block_size, u32 cache_size)
    : m_file(&fd)
    , m_capacity(cache_size + 32)
    , m_block_size(block_size)
    , m_pool()
    , m_blocks(m_capacity)
    , m_cache(cache_size)
    , m_stats()
{
    EXTPP_CHECK(is_pow2(block_size), "block size must be a power of two.");
}

file_engine_impl::~file_engine_impl()
{
#ifdef EXTPP_DEBUG
    for (block& b : m_blocks) {
        // The engine is going to be destroyed; the user must not have
        // any remaining block handles. However, the LRU cache may
        // still hold some blocks.
        EXTPP_ASSERT(b.m_refcount == 1 && m_cache.contains(b),
                    "Blocks must not be referenced when the engine is destroyed.");

    }
#endif

    // Make an attempt to flush pending IO.
    try {
        flush();
    } catch (...) {}

    // All blocks are forced to be clean.
    // Clearing the cache removes the last reference to the blocks
    // and prompts a call to finalize_block, which does no I/O
    // and puts the blocks into the pool, which deletes them all inside clear().
    m_dirty.clear();
    m_cache.clear();
    m_blocks.clear();
    m_pool.clear();
}

boost::intrusive_ptr<block> file_engine_impl::read(u64 index)
{
    return read_impl(index, [&](byte* data) {
        m_file->read(index * m_block_size, data, m_block_size);
        ++m_stats.reads;
    });
}

boost::intrusive_ptr<block> file_engine_impl::overwrite_zero(u64 index)
{
    // The read function is a no-op. Everything else is the same as in read().
    auto blk = read_impl(index, [&](byte*) {});

    std::memset(blk->m_data, 0, m_block_size);
    blk->set_dirty();
    return blk;
}

boost::intrusive_ptr<block> file_engine_impl::overwrite_with(u64 index, const byte* data)
{
    auto blk = read_impl(index, [&](byte*) {});

    std::memcpy(blk->m_data, data, m_block_size);
    blk->set_dirty();
    return blk;
}

template<typename ReadAction>
boost::intrusive_ptr<block> file_engine_impl::read_impl(u64 index, ReadAction&& read) {
    rethrow_write_error();

    if (block* blk = m_blocks.find(index)) {
        ++m_stats.cache_hits;
        return boost::intrusive_ptr<block>(blk);
    }

    block& blk = allocate_block();
    detail::rollback guard = [&]{
        free_block(blk);
    };

    EXTPP_ASSERT(blk.m_block_size == m_block_size,
                "block size invariant");
    {
        blk.m_index = index;
        read(blk.m_data);
    }
    guard.commit();

    boost::intrusive_ptr<block> result(&blk);
    m_blocks.insert(blk);
    m_cache.use(blk);       // This can cause a block write (and therefore an error) for a
                            // block that is evicted from the cache.
    rethrow_write_error();
    return result;
}

void file_engine_impl::flush()
{
    rethrow_write_error();

    for (auto i = m_dirty.begin(), e = m_dirty.end();
         i != e; )
    {
        auto n = std::next(i);
        flush_block(*i);
        i = n;
    }

    EXTPP_ASSERT(m_dirty.begin() == m_dirty.end(),
                "no dirty blocks can remain.");
}

void file_engine_impl::set_dirty(block& blk) noexcept {
    m_dirty.add(blk);
}

void file_engine_impl::flush_block(block& blk)
{
    EXTPP_ASSERT(blk.m_block_size == m_block_size,
                "block size invariant");

    if (m_dirty.contains(blk)) {
        m_file->write(blk.m_index * m_block_size, blk.m_data, m_block_size);
        m_dirty.remove(blk);
        blk.m_writable = false;
        ++m_stats.writes;
    }
}

void file_engine_impl::finalize_block(block& blk) noexcept
{
    EXTPP_ASSERT(blk.m_refcount == 0, "refcount must be zero");
    EXTPP_ASSERT(blk.m_engine == this, "block belongs to wrong engine");

    try {
        flush_block(blk);
    } catch (...) {
        // Because this function is called from a noexcept context (the block handle's
        // destructor), we don't have a place where we can report the issue.
        // Therefore we cache the error and report it at the next opportunity (read or flush).
        // TODO: block index should be stored as well. Nested exception?
        if (!m_write_error) {
            m_write_error = std::current_exception();
        }
        m_dirty.remove(blk); // XXX See comment below.
    }

    // TODO: Blocks with write errors are deleted form memory == data loss.
    // Think of a better error handling scheme. For example, the user could be notified
    // so that he can take a custom action to "rescue" the data.
    m_blocks.remove(blk);
    free_block(blk);
}

block& file_engine_impl::allocate_block()
{
    block* blk = m_pool.remove();
    if (!blk) {
        blk = new block(this, m_block_size);
    }
    return *blk;
}

// Add to pool or delete depending on the number of blocks in memory.
void file_engine_impl::free_block(block& blk) noexcept
{
    if (m_blocks.size() + m_pool.size() < m_capacity) {
        blk.reset();
        m_pool.add(blk);
    } else {
        delete &blk;
    }
}

void file_engine_impl::rethrow_write_error()
{
    if (m_write_error) {
        auto error = std::exchange(m_write_error, std::exception_ptr());
        std::rethrow_exception(std::move(error));
    }
}

file_engine::file_engine(file& fd, u32 block_size, u32 cache_size)
    : engine(block_size)
    , m_impl(std::make_unique<file_engine_impl>(fd, block_size, cache_size))
{}

file_engine::~file_engine() {}

file& file_engine::fd() const { return impl().fd(); }

file_engine_stats file_engine::stats() const { return impl().stats(); }

u64 file_engine::do_size() const {
    return fd().file_size() >> block_size_log();
}

void file_engine::do_grow(u64 n) {
    u64 new_size_blocks = checked_add(do_size(), n);
    u64 new_size_bytes = checked_mul<u64>(new_size_blocks, block_size());
    fd().truncate(new_size_bytes);
}

block_handle file_engine::do_read(block_index index) {
    EXTPP_CHECK(index, "Invalid index.");
    return block_handle(this, impl().read(index.value()).detach());
}

block_handle file_engine::do_zeroed(block_index index) {
    EXTPP_CHECK(index, "Invalid index.");
    return block_handle(this, impl().overwrite_zero(index.value()).detach());
}

block_handle file_engine::do_overwritten(block_index index, const byte* data) {
    EXTPP_CHECK(index, "Invalid index.");
    return block_handle(this, impl().overwrite_with(index.value(), data).detach());
}

void file_engine::do_flush() {
    impl().flush();
}

file_engine_impl& file_engine::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid instance.");
    return *m_impl;
}

} // namespace extpp
