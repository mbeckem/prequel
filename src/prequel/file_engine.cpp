#include <prequel/file_engine.hpp>

#include <prequel/address.hpp>
#include <prequel/assert.hpp>
#include <prequel/block_index.hpp>
#include <prequel/math.hpp>
#include <prequel/detail/deferred.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list_hook.hpp>

#include <fmt/format.h>

#include <exception>
#include <type_traits>

#ifdef PREQUEL_TRACE_IO
#   define PREQUEL_PRINT_READ(index) (fmt::print("Reading block {}\n", (index)))
#   define PREQUEL_PRINT_WRITE(index) (fmt::print("Writing block {}\n", (index)))
#else
#   define PREQUEL_PRINT_READ(index)
#   define PREQUEL_PRINT_WRITE(index)
#endif

namespace prequel {

namespace detail {

namespace {

struct block;
struct block_cache;
struct block_dirty_set;
struct block_pool;

/*
 * Represents a block loaded from disk into memory.
 * A block is either pinned or in the cache.
 *
 * Dirty blocks are written back to disk before they are removed
 * from main memory.
 */
struct block {
public:
    /// The engine this block belongs to.
    file_engine_impl* const m_engine;

    /// Index of the block within the file.
    u64 m_index = 0;

    /// Block sized data array.
    byte* m_data = nullptr;

    /// True if the block is referenced from the outside.
    /// It must not be dropped from memory until it is unpinned by the application.
    bool m_pinned = false;

    /// Used by the free list in block_pool.
    boost::intrusive::list_member_hook<> m_free_hook;

    /// Used by the block_cache.
    boost::intrusive::list_member_hook<> m_cache_hook;

    /// Used by the block_map.
    boost::intrusive::unordered_set_member_hook<> m_map_hook;

    /// Marks the block as dirty and links all dirty blocks together
    /// in an ordered sequence. Used by the block_dirty_set.
    boost::intrusive::set_member_hook<> m_dirty_hook;

public:
    explicit block(file_engine_impl* engine);

    ~block();

    block(const block&) = delete;
    block& operator=(const block&) = delete;

    /// Puts the block into a state where it can be reused.
    void reset() noexcept {
        // The block must not be in any of the containers
        // when it is being reset.
        PREQUEL_ASSERT(!m_free_hook.is_linked(), "in free list");
        PREQUEL_ASSERT(!m_cache_hook.is_linked(), "in lru list");
        PREQUEL_ASSERT(!m_map_hook.is_linked(), "in block map");
        PREQUEL_ASSERT(!m_dirty_hook.is_linked(), "in dirty list");

        m_index = 0;
        // Not zeroing the data array because it will
        // be overwritten by a read() anyway.
    }

    u64 index() const { return m_index; }
    byte* data() const { return m_data; }
    bool dirty() const { return m_dirty_hook.is_linked(); }
    bool cached() const { return m_cache_hook.is_linked(); }
    bool pinned() const { return m_pinned; }
};

// Blocks are indexed by their block index.
struct block_key {
    using type = u64;

    u64 operator()(const block& blk) const noexcept {
        return blk.index();
    }
};

/// Caches uses blocks in main memory. Blocks in the cache must not be pinned.
struct block_cache {
private:
    using list_t = boost::intrusive::list<
        block,
        boost::intrusive::member_hook<
            block, boost::intrusive::list_member_hook<>, &block::m_cache_hook
        >
    >;

private:
    /// Linked list of cached blocks (intrusive).
    /// The most recently used block is at the front.
    list_t m_list;

public:
    explicit block_cache() noexcept;

    ~block_cache();

    void clear() noexcept;

    /// True if the block is in the cache.
    bool contains(block& blk) const noexcept {
        return blk.m_cache_hook.is_linked();
    }

    /// Inserts the block into the cache. The block must not be cached already.
    void add(block& blk) noexcept;

    /// Removes the block from the cache. The block must be cached.
    void remove(block& blk) noexcept;

    /// Returns a pointer to the block that should be evicted next.
    /// Does not remove that block.
    block* lru_candidate() noexcept;

    size_t size() const noexcept { return m_list.size(); }

    block_cache(const block_cache&) = delete;
    block_cache& operator=(const block_cache&) = delete;

private:

};

/// A map that indexes all live block instances.
struct block_map {
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

    template<typename Disposer>
    void dispose(Disposer&& d);

    /// Insert a block into the map.
    ///
    /// \pre The block must not be stored already.
    /// \pre The block's index must be unique.
    void insert(block& blk) noexcept;

    /// Removes a block from the map.
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
    size_t size() const noexcept { return m_map.size(); }

    block_map(const block_map&) = delete;
    block_map& operator=(const block_map&) = delete;
};

/// Indexes dirty blocks.
struct block_dirty_set {
private:
    // Set of dirty blocks, indexed by block index.
    using set_t = boost::intrusive::set<
        block,
        boost::intrusive::member_hook<
            block, boost::intrusive::set_member_hook<>, &block::m_dirty_hook
        >,
        boost::intrusive::key_of_value<block_key>
    >;

private:
    /// List of all dirty blocks.
    set_t m_set;

public:
    block_dirty_set();

    ~block_dirty_set();

    auto begin() noexcept { return m_set.begin(); }

    auto end() noexcept { return m_set.end(); }

    /// Marks the block as dirty.
    void add(block& blk) noexcept;

    /// Returns true if the block has been marked as dirty.
    bool contains(block& blk) const noexcept { return blk.m_dirty_hook.is_linked(); }

    /// Marks the block as clean.
    /// \pre `contains(blk)`.
    void remove(block& blk) noexcept;

    void clear() noexcept;

    size_t size() const { return m_set.size(); }

    block_dirty_set(const block_dirty_set&) = delete;
    block_dirty_set& operator=(const block_dirty_set&) = delete;
};

/// Stores reuseable block instances.
struct block_pool {
private:
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
    void add(block* blk) noexcept;

    /// Removes a single block instance from the pool.
    /// The block is owned by the caller.
    /// Returns a nullptr if the pool is empty.
    block* remove() noexcept;

    /// The number of block instances in the pool
    size_t size() const noexcept { return m_list.size(); }

    /// True iff the pool is empty.
    bool empty() const noexcept { return m_list.empty(); }

    /// Removes all block instances from the pool and deletes them.
    void clear() noexcept;

    block_pool(const block_pool&) = delete;
    block_pool& operator=(const block_pool&) = delete;

private:
    static void dispose(block* blk) noexcept;
};

} // namespace

class file_engine_impl {
private:
    /// Underlying I/O-object.
    file* m_file;

    /// Size of a single block. Must be a power of two.
    u32 m_block_size;

    /// Maximum number of used blocks (pinned + cached).
    /// Can be violated if there are too many pinned blocks.
    size_t m_max_blocks;

    /// Maximum number of block instances (used + pooled).
    /// Slightly larger than m_max_blocks to avoid trashing on new/delete.
    size_t m_max_pooled_blocks;

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

public:
    explicit file_engine_impl(file& fd, u32 block_size, size_t cache_size);

    ~file_engine_impl();

    file& fd() const { return *m_file; }

    u32 block_size() const noexcept { return m_block_size; }

    const file_engine_stats& stats() const noexcept { return m_stats; }

    block* pin(u64 index, bool initialize);
    void unpin(u64 index, block* blk) noexcept;
    void dirty(u64 index, block* blk);
    void flush(u64 index, block* blk);

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    void flush();

    file_engine_impl(const file_engine_impl&) = delete;
    file_engine_impl& operator=(const file_engine_impl&) = delete;

private:
    /// Removes a cached block from main memory.
    /// Writes the block if it's dirty.
    void evict_block(block* blk);

    /// Write a single block back to disk.
    void flush_block(block* blk);

    /// Returns a new block instance, possibly
    /// from the free list.
    block* allocate_block();

    /// Deallocates a block (or puts it into the free list
    /// for later use).
    void free_block(block* blk) noexcept;
};

block::block(file_engine_impl* engine)
    : m_engine(engine)
{
    m_data = static_cast<byte*>(std::malloc(engine->block_size()));
    if (!m_data)
        throw std::bad_alloc();
}

block::~block() {
    std::free(m_data);
}

// --------------------------------
//
//   block cache
//   An lru cache for blocks read from disk.
//
// --------------------------------

block_cache::block_cache() noexcept
    : m_list()
{}

block_cache::~block_cache()
{
    clear();
}

void block_cache::clear() noexcept
{
    m_list.clear();
}

void block_cache::add(block& blk) noexcept
{
    PREQUEL_ASSERT(!contains(blk), "Must not be stored in the cache.");
    m_list.push_front(blk);
}

void block_cache::remove(block& blk) noexcept
{
    PREQUEL_ASSERT(contains(blk), "Must be stored in the cache.");

    auto iter = m_list.iterator_to(blk);
    m_list.erase(iter);
}

block* block_cache::lru_candidate() noexcept {
    return m_list.size() == 0 ? nullptr : &m_list.back();
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

template<typename Disposer>
void block_map::dispose(Disposer&& d) {
    m_map.clear_and_dispose(std::forward<Disposer>(d));
}

void block_map::insert(block& blk) noexcept
{
    PREQUEL_ASSERT(!contains(blk), "block is already stored in a map.");

    bool inserted;
    std::tie(std::ignore, inserted) = m_map.insert(blk);
    PREQUEL_ASSERT(inserted, "a block with that index already exists.");
}

void block_map::remove(block& blk) noexcept
{
    PREQUEL_ASSERT(contains(blk), "block not stored in map.");

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
    PREQUEL_ASSERT(m_list.empty(), "Blocks in the pool must not outlive the engine.");
}

void block_pool::clear() noexcept
{
    m_list.clear_and_dispose(dispose);
}

void block_pool::add(block* blk) noexcept
{
    PREQUEL_ASSERT(!blk->pinned() && !blk->cached(), "Block must not be referenced.");
    PREQUEL_ASSERT(!blk->dirty(), "Block must not be dirty.");
    PREQUEL_ASSERT(!blk->m_free_hook.is_linked(), "Block must not be in the pool.");
    m_list.push_back(*blk);
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
    m_set.clear();
}

void block_dirty_set::add(block& blk) noexcept
{
    PREQUEL_ASSERT(!contains(blk), "Block was already marked as dirty.");
    auto pair = m_set.insert(blk);
    PREQUEL_ASSERT(pair.second, "Insertion must have succeeded "
                                "because the block was not dirty.");
    unused(pair);
}

void block_dirty_set::remove(block& blk) noexcept
{
    PREQUEL_ASSERT(contains(blk), "block is not dirty");
    m_set.erase(m_set.iterator_to(blk));
}

// --------------------------------
//
//   block engine
//   Reads and writes blocks from a disk file
//   and caches them in memory.
//
// --------------------------------

file_engine_impl::file_engine_impl(file& fd, u32 block_size, size_t cache_size)
    : m_file(&fd)
    , m_block_size(block_size)
    , m_max_blocks(cache_size)
    , m_max_pooled_blocks((cache_size + 8) < cache_size ? size_t(-1) : (cache_size + 8))
    , m_pool()
    , m_blocks(m_max_blocks)
    , m_cache()
    , m_stats()
{
    PREQUEL_CHECK(is_pow2(block_size), "block size must be a power of two.");
}

file_engine_impl::~file_engine_impl()
{
    // Make an attempt to flush pending IO.
    try {
        flush();
    } catch (...) {}

    m_dirty.clear();
    m_cache.clear();
    m_blocks.dispose([](block* blk) {
        delete blk;
    });
    m_pool.clear();
}

block* file_engine_impl::pin(u64 index, bool initialize)
{
    // Check the cache.
    if (block* blk = m_blocks.find(index)) {
        if (blk->pinned()) {
            PREQUEL_THROW(bad_argument(
                fmt::format("Block is already pinned (index {})", index)
            ));
        }

        PREQUEL_ASSERT(blk->cached(), "Unpinned blocks in memory are always in the cache.");
        ++m_stats.cache_hits;
        m_cache.remove(*blk);

        blk->m_pinned = true;
        return blk;
    }

    // We need to allocate a new block instance; fix the cache.
    while (m_blocks.size() > m_max_blocks) {
        block* evict = m_cache.lru_candidate();
        if (!evict)
            break;

        evict_block(evict);
    }

    block* blk = allocate_block();
    detail::deferred guard = [&]{
        free_block(blk);
    };

    {
        blk->m_index = index;
        if (initialize) {
            PREQUEL_PRINT_READ(index);
            m_file->read(index * m_block_size, blk->m_data, m_block_size);
            ++m_stats.reads;
        }
    }
    guard.disable();

    blk->m_pinned = true;
    m_blocks.insert(*blk);
    return blk;
}

void file_engine_impl::unpin(u64 index, block* blk) noexcept {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    blk->m_pinned = false;
    m_cache.add(*blk);
}

void file_engine_impl::dirty(u64 index,block* blk) {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    if (!blk->dirty()) {
        m_dirty.add(*blk);
    }
}

void file_engine_impl::flush(u64 index,block* blk) {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    if (blk->dirty()) {
        flush_block(blk);
    }
}

void file_engine_impl::flush()
{
    for (auto i = m_dirty.begin(), e = m_dirty.end();
         i != e; )
    {
        // Compute successor iterator here, flush_block will remove
        // the block from the dirty set and invalidate `i`.
        auto n = std::next(i);
        flush_block(&*i);
        i = n;
    }
    m_file->sync();

    PREQUEL_ASSERT(m_dirty.begin() == m_dirty.end(),
                   "no dirty blocks can remain.");
}

void file_engine_impl::evict_block(block* blk)
{
    PREQUEL_ASSERT(blk->cached(), "The block must be cached.");
    if (blk->dirty()) {
        flush_block(blk);
    }

    m_cache.remove(*blk);
    m_blocks.remove(*blk);
    free_block(blk);
}

void file_engine_impl::flush_block(block* blk)
{
    PREQUEL_ASSERT(m_dirty.contains(*blk), "Block must be registered as dirty.");

    PREQUEL_PRINT_WRITE(blk->index());
    m_file->write(blk->m_index * m_block_size, blk->m_data, m_block_size);
    m_dirty.remove(*blk);
    ++m_stats.writes;
}

block* file_engine_impl::allocate_block()
{
    block* blk = m_pool.remove();
    if (!blk) {
        blk = new block(this);
    }
    return blk;
}

// Add to pool or delete depending on the number of blocks in memory.
void file_engine_impl::free_block(block* blk) noexcept
{
    if (m_blocks.size() + m_pool.size() < m_max_pooled_blocks) {
        blk->reset();
        m_pool.add(blk);
    } else {
        delete blk;
    }
}

} // namespace detail

file_engine::file_engine(file& fd, u32 block_size, size_t cache_size)
    : engine(block_size)
    , m_impl(std::make_unique<detail::file_engine_impl>(fd, block_size, cache_size))
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

void file_engine::do_flush() {
    impl().flush();
}


engine::pin_result file_engine::do_pin(block_index index, bool initialize) {
    detail::block* blk = impl().pin(index.value(), initialize);

    pin_result result;
    result.data = blk->data();
    result.cookie = reinterpret_cast<uintptr_t>(blk);
    return result;
}

void file_engine::do_unpin(block_index index, uintptr_t cookie) noexcept {
    impl().unpin(index.value(), reinterpret_cast<detail::block*>(cookie));
}

void file_engine::do_dirty(block_index index, uintptr_t cookie) {
    impl().dirty(index.value(), reinterpret_cast<detail::block*>(cookie));
}

void file_engine::do_flush(block_index index, uintptr_t cookie) {
    impl().flush(index.value(), reinterpret_cast<detail::block*>(cookie));
}

detail::file_engine_impl& file_engine::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid engine instance.");
    return *m_impl;
}

} // namespace prequel
