#ifndef EXTPP_FILE_ENGINE_HPP
#define EXTPP_FILE_ENGINE_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/block_handle.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/io.hpp>
#include <extpp/math.hpp>

#include <boost/intrusive_ptr.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/unordered_set.hpp>
#include <boost/intrusive/list_hook.hpp>

#include <exception>
#include <iosfwd>
#include <type_traits>

namespace extpp {

class file;

/// Contains performance statistics for a single engine.
struct file_engine_stats {
    /// Number of blocks read from disk. Also the
    /// total number of cache misses.
    u64 reads = 0;

    /// Number of blocks written to disk.
    u64 writes = 0;

    /// Number of times a block was retrieved
    /// from the cache (i.e. no read was required).
    u64 cache_hits = 0;
};

namespace detail {

class block;
class block_cache;
class block_dirty_set;
class block_engine;
class block_map;
class block_pool;
class block_test;

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
class block final : public detail::block_handle_impl {
public:
    /// The engine this block belongs to.
    block_engine* const m_engine;

    /// The block size.
    const u32 m_block_size;

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

    /// The index of this block on disk.
    u64 m_index = -1;

    /// The block's raw data.
    byte* const m_buffer = nullptr;

    explicit block(block_engine* engine, u32 block_size)
        : m_engine(engine)
        , m_block_size(block_size)
        , m_buffer(static_cast<byte*>(std::malloc(block_size)))
    {
        if (!m_buffer)
            throw std::bad_alloc();
    }

    ~block() {
        free(m_buffer);
    }

    /// Puts the block into a state where it can be reused.
    void reset() noexcept {
        // The block must not be in any of the containers
        // when it is being reset.
        EXTPP_ASSERT(!m_free_hook.is_linked(), "in free list");
        EXTPP_ASSERT(!m_lru_hook.is_linked(), "in lru list");
        EXTPP_ASSERT(!m_map_hook.is_linked(), "in block map");
        EXTPP_ASSERT(!m_dirty_hook.is_linked(), "in dirty list");

        m_index = -1;
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

    virtual u64 index() const noexcept override { return m_index; }

    virtual byte* data() const noexcept override { return m_buffer; }

    virtual u32 block_size() const noexcept override { return m_block_size; }

    virtual void dirty() override { set_dirty(); }

    virtual block* copy() override {
        ref();
        return this;
    }

    virtual void destroy() override {
        unref();
    }

private:
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
class block_cache {
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
class block_map {
    // Blocks are indexed by their block index.
    struct block_key {
        using type = u64;

        u64 operator()(const block& blk) const noexcept {
            return blk.m_index;
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

    block_dirty_set(const block_dirty_set&) = delete;
    block_dirty_set& operator=(const block_dirty_set&) = delete;
};

class block_engine {
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
    /// Constructs a new block engine.
    ///
    /// \param fd
    ///     The file used for input and output. The reference must remain
    ///     valid for the lifetime of this instance.
    ///
    /// \param block_size
    ///     The size of a single block, in bytes.
    ///     Must be a power of two.
    ///
    /// \param cache_size
    ///     The number of blocks that can be cached in memory.
    explicit block_engine(file& fd, u32 block_size, u32 cache_size);

    ~block_engine();

    file& fd() const { return *m_file; }

    u32 block_size() const noexcept { return m_block_size; }

    /// Returns performance statistics for this engine.
    const file_engine_stats& stats() const noexcept { return m_stats; }

    /// Accesses the block with the given index if its already in memory.
    /// Otherwise, returns an invalid pointer. Never performs I/O.
    ///
    /// \note A successful access does not count as a cache-hit.
    boost::intrusive_ptr<block> access(u64 index);

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

    block_engine(const block_engine&) = delete;
    block_engine& operator=(const block_engine&) = delete;

private:
    friend class block;
    friend class block_test;

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

} // namespace detail

class file_engine : public engine
{
public:
    file_engine(file& fd, u32 block_size, u32 cache_size)
        : engine(block_size)
        , m_impl(fd, block_size, cache_size)
    {}

    file& fd() const { return m_impl.fd(); }

    const file_engine_stats& stats() const { return m_impl.stats(); }

private:
    u64 do_size() const override {
        return fd().file_size() / block_size();
    }

    void do_grow(u64 n) override {
        u64 new_size_blocks = checked_add(do_size(), n);
        u64 new_size_bytes = checked_mul<u64>(new_size_blocks, block_size());
        fd().truncate(new_size_bytes);
    }

    block_handle do_read(block_index index) override {
        EXTPP_CHECK(index, "Invalid index.");
        return {m_impl.read(index.value()).detach()};
    }

    block_handle do_zeroed(block_index index) override {
        EXTPP_CHECK(index, "Invalid index.");
        return {m_impl.overwrite_zero(index.value()).detach()};
    }

    block_handle do_overwritten(block_index index, const byte* data) override {
        EXTPP_CHECK(index, "Invalid index.");
        return {m_impl.overwrite_with(index.value(), data).detach()};
    }

    void do_flush() override {
        m_impl.flush();
    }

private:
    detail::block_engine m_impl;
};

} // namespace extpp

#endif // EXTPP_FILE_ENGINE_HPP
