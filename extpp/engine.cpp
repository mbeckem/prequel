#include <extpp/engine.hpp>

#include <extpp/io.hpp>
#include <extpp/math.hpp>

#include <extpp/detail/rollback.hpp>

#include <new>
#include <ostream>

namespace extpp {
namespace detail {

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
    if (m_list.size() > m_max_size) {
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

block_engine::block_engine(file& fd, u32 block_size, u32 cache_size)
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

block_engine::~block_engine()
{
    // TODO: Own debug macro
#ifndef NDEBUG
    for (block& b : m_blocks) {
        // The engine is going to be destroyed; the user must not have
        // any remaining block handles. However, the LRU cache may
        // still hold some blocks.
        EXTPP_ASSERT(b.m_refcount == 1 && m_cache.contains(b),
                    "Blocks must not be referenced from the outside.");

    }
#endif

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

boost::intrusive_ptr<block> block_engine::access(u64 index)
{
    if (block* blk = m_blocks.find(index)) {
        return boost::intrusive_ptr<block>(blk);
    }
    return {};
}

boost::intrusive_ptr<block> block_engine::read(u64 index)
{
    return read_impl(index, [&](byte* data) {
        m_file->read(index * m_block_size, data, m_block_size);
        ++m_stats.reads;
    });
}

boost::intrusive_ptr<block> block_engine::overwrite_zero(u64 index)
{
    // The read function is a no-op. Everything else is the same as in read().
    auto blk = read_impl(index, [&](byte*) {});

    std::memset(blk->m_buffer, 0, m_block_size);
    blk->set_dirty();
    return blk;
}

boost::intrusive_ptr<block> block_engine::overwrite_with(u64 index, const byte* data)
{
    auto blk = read_impl(index, [&](byte*) {});

    std::memcpy(blk->m_buffer, data, m_block_size);
    blk->set_dirty();
    return blk;
}

template<typename ReadAction>
boost::intrusive_ptr<block> block_engine::read_impl(u64 index, ReadAction&& read) {
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
        read(blk.m_buffer);
    }
    guard.commit();

    boost::intrusive_ptr<block> result(&blk);
    m_blocks.insert(blk);
    m_cache.use(blk);       // This can cause a block write (and therefore an error) for a
                            // block that is evicted from the cache.
    rethrow_write_error();
    return result;
}

void block_engine::flush()
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

void block_engine::set_dirty(block& blk) noexcept {
    m_dirty.add(blk);
}

void block_engine::flush_block(block& blk)
{
    EXTPP_ASSERT(blk.m_block_size == m_block_size,
                "block size invariant");

    if (m_dirty.contains(blk)) {
        m_file->write(blk.m_index * m_block_size, blk.m_buffer, m_block_size);
        m_dirty.remove(blk);
        ++m_stats.writes;
    }
}

void block_engine::finalize_block(block& blk) noexcept
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

block& block_engine::allocate_block()
{
    block* blk = m_pool.remove();
    if (!blk) {
        blk = new block(this, m_block_size);
    }
    return *blk;
}

// Add to pool or delete depending on the number of blocks in memory.
void block_engine::free_block(block& blk) noexcept
{
    if (m_blocks.size() + m_pool.size() < m_capacity) {
        blk.reset();
        m_pool.add(blk);
    } else {
        delete &blk;
    }
}

void block_engine::rethrow_write_error()
{
    if (m_write_error) {
        auto error = std::exchange(m_write_error, std::exception_ptr());
        std::rethrow_exception(std::move(error));
    }
}

} // namespace detail
} // namespace extpp
