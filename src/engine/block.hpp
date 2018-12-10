#ifndef PREQUEL_ENGINE_BLOCK_HPP
#define PREQUEL_ENGINE_BLOCK_HPP

#include "base.hpp"

#include <cstdlib>

namespace prequel::detail::engine_impl {

/*
 * Represents a block loaded from disk into memory.
 * A block is either pinned, in the cache or in the pool of reusable objects.
 *
 * Dirty blocks are written back to disk before they are removed from main memory.
 */
struct block {
public:
    /// The engine this block belongs to.
    file_engine* const m_engine;

    /// Index of the block within the file.
    u64 m_index = 0;

    /// Block sized data array.
    byte* m_data = nullptr;

    /// True if the block is referenced from the outside.
    /// It must not be dropped from memory until it is unpinned by the application.
    bool m_pinned = false;

    /// Used by the free list in block_pool.
    block_pool_hook m_pool_hook;

    /// Used by the block_cache.
    block_cache_hook m_cache_hook;

    /// Used by the block_map.
    block_map_hook m_map_hook;

    /// Marks the block as dirty and links all dirty blocks together
    /// in an ordered sequence. Used by the block_dirty_set.
    block_dirty_set_hook m_dirty_hook;

public:
    explicit block(file_engine* engine);

    ~block();

    block(const block&) = delete;
    block& operator=(const block&) = delete;

    /// Puts the block into a state where it can be reused.
    void reset() noexcept {
        // The block must not be in any of the containers
        // when it is being reset.
        PREQUEL_ASSERT(!m_pool_hook.is_linked(), "in free list");
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

// Blocks can be indexed by their block index.
struct index_of_block {
    using type = u64;

    u64 operator()(const block& blk) const noexcept { return blk.index(); }
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_HPP
