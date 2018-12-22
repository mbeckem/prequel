#ifndef PREQUEL_ENGINE_ENGINE_BASE_HPP
#define PREQUEL_ENGINE_ENGINE_BASE_HPP

#include "base.hpp"
#include "block.hpp"
#include "block_cache.hpp"
#include "block_dirty_set.hpp"
#include "block_map.hpp"
#include "block_pool.hpp"

#include <prequel/file_engine.hpp>

namespace prequel::detail::engine_impl {

class engine_base {
public:
    // Cache size: number of blocks cached in memory.
    inline explicit engine_base(u32 block_size, size_t cache_blocks, bool read_only);

    inline virtual ~engine_base();

    u32 block_size() const noexcept { return m_block_size; }
    const file_engine_stats& stats() const noexcept { return m_stats; }

    inline virtual block* pin(u64 index, bool initialize);
    inline virtual void unpin(u64 index, block* blk) noexcept;
    inline virtual void dirty(u64 index, block* blk);
    inline virtual void flush(u64 index, block* blk);

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    inline virtual void flush();

    engine_base(const engine_base&) = delete;
    engine_base& operator=(const engine_base&) = delete;

protected:
    // Index is a block index, starting from 0. Buffer size is m_block_size.
    virtual void do_read(u64 index, byte* buffer) = 0;
    virtual void do_write(u64 index, const byte* buffer) = 0;

protected:
    /// Throws away all dirty blocks.
    /// Requires that none of those blocks are pinned.
    inline void discard_dirty();

    /// Discards the block with the given index (if it has been loaded into memory).
    inline void discard(u64 index);

private:
    /// Removes a cached block from main memory.
    /// Writes the block if it's dirty.
    inline void evict_block(block* blk);

    /// Write a single block back to disk.
    inline void flush_block(block* blk);

    /// Returns a new block instance, possibly
    /// from the free list.
    inline block* allocate_block();

    /// Deallocates a block (or puts it into the free list
    /// for later use).
    inline void free_block(block* blk) noexcept;

protected:
    /// Size of a single block. Must be a power of two.
    const u32 m_block_size;

    /// Log2(m_block_size) for fast division.
    const u32 m_block_size_log;

    /// Maximum number of used blocks (pinned + cached).
    /// Can be violated if there are too many pinned blocks.
    const size_t m_max_blocks;

    /// Maximum number of block instances (used + pooled).
    /// Slightly larger than m_max_blocks to avoid trashing on new/delete.
    const size_t m_max_pooled_blocks;

    /// True if the underlying file was opened in read-only mode.
    const bool m_read_only = false;

private:
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
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_ENGINE_BASE_HPP
