#ifndef PREQUEL_ENGINE_FILE_ENGINE_HPP
#define PREQUEL_ENGINE_FILE_ENGINE_HPP

#include "base.hpp"
#include "block.hpp"
#include "block_cache.hpp"
#include "block_dirty_set.hpp"
#include "block_map.hpp"
#include "block_pool.hpp"

#include <prequel/file_engine.hpp>
#include <prequel/vfs.hpp>

#ifdef PREQUEL_TRACE_IO
#    define PREQUEL_PRINT_READ(index) (fmt::print("Reading block {}\n", (index)))
#    define PREQUEL_PRINT_WRITE(index) (fmt::print("Writing block {}\n", (index)))
#else
#    define PREQUEL_PRINT_READ(index)
#    define PREQUEL_PRINT_WRITE(index)
#endif

namespace prequel::detail::engine_impl {

class file_engine {
public:
    explicit file_engine(file& fd, u32 block_size, size_t cache_size);

    ~file_engine();

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

    file_engine(const file_engine&) = delete;
    file_engine& operator=(const file_engine&) = delete;

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

    /// True if the underlying file was opened in read-only mode.
    bool m_read_only = false;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_FILE_ENGINE_HPP
