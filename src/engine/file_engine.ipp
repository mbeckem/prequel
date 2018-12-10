#ifndef PREQUEL_ENGINE_FILE_ENGINE_IPP
#define PREQUEL_ENGINE_FILE_ENGINE_IPP

#include "file_engine.hpp"

#include <prequel/deferred.hpp>

namespace prequel::detail::engine_impl {

inline file_engine::file_engine(file& fd, u32 block_size, size_t cache_size)
    : m_file(&fd)
    , m_block_size(block_size)
    , m_max_blocks(cache_size)
    , m_max_pooled_blocks((cache_size + 8) < cache_size ? size_t(-1) : (cache_size + 8))
    , m_pool()
    , m_blocks(m_max_blocks)
    , m_cache()
    , m_stats()
    , m_read_only(m_file->read_only()) {
    PREQUEL_CHECK(is_pow2(block_size), "block size must be a power of two.");
}

inline file_engine::~file_engine() {
    // Make an attempt to flush pending IO.
    try {
        flush();
    } catch (...) {
    }

    m_dirty.clear();
    m_cache.clear();
    m_blocks.dispose([](block* blk) { delete blk; });
    m_pool.clear();
}

inline block* file_engine::pin(u64 index, bool initialize) {
    // Check the cache.
    if (block* blk = m_blocks.find(index)) {
        if (blk->pinned()) {
            PREQUEL_THROW(bad_argument(fmt::format("Block is already pinned (index {})", index)));
        }

        PREQUEL_ASSERT(blk->cached(), "Unpinned blocks in memory are always in the cache.");
        ++m_stats.cache_hits;
        m_cache.remove(blk);

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
    deferred guard = [&] { free_block(blk); };

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
    m_blocks.insert(blk);
    return blk;
}

inline void file_engine::unpin(u64 index, block* blk) noexcept {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    blk->m_pinned = false;
    m_cache.add(blk);
}

inline void file_engine::dirty(u64 index, block* blk) {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    if (PREQUEL_UNLIKELY(m_read_only))
        PREQUEL_THROW(
            io_error("The file cannot be written to because it was opened in read-only mode."));

    if (!blk->dirty()) {
        m_dirty.add(blk);
    }
}

inline void file_engine::flush(u64 index, block* blk) {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    if (blk->dirty()) {
        flush_block(blk);
    }
}

inline void file_engine::flush() {
    for (auto i = m_dirty.begin(), e = m_dirty.end(); i != e;) {
        // Compute successor iterator here, flush_block will remove
        // the block from the dirty set and invalidate `i`.
        auto n = std::next(i);
        flush_block(&*i);
        i = n;
    }

    PREQUEL_ASSERT(m_dirty.begin() == m_dirty.end(), "no dirty blocks can remain.");
}

inline void file_engine::evict_block(block* blk) {
    PREQUEL_ASSERT(blk->cached(), "The block must be cached.");
    if (blk->dirty()) {
        flush_block(blk);
    }

    m_cache.remove(blk);
    m_blocks.remove(blk);
    free_block(blk);
}

inline void file_engine::flush_block(block* blk) {
    PREQUEL_ASSERT(m_dirty.contains(blk), "Block must be registered as dirty.");

    PREQUEL_PRINT_WRITE(blk->index());
    m_file->write(blk->m_index * m_block_size, blk->m_data, m_block_size);
    m_dirty.remove(blk);
    ++m_stats.writes;
}

inline block* file_engine::allocate_block() {
    block* blk = m_pool.remove();
    if (!blk) {
        blk = new block(this);
    }
    return blk;
}

// Add to pool or delete depending on the number of blocks in memory.
inline void file_engine::free_block(block* blk) noexcept {
    if (m_blocks.size() + m_pool.size() < m_max_pooled_blocks) {
        blk->reset();
        m_pool.add(blk);
    } else {
        delete blk;
    }
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_FILE_ENGINE_IPP
