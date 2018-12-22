#ifndef PREQUEL_ENGINE_ENGINE_BASE_IPP
#define PREQUEL_ENGINE_ENGINE_BASE_IPP

#include "engine_base.hpp"

#include <prequel/deferred.hpp>
#include <prequel/math.hpp>

namespace prequel::detail::engine_impl {

engine_base::engine_base(u32 block_size, size_t cache_blocks, bool read_only)
    : m_block_size(block_size)
    , m_block_size_log(log2(m_block_size))
    , m_max_blocks(cache_blocks)
    , m_max_pooled_blocks((cache_blocks + 8) < cache_blocks ? size_t(-1) : (cache_blocks + 8))
    , m_read_only(read_only)
    , m_pool()
    , m_blocks(m_max_blocks)
    , m_cache()
    , m_stats() {
    PREQUEL_CHECK(is_pow2(block_size), "block size must be a power of two.");
}

engine_base::~engine_base() {
    m_dirty.clear();
    m_cache.clear();
    m_blocks.dispose([](block* blk) { delete blk; });
    m_pool.clear();
}

block* engine_base::pin(u64 index, bool initialize) {
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
            do_read(index, blk->m_data);
            ++m_stats.reads;
        }
    }
    guard.disable();

    blk->m_pinned = true;
    m_blocks.insert(blk);
    return blk;
}

void engine_base::unpin(u64 index, block* blk) noexcept {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    blk->m_pinned = false;
    m_cache.add(blk);
}

void engine_base::dirty(u64 index, block* blk) {
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

void engine_base::flush(u64 index, block* blk) {
    PREQUEL_ASSERT(blk && blk->pinned(), "Block was not pinned");
    PREQUEL_ASSERT(blk->index() == index, "Inconsistent block and block index.");
    unused(index);

    if (blk->dirty()) {
        flush_block(blk);
    }
}

void engine_base::flush() {
    for (auto i = m_dirty.begin(), e = m_dirty.end(); i != e;) {
        // Compute successor iterator here, flush_block will remove
        // the block from the dirty set and invalidate `i`.
        auto n = std::next(i);
        flush_block(&*i);
        i = n;
    }

    PREQUEL_ASSERT(m_dirty.begin() == m_dirty.end(), "no dirty blocks can remain.");
}

void engine_base::discard_dirty() {
    for (auto i = m_dirty.begin(), e = m_dirty.end(); i != e;) {
        auto n = std::next(i);

        block* blk = &*i;
        PREQUEL_ASSERT(!blk->pinned(), "Cannot discard pinned blocks.");
        PREQUEL_ASSERT(blk->cached(), "Block must be in the cache.");
        m_dirty.remove(blk);
        m_cache.remove(blk);
        m_blocks.remove(blk);
        free_block(blk);

        i = n;
    }

    PREQUEL_ASSERT(m_dirty.begin() == m_dirty.end(), "no dirty blocks can remain.");
}

void engine_base::discard(u64 index) {
    if (block* blk = m_blocks.find(index)) {
        PREQUEL_ASSERT(!blk->pinned(), "Cannot discard pinned blocks.");
        PREQUEL_ASSERT(blk->cached(), "Block must be in the cache.");

        if (m_dirty.contains(blk))
            m_dirty.remove(blk);
        m_cache.remove(blk);
        m_blocks.remove(blk);
        free_block(blk);
    }
}

void engine_base::evict_block(block* blk) {
    PREQUEL_ASSERT(blk->cached(), "The block must be cached.");
    if (blk->dirty()) {
        flush_block(blk);
    }

    m_cache.remove(blk);
    m_blocks.remove(blk);
    free_block(blk);
}

void engine_base::flush_block(block* blk) {
    PREQUEL_ASSERT(m_dirty.contains(blk), "Block must be registered as dirty.");
    PREQUEL_ASSERT(!m_read_only, "Must not write blocks when engine is read only.");

    PREQUEL_PRINT_WRITE(blk->index());
    do_write(blk->m_index, blk->m_data);
    m_dirty.remove(blk);
    ++m_stats.writes;
}

block* engine_base::allocate_block() {
    block* blk = m_pool.remove();
    if (!blk) {
        blk = new block(this);
    }
    return blk;
}

// Add to pool or delete depending on the number of blocks in memory.
void engine_base::free_block(block* blk) noexcept {
    if (m_blocks.size() + m_pool.size() < m_max_pooled_blocks) {
        blk->reset();
        m_pool.add(blk);
    } else {
        delete blk;
    }
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_ENGINE_BASE_IPP
