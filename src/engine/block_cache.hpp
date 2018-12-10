#ifndef PREQUEL_ENGINE_BLOCK_CACHE_HPP
#define PREQUEL_ENGINE_BLOCK_CACHE_HPP

#include "base.hpp"
#include "block.hpp"

#include <prequel/type_traits.hpp>

#include <boost/intrusive/list.hpp>

namespace prequel::detail::engine_impl {

/// Caches uses blocks in main memory.
/// Note that the cache does not own the blocks.
struct block_cache {
public:
    explicit block_cache() noexcept {};

    ~block_cache() { clear(); }

    void clear() noexcept { m_list.clear(); }

    /// True if the block is in the cache.
    bool contains(block* blk) const noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        return blk->m_cache_hook.is_linked();
    }

    /// Inserts the block into the cache. The block must not be cached already.
    void add(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(!contains(blk), "Must not be stored in the cache.");
        m_list.push_front(*blk);
    }

    /// Removes the block from the cache. The block must be cached.
    void remove(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(contains(blk), "Must be stored in the cache.");

        auto iter = m_list.iterator_to(*blk);
        m_list.erase(iter);
    }

    /// Returns a pointer to the block that should be evicted next.
    /// Does not remove that block.
    block* lru_candidate() noexcept { return m_list.size() == 0 ? nullptr : &m_list.back(); }

    /// Returns the current number of cached blocks.
    size_t size() const noexcept { return m_list.size(); }

    block_cache(const block_cache&) = delete;
    block_cache& operator=(const block_cache&) = delete;

private:
    using list_t = boost::intrusive::list<
        block, boost::intrusive::member_hook<block, boost::intrusive::list_member_hook<>,
                                             &block::m_cache_hook>>;

private:
    /// Linked list of cached blocks (intrusive).
    /// The most recently used block is at the front.
    list_t m_list;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_CACHE_HPP
