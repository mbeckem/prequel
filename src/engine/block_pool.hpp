#ifndef PREQUEL_ENGINE_BLOCK_POOL_HPP
#define PREQUEL_ENGINE_BLOCK_POOL_HPP

#include "base.hpp"
#include "block.hpp"

#include <boost/intrusive/list.hpp>

namespace prequel::detail::engine_impl {

/// Stores reuseable block instances.
class block_pool {
public:
    block_pool() = default;

    /// Deletes all block instances that are currently in the pool.
    ~block_pool() { clear(); }

    /// Add a block to the pool for future use.
    /// The pool takes ownership of this block.
    /// The block must not already be in this pool.
    void add(block* blk) noexcept {
        PREQUEL_ASSERT(!blk->pinned() && !blk->cached(), "Block must not be referenced.");
        PREQUEL_ASSERT(!blk->dirty(), "Block must not be dirty.");
        PREQUEL_ASSERT(!blk->m_pool_hook.is_linked(), "Block must not be in the pool.");
        m_list.push_back(*blk);
    }

    /// Removes a single block instance from the pool.
    /// The block is owned by the caller.
    /// Returns a nullptr if the pool is empty.
    block* remove() noexcept {
        if (m_list.empty())
            return nullptr;

        block* blk = &m_list.front();
        m_list.pop_front();
        return blk;
    }

    /// The number of block instances in the pool
    size_t size() const noexcept { return m_list.size(); }

    /// True iff the pool is empty.
    bool empty() const noexcept { return m_list.empty(); }

    /// Removes all block instances from the pool and deletes them.
    void clear() noexcept { m_list.clear_and_dispose([](block* blk) { delete blk; }); }

    block_pool(const block_pool&) = delete;
    block_pool& operator=(const block_pool&) = delete;

private:
    using list_t = boost::intrusive::list<
        block, boost::intrusive::member_hook<block, boost::intrusive::list_member_hook<>,
                                             &block::m_pool_hook>>;

private:
    /// List of free blocks.
    list_t m_list;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_POOL_HPP
