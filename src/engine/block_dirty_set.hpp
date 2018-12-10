#ifndef PREQUEL_ENGINE_BLOCK_DIRTY_SET_HPP
#define PREQUEL_ENGINE_BLOCK_DIRTY_SET_HPP

#include "base.hpp"
#include "block.hpp"

#include <boost/intrusive/set.hpp>

namespace prequel::detail::engine_impl {

/// Indexes dirty blocks (in an ordered fashion).
class block_dirty_set {
public:
    block_dirty_set() = default;

    ~block_dirty_set() { clear(); }

    auto begin() noexcept { return m_set.begin(); }

    auto end() noexcept { return m_set.end(); }

    /// Marks the block as dirty.
    void add(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(!contains(blk), "Block was already marked as dirty.");

        auto pair = m_set.insert(*blk);
        PREQUEL_ASSERT(pair.second, "Insertion must have succeeded because the block was not dirty.");
        unused(pair);
    }

    /// Returns true if the block has been marked as dirty.
    bool contains(block* blk) const noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        return blk->m_dirty_hook.is_linked();
    }

    /// Marks the block as clean.
    /// \pre `contains(blk)`.
    void remove(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(contains(blk), "Block is not dirty.");
        m_set.erase(m_set.iterator_to(*blk));
    }

    void clear() noexcept { m_set.clear(); }

    size_t size() const { return m_set.size(); }

    block_dirty_set(const block_dirty_set&) = delete;
    block_dirty_set& operator=(const block_dirty_set&) = delete;

private:
    // Set of dirty blocks, indexed by block index.
    using set_t = boost::intrusive::set<
        block,
        boost::intrusive::member_hook<block, boost::intrusive::set_member_hook<>, &block::m_dirty_hook>,
        boost::intrusive::key_of_value<index_of_block>>;

private:
    /// Ordered set of all dirty blocks.
    set_t m_set;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_DIRTY_SET_HPP
