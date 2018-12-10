#ifndef PREQUEL_ENGINE_BLOCK_MAP_HPP
#define PREQUEL_ENGINE_BLOCK_MAP_HPP

#include "base.hpp"
#include "block.hpp"

#include <prequel/math.hpp>

#include <boost/intrusive/unordered_set.hpp>

#include <algorithm>
#include <vector>

namespace prequel::detail::engine_impl {

/// A map that indexes all live block instances.
class block_map {
public:
    /// Constructs a block map with the given size hint.
    ///
    /// \param expected_load
    ///     The approximate expected worst case load.
    ///     This number is used to compute an appropriate
    ///     number of hash table buckets.
    explicit block_map(size_t expected_load)
        : m_buckets(round_towards_pow2(std::max(size_t(32), (expected_load * 4) / 3)))
        , m_map(map_t::bucket_traits(m_buckets.data(), m_buckets.size())) {}

    ~block_map() = default;

    auto begin() noexcept { return m_map.begin(); }

    auto end() noexcept { return m_map.end(); }

    void clear() noexcept { m_map.clear(); }

    template<typename Disposer>
    void dispose(Disposer&& d) {
        m_map.clear_and_dispose(std::forward<Disposer>(d));
    }

    /// Insert a block into the map.
    ///
    /// \pre The block must not be stored already.
    /// \pre The block's index must be unique.
    void insert(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(!contains(blk), "Block is already stored in a map.");

        bool inserted;
        std::tie(std::ignore, inserted) = m_map.insert(*blk);
        PREQUEL_ASSERT(inserted, "A block with that index already exists.");
    }

    /// Removes a block from the map.
    ///
    /// \pre `contains(blk)`.
    void remove(block* blk) noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        PREQUEL_ASSERT(contains(blk), "block not stored in map.");

        m_map.erase(m_map.iterator_to(*blk));
    }

    /// Finds the block with the given index and returns it.
    /// Returns nullptr if no such block was found.
    block* find(u64 index) const noexcept {
        auto iter = m_map.find(index);
        if (iter == m_map.end()) {
            return nullptr;
        }

        const block& blk = *iter;
        return const_cast<block*>(&blk);
    }

    /// Returns true if the block is inside this map.
    bool contains(block* blk) const noexcept {
        PREQUEL_ASSERT(blk, "Invalid block pointer.");
        return blk->m_map_hook.is_linked();
    }

    /// Returns the number of blocks in this map.
    size_t size() const noexcept { return m_map.size(); }

    block_map(const block_map&) = delete;
    block_map& operator=(const block_map&) = delete;

private:
    struct block_index_hash {
        size_t operator()(u64 index) const noexcept { return boost::hash_value(index); }
    };

    using map_t = boost::intrusive::unordered_set<
        block,
        boost::intrusive::member_hook<block, boost::intrusive::unordered_set_member_hook<>,
                                      &block::m_map_hook>,
        boost::intrusive::power_2_buckets<true>, boost::intrusive::key_of_value<index_of_block>,
        boost::intrusive::hash<block_index_hash>>;

    using bucket_t = typename map_t::bucket_type;

private:
    /// Holds the buckets for the intrusive map.
    std::vector<bucket_t> m_buckets;

    /// Maps block indices to block instances.
    map_t m_map;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_MAP_HPP
