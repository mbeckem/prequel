#ifndef PREQUEL_DEFAULT_ALLOCATOR2_HPP
#define PREQUEL_DEFAULT_ALLOCATOR2_HPP

#include <prequel/allocator.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/btree.hpp>
#include <prequel/defs.hpp>
#include <prequel/detail/free_list.hpp>
#include <prequel/serialization.hpp>

#include <ostream>

namespace prequel {

class default_allocator : public allocator {
private:
    // An extent represents a region of space within the file.
    struct extent_t {
        block_index block; ///< Index of the first block.
        u64 size = 0;      ///< Number of blocks in this extent.

        extent_t() = default;

        extent_t(block_index block_, u64 size_)
            : block(block_)
            , size(size_) {}

        static constexpr auto get_binary_format() {
            return make_binary_format(&extent_t::block, &extent_t::size);
        }

        bool operator==(const extent_t& other) const {
            return block == other.block && size == other.size;
        }

        bool operator!=(const extent_t& other) const { return !(*this == other); }
    };

    struct derive_block_key {
        block_index operator()(const extent_t& e) const {
            return e.block; // block indices are unique
        }
    };

    struct order_by_size {
        bool operator()(const extent_t& a, const extent_t& b) const {
            if (a.size != b.size)
                return a.size < b.size;
            return a.block < b.block;
        }
    };

    using extent_position_tree = btree<extent_t, derive_block_key>;
    using extent_size_tree = btree<extent_t, identity_t, order_by_size>;

public:
    class anchor {
        /// TODO: Use a ring buffer or queue here.
        detail::free_list::anchor meta_freelist;

        /// Number of blocks set aside for metadata (includes free blocks).
        u64 meta_total = 0;

        /// Number of free blocks on the meta freelist.
        u64 meta_free = 0;

        /// Total number of blocks managed by the allocator.
        u64 data_total = 0;

        /// Total number of data blocks that are currently unused.
        u64 data_free = 0;

        /// Indexes free extents by their position.
        extent_position_tree::anchor extents_by_position;

        /// Indexes free extents by their size.
        extent_size_tree::anchor extents_by_size;

        friend class binary_format_access;
        friend class default_allocator;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::meta_freelist, &anchor::meta_total,
                                      &anchor::meta_free, &anchor::data_total, &anchor::data_free,
                                      &anchor::extents_by_position, &anchor::extents_by_size);
        }
    };

public:
    default_allocator(anchor_handle<anchor> _anchor, engine& _engine);
    default_allocator(anchor_handle<anchor> _anchor, engine& _engine, bool can_grow);
    ~default_allocator();

    default_allocator(const default_allocator&) = delete;
    default_allocator& operator=(const default_allocator&) = delete;

public:
    /**
     * Adds a region of space to the allocator. The region will be used
     * to satisfy future allocations.
     */
    void add_region(block_index block, u64 size);

    struct allocation_stats {
        u64 data_total = 0; ///< Total number of blocks managed by the allocator (used + free + meta).
        u64 data_used = 0; ///< Number of blocks allocated and not freed.
        u64 data_free = 0; ///< Number of free blocks.
        u64 meta_data = 0; ///< Number of blocks used for internal metadata.
    };

    allocation_stats stats() const;

public:
    /// Minimum chunk size by which to grow the underlying file when new space for
    /// data is required.
    /// \{
    u32 min_chunk() const;
    void min_chunk(u32 chunk_size);
    /// \}

    /// Controls whether the allocated is allowed to request more storage from the storage engine
    /// by growing the file. Defaults to true.
    /// \{
    bool can_grow() const;
    void can_grow(bool can_grow);
    /// \}

    void dump(std::ostream& o) const;

    /// Perform validation of the allocator's state.
    void validate() const;

protected:
    virtual block_index do_allocate(u64 size) override;
    virtual block_index do_reallocate(block_index block, u64 old_size, u64 n) override;
    virtual void do_free(block_index block, u64 size) override;

private:
    struct impl_t;

    impl_t& impl() const;

private:
    std::unique_ptr<impl_t> m_impl;
};

} // namespace prequel

#endif // PREQUEL_DEFAULT_ALLOCATOR2_HPP
