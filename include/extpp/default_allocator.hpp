#ifndef EXTPP_DEFAULT_ALLOCATOR_HPP
#define EXTPP_DEFAULT_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
#include <extpp/defs.hpp>
#include <extpp/btree.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/handle.hpp>
#include <extpp/serialization.hpp>
#include <extpp/detail/free_list.hpp>

#include <ostream>

namespace extpp {

// TODO: Should probably end up somewhere else.
class block_source {
public:
    block_source() = default;
    virtual ~block_source();

    /// Returns the first block index of this instance.
    virtual block_index begin() = 0;

    /// Returns an estimate of the number of available blocks that
    /// can still be allocated through this instance.
    /// Should return `u64(-1)` if there is no physical limit.
    virtual u64 available() = 0;

    /// Returns the current size (number of allocated blocks)
    /// of this instance.
    virtual u64 size() = 0;

    /// Grow by exactly `n` blocks.
    virtual void grow(u64 n) = 0;
};

// TODO: Review the implementation and make sure that allocation failures are always
// recoverable, i.e. a future allocation (with a smaller size) should be able to succeed
// and no data should be leaked.
class default_allocator : public allocator {
private:
    // An extent represents a region of allocated space.
    // Free'd allocations that neighbor each other are merged in order
    // to form maximal extents. Extents are indexed by their starting block address.
    struct extent_t {
        block_index block;  ///< Index of the first block.
        u64 size: 63;       ///< Number of blocks in this extent.
        u64 free: 1;        ///< Free or used.

        extent_t(): extent_t(block_index(), 0, false) {}

        extent_t(block_index block, u64 size, bool free)
            : block(block), size(size), free(free)
        {}

        // Indexed by block index.
        struct derive_key {
            block_index operator()(const extent_t& e) const {
                return e.block;
            }
        };

        // Custom serializer to encode the bitfield.
        struct binary_serializer {
            static constexpr u64 free_bit = u64(1) << 63;

            static constexpr size_t serialized_size() {
                return extpp::serialized_size<block_index>() + extpp::serialized_size<u64>();
            }

            static void serialize(const extent_t& e, byte* b) {
                b = extpp::serialize(e.block, b);

                u64 packed = 0;
                packed |= e.free ? free_bit : 0;
                packed |= e.size;
                extpp::serialize(packed, b);
            }

            static void deserialize(extent_t& e, const byte* b) {
                b = extpp::deserialize(e.block, b);

                u64 packed;
                extpp::deserialize(packed, b);
                e.free = packed & free_bit ? 1 : 0;
                e.size = packed; // Compiler truncates.
            }
        };
    };

    using extent_tree_t = btree<extent_t, extent_t::derive_key>;

private:
    /// Represents a freed extent in the free list, which is index by extent size.
    /// Entries for an allocation size of N blocks can quickly be found by
    /// finding the smallest free extent with at least N blocks in the tree.
    ///
    /// Note: duplicate sizes are allowed, entries are distinguished by the block index
    /// if their sizes are equal.
    struct free_extent_t {
        u64 size = 0;
        block_index block;

        free_extent_t() = default;
        free_extent_t(u64 size, block_index block): size(size), block(block) {}

        bool operator<(const free_extent_t& other) const {
            if (size != other.size)
                return size < other.size;
            return block < other.block;
        }

        static constexpr auto get_binary_format() {
            return make_binary_format(&free_extent_t::size, &free_extent_t::block);
        }
    };

    using free_extent_tree_t = btree<free_extent_t>;

public:
    class anchor {
        /// Number of blocks allocated for metadata.
        u64 metadata_total = 0;

        /// Number of free metadata blocks.
        u64 metadata_free = 0;

        /// Contains free metadata blocks. These are used for the internal datastructures (the btrees).
        detail::free_list::anchor meta_freelist;

        /// Number of blocks allocated for data.
        u64 data_total = 0;

        /// Total number of data blocks that are currently unused.
        u64 data_free = 0;

        /// Maps every known extent address to its state.
        typename extent_tree_t::anchor extents;

        /// Contains free extents.
        typename free_extent_tree_t::anchor free_extents;

        friend class binary_format_access;
        friend class default_allocator;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::metadata_total, &anchor::metadata_free,
                                      &anchor::meta_freelist,
                                      &anchor::data_total, &anchor::data_free,
                                      &anchor::extents, &anchor::free_extents);
        }
    };

public:
    default_allocator(anchor_handle<anchor> _anchor, engine& _engine);
    default_allocator(anchor_handle<anchor> _anchor, engine& _engine, block_source& source);
    ~default_allocator();

    default_allocator(const default_allocator&) = delete;
    default_allocator& operator=(const default_allocator&) = delete;

public:
    /// Allocation statistics.
    struct allocation_stats_t {
        u64 data_total = 0;         ///< Total number of data blocks allocated from the underlying file.
        u64 data_used = 0;          ///< Number of data blocks in use (i.e. alloc without free).
        u64 data_free = 0;          ///< Number of free data blocks.

        u64 metadata_total = 0;     ///< Total number of metadata blocks allocated from the underlying file.s
        u64 metadata_used = 0;      ///< Number of metadata blocks in use.
        u64 metadata_free = 0;      ///< Number of free metadata blocks.
    };

    /// Returns current allocation statistics for this instance.
    allocation_stats_t stats() const;

public:
    /// As an extension to the normal allocator interface, this allocator allows
    /// querying for the allocation size. `index` must have been allocated (or reallocated)
    /// through this instance and must not have been freed yet.
    ///
    /// \return Returns the number of blocks allocated at that index, i.e. precisely
    /// the number of blocks requested in the call to `allocate` or `reallocate` that
    /// returned this index.
    u64 allocated_size(block_index index) const;

    /// Minimum chunk size by which to grow the underlying file when new space for
    /// data is required.
    /// \{
    u32 min_chunk() const;
    void min_chunk(u32 chunk_size);
    /// \}

    /// Minimum chunk size by which to grow the underlying file when new space for
    /// metadata is required.
    /// \{
    u32 min_meta_chunk() const;
    void min_meta_chunk(u32 chunk_size);
    /// \}

    void dump(std::ostream& o) const;

    /// Perform validation of the allocator's state.
    void validate() const;

protected:
    virtual block_index do_allocate(u64 n) override;
    virtual block_index do_reallocate(block_index a, u64 n) override;
    virtual void do_free(block_index a) override;

private:
    struct impl_t;

    impl_t& impl() const;

private:
    std::unique_ptr<impl_t> m_impl;
};

} // namespace extpp

#endif // EXTPP_DEFAULT_ALLOCATOR_HPP
