#ifndef EXTPP_REGION_ALLOCATOR_HPP
#define EXTPP_REGION_ALLOCATOR_HPP

#include <extpp/defs.hpp>
#include <extpp/default_allocator.hpp>
#include <extpp/engine.hpp>

namespace extpp {

/// Allocates blocks from a fixed size contiguous region in secondary storage.
class region_allocator : public allocator {
public:
    class anchor {
        block_index begin;                  ///< First block on disk.
        u64 size = 0;                       ///< Total number of available blocks.
        u64 used = 0;                       ///< Blocks `[0, used)` are used.
        default_allocator::anchor alloc;    ///< Allocates from the region.

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::begin, &anchor::used,
                                      &anchor::size, &anchor::alloc);
        }

        friend binary_format_access;
        friend region_allocator;
    };

public:
    region_allocator(anchor_handle<anchor> _anchor, engine& _engine);

    region_allocator(const region_allocator&) = delete;
    region_allocator& operator=(const region_allocator&) = delete;

    /// Initialize the region allocator with the given range of free blocks.
    /// Reinitializing an allocator is not allowed (drop the anchor instead).
    ///
    /// TODO: Think about this init function more carefully, maybe make a static
    /// "create()" function here and use that approach everywhere in the future.
    void initialize(block_index begin, u64 size);

    default_allocator::allocation_stats_t stats() const { return m_alloc.stats(); }

    /// Returns the first block index of the region.
    block_index begin() const;

    /// Returns the total number of blocks in the allocator's region.
    u64 size() const;

    /// Returns the number of blocks in this region that have been used for allocations.
    u64 used() const;

    /// TODO: inherit docs from default allocator.
    u32 min_chunk() const;
    void min_chunk(u32 chunk_size);
    u32 min_meta_chunk() const;
    void min_meta_chunk(u32 chunk_size);

    void dump(std::ostream& o) const;
    void validate() const;

protected:
    block_index do_allocate(u64 n) override;
    block_index do_reallocate(block_index a, u64 n) override;
    void do_free(block_index a) override;

private:
    struct allocator_source final : block_source {
        region_allocator* m_parent;

        allocator_source(region_allocator* parent);
        ~allocator_source();

        block_index begin() override;
        u64 available() override;
        u64 size() override;
        void grow(u64 n) override;
    };

private:
    void check_initialized() const;

private:
    anchor_handle<anchor> m_anchor;
    allocator_source m_source;
    default_allocator m_alloc;
};

} // namespace extpp

#endif // EXTPP_REGION_ALLOCATOR_HPP
