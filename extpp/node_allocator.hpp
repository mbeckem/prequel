#ifndef EXTPP_NODE_ALLOCATOR_HPP
#define EXTPP_NODE_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/handle.hpp>
#include <extpp/detail/free_list.hpp>

namespace extpp {

/// A very simple allocator that can only hand out block-sized nodes.
/// In other words, the only supported allocation size is 1 and
/// the only supported reallocation sizes are 1 and 0.
///
/// This allocator can be used for very simple node based containers,
/// such as lists and btrees.
///
/// TODO: pimpl
class node_allocator : public allocator {
public:
    class anchor {
        // Free'd blocks are put on the free list.
        detail::free_list::anchor list;

        // Total number of allocated blocks.
        u64 total = 0;

        // Total number of free blocks.
        u64 free = 0;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::list, &anchor::total, &anchor::free);
        }

        friend node_allocator;
        friend binary_format_access;
    };

public:
    node_allocator(handle<anchor> anchor_, engine& engine_);

    /// Number of blocks allocated at once.
    u32 chunk_size() const { return m_chunk_size; }
    void chunk_size(u32 size);

    /// Total number of blocks managed by this allocator.
    u64 data_total() const;

    /// Number of blocks in use (allocated but not freed).
    u64 data_used() const;

    /// Number of free blocks.
    u64 data_free() const;

private:
    raw_address do_allocate(u64 n) override;
    raw_address do_reallocate(raw_address a, u64 n) override;
    void do_free(raw_address a) override;

private:
    handle<anchor> m_anchor;
    detail::free_list m_list;
    u32 m_chunk_size = 32;
};


} // namespace extpp

#endif // EXTPP_NODE_ALLOCATOR_HPP
