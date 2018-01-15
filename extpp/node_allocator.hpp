#ifndef EXTPP_NODE_ALLOCATOR_HPP
#define EXTPP_NODE_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/handle.hpp>
#include <extpp/detail/free_list.hpp>

namespace extpp {

/// A very simple allocator that can only hand out block-sized nodes.
class node_allocator : public allocator {
public:
    class anchor {
        detail::free_list::anchor list;
        u64 total = 0;
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
    void do_free(raw_address a);

private:
    handle<anchor> m_anchor;
    detail::free_list m_list;
    u32 m_chunk_size = 32;
};


} // namespace extpp

#endif // EXTPP_NODE_ALLOCATOR_HPP
