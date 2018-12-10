#ifndef PREQUEL_DETAIL_FREE_LIST_HPP
#define PREQUEL_DETAIL_FREE_LIST_HPP

#include <prequel/address.hpp>
#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/handle.hpp>

namespace prequel {
namespace detail {

class free_list {
public:
    class anchor {
        /// Points to the first block in the list.
        block_index head;

        constexpr static auto get_binary_format() { return binary_format(&anchor::head); }

        friend binary_format_access;
        friend free_list;
    };

public:
    free_list(anchor_handle<anchor> anchor_, engine& engine_);

    engine& get_engine() const { return *m_engine; }

    /// True if there are no free blocks.
    bool empty() const;

    /// Returns the number of entries per block.
    u32 block_capacity() const { return m_block_capacity; }

    /// Add a single free block to the list.
    /// The block must not be in use anywhere else.
    /// Some blocks are reused to form the list itself,
    /// so their content must not be modified, except
    /// through this list.
    void push(block_index block);

    /// Remove a single free block from the list.
    /// Throws if the list is empty.
    block_index pop();

private:
    anchor_handle<anchor> m_anchor;
    engine* m_engine;
    u32 m_block_capacity;
};

} // namespace detail
} // namespace prequel

#endif // PREQUEL_DETAIL_FREE_LIST_HPP
