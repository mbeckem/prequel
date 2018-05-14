#ifndef EXTPP_DETAIL_FREE_LIST_HPP
#define EXTPP_DETAIL_FREE_LIST_HPP

#include <extpp/address.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>

namespace extpp {
namespace detail {

class free_list {
public:
    class anchor {
        /// Points to the first block in the list.
        block_index head;

        constexpr static auto get_binary_format() {
            return make_binary_format(&anchor::head);
        }

        friend extpp::binary_format_access;
        friend free_list;
    };

public:
    free_list(handle<anchor> anchor_, engine& engine_);

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
    handle<anchor> m_anchor;
    engine* m_engine;
    u32 m_block_capacity;
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_FREE_LIST_HPP
