#ifndef EXTPP_DETAIL_FREE_LIST_HPP
#define EXTPP_DETAIL_FREE_LIST_HPP

#include <extpp/address.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/block.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>

namespace extpp {
namespace detail {

/// A block storage based free list.
template<u32 BlockSize>
class free_list {
private:
    struct node_block;

    struct node_header {
        /// Points to the next block in the free list.
        address<node_block> next;

        /// Number of (used) entries in the `free` array.
        u32 count = 0;
    };

    struct node_block : make_array_block_t<node_header, raw_address, BlockSize> {
        bool full() { return this->count == node_block::capacity; }
        bool empty() { return this->count == 0; }

        void push(raw_address addr) {
            EXTPP_ASSERT(!full(), "node is full");
            this->values[this->count++] = addr;
        }

        raw_address pop() {
            EXTPP_ASSERT(!empty(), "node is empty");
            return this->values[--this->count];
        }
    };

    using node_type = handle<node_block>;

public:
    class anchor {
        /// Points to the first block in the list.
        address<node_block> head;

        friend class free_list;
    };

public:
    explicit free_list(anchor_ptr<anchor> a, engine& e)
        : m_anchor(std::move(a))
        , m_engine(&e)
    {
        EXTPP_CHECK(e.block_size() >= BlockSize, "Incompatible block size.");
    }

    /// True if there are no free blocks.
    bool empty() const { return !m_anchor->head; }

    /// Returns the maximum number of entries in a single block.
    static constexpr u32 block_capacity() { return node_block::capacity; }

    /// Add a single free block to the list.
    /// The block must not be in use anywhere else.
    /// Some blocks are reused to form the list itself,
    /// so their content must not be modified, except
    /// through this list.
    void push(raw_address block) {
        if (m_anchor->head) {
            node_type node = access(*m_engine, m_anchor->head);
            if (!node->full()) {
                node->push(block);
                node.dirty();
                return;
            }
        }

        // Reuse the current block to form a new, empty list node.
        node_type node = construct<node_block>(*m_engine, block);
        node->next = m_anchor->head;
        m_anchor->head = node.address();
        m_anchor.dirty();
    }

    /// Remove a single free block from the list.
    /// Throws if the list is empty.
    raw_address pop() {
        if (!m_anchor->head)
            throw std::logic_error("free_list::pop(): list is empty.");

        node_type node = access(*m_engine, m_anchor->head);
        if (!node->empty()) {
            raw_address result = node->pop();
            node.dirty();
            return result;
        }

        // use the empty list node itself to satisfy the request.
        raw_address result = m_anchor->head.raw();
        m_anchor->head = node->next;
        m_anchor.dirty();
        return result;
    }

    free_list(free_list&& other) noexcept = default;
    free_list& operator=(free_list&&) noexcept = default;

private:
    anchor_ptr<anchor> m_anchor;
    engine* m_engine;
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_FREE_LIST_HPP
