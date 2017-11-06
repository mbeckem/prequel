#ifndef EXTPP_STACK_HPP
#define EXTPP_STACK_HPP

#include <extpp/address.hpp>
#include <extpp/allocator.hpp>
#include <extpp/block.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/raw.hpp>
#include <extpp/type_traits.hpp>

#include <boost/iterator/iterator_facade.hpp>

namespace extpp {

template<typename T, u32 BlockSize>
class stack {
public:
    using value_type = T;
    using size_type = u64;

    class iterator;
    using const_iterator = iterator;

    class visitor;

    static constexpr u32 block_size = BlockSize;

    static_assert(is_trivial<T>::value, "The value type must be trivial.");

private:
    struct node_block;

    using node_address = address<node_block, BlockSize>;

    struct node_header {
        /// Points to the previous block.
        node_address prev;

        /// Number of values in this node.
        u32 count = 0;
    };

    struct node_block : make_array_block_t<node_header, raw<T>, BlockSize> {
        bool empty() { return this->count == 0; }

        bool full() { return this->count == node_block::capacity; }

        void pop() {
            EXTPP_ASSERT(!empty(), "cannot pop from an empty block");
            this->count -= 1;
        }

        void push(const value_type& value) {
            EXTPP_ASSERT(!full(), "cannot push into a full block.");
            this->values[this->count] = value;
            this->count += 1;
        }
    };

    using node_type = handle<node_block, BlockSize>;

public:
    class anchor {
        /// The total number of elements in this stack.
        u64 size = 0;

        /// The total number of nodes.
        u64 nodes = 0;

        /// Points to the last block. This block might be empty
        /// to facilitate better buffering.
        node_address last;

        friend class stack;
    };

public:
    stack(handle<anchor, BlockSize> anc, extpp::engine<BlockSize>& eng, extpp::allocator<BlockSize>& alloc)
        : m_anchor(std::move(anc))
        , m_engine(&eng)
        , m_alloc(&alloc)
    {
        // TODO: These blocks could be lazily loaded only when required.
        if (m_anchor->last) {
            m_buf[0] = access(m_anchor->last);
            if (m_buf[0]->prev) {
                m_buf[1] = m_buf[0];
                m_buf[0] = access(m_buf[1]->prev);
            }
        }
    }

    stack(const stack&) = delete;
    stack(stack&&) noexcept = default;

    stack& operator=(const stack&) = delete;
    stack& operator=(stack&&) noexcept = default;

    extpp::engine<BlockSize>& engine() const { return *m_engine; }
    extpp::allocator<BlockSize>& allocator() const { return *m_alloc; }

    bool empty() const { return m_anchor->size == 0; }
    u64 size() const { return m_anchor->size; }
    u64 nodes() const { return m_anchor->nodes; }

    /// Maximum number of values per node.
    static constexpr u32 node_capacity() { return node_block::capacity; }

    /// The average fullness of this stacks's nodes.
    double fill_factor() const {
        return empty() ? 0 : double(size()) / (nodes() * node_capacity());
    }

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const { return nodes() * BlockSize; }

    std::array<raw_address<BlockSize>, 2> buffered() const {
        return {
            m_buf[0] ? m_buf[0].address().raw() : raw_address<BlockSize>(),
            m_buf[1] ? m_buf[1].address().raw() : raw_address<BlockSize>(),
        };
    }

    /// Allows for (read-only) inspection of the stack's nodes.
    visitor visit() const { return visitor(this); }

    /// Returns an iterator to the first element (i.e. the top)
    /// or `end()` if the stack is empty.
    iterator begin() const {
        buffer_invariants();

        if (empty())
            return end();
        if (!m_buf[1]->empty())
            return iterator(this, m_buf[1], m_buf[1]->count - 1);
        return iterator(this, m_buf[0], m_buf[0]->count - 1);
    }

    /// Returns the past-the-end iterator.
    iterator end() const { return iterator(this); }

    const value_type& top() const {
        EXTPP_ASSERT(!empty(), "Cannot get the top element from an empty stack.");
        buffer_invariants();

        if (m_buf[1] && !m_buf[1]->empty())
            return m_buf[1]->values[m_buf[1]->count - 1];
        return m_buf[0]->values[m_buf[0]->count - 1];
    }

    void push(const value_type& value) {
        buffer_invariants();

        if (!m_buf[0]) {
            m_buf[0] = create();

            m_anchor->last = m_buf[0].address();
            m_anchor.dirty();
        }

        m_anchor->size += 1;
        m_anchor.dirty();

        if (!m_buf[0]->full()) {
            m_buf[0]->push(value);
            m_buf[0].dirty();
            return;
        }

        if (!m_buf[1]) {
            m_buf[1] = create();
            m_buf[1]->prev = m_buf[0].address();

            m_anchor->last = m_buf[1].address();
            m_anchor.dirty();
        } else if (m_buf[1]->full()) {
            m_buf[0] = std::move(m_buf[1]);
            m_buf[1] = create();
            m_buf[1]->prev = m_buf[0].address();

            m_anchor->last = m_buf[1].address();
            m_anchor.dirty();
        }

        m_buf[1]->push(value);
        m_buf[1].dirty();
        return;
    }

    void pop() {
        EXTPP_ASSERT(!empty(), "Cannot pop from an empty stack.");
        buffer_invariants();

        m_anchor->size -= 1;
        m_anchor.dirty();

        if (m_buf[1] && !m_buf[1]->empty()) {
            m_buf[1]->pop();
            m_buf[1].dirty();
            return;
        }

        EXTPP_ASSERT(m_buf[0] && !m_buf[0]->empty(),
                "The first buffer block cannot be empty.");
        m_buf[0]->pop();
        m_buf[0].dirty();

        if (m_buf[0]->empty()) {
            if (m_buf[1]) {
                destroy(m_buf[1]);
                m_buf[1].reset();

                m_anchor->last = m_buf[0].address();
                m_anchor.dirty();
            }

            if (m_buf[0]->prev) {
                m_buf[1] = std::move(m_buf[0]);
                m_buf[0] = access(m_buf[1]->prev);
            } else {
                destroy(m_buf[0]);
                m_buf[0].reset();

                m_anchor->last = {};
                m_anchor.dirty();
            }
        }
    }

    void clear() {
        for (node_address ptr = m_anchor->last; ptr; ) {
            node_type node = access(ptr);
            ptr = node->prev;
            free(node.address());
        }
        m_anchor->nodes = 0;
        m_anchor->last = {};
        m_anchor->size = 0;
        m_anchor.dirty();
    }

private:
    node_type create() const {
        raw_address<BlockSize> node = m_alloc->allocate(1);
        m_anchor->nodes += 1;
        m_anchor.dirty();

        return construct<node_block>(*m_engine, node);
    }

    void destroy(const node_type& node) {
        m_alloc->free(node.address().raw());
        m_anchor->nodes -= 1;
        m_anchor.dirty();
    }

    void buffer_invariants() const {
#ifdef EXTPP_DEBUG
        if (empty()) {
            EXTPP_ASSERT(!m_buf[0] && !m_buf[1], "no blocks can be buffered.");
            return;
        }

        if (nodes() == 1) {
            EXTPP_ASSERT(m_buf[0] && m_buf[0].address() == m_anchor->last,
                    "Must buffer the last node.");
            EXTPP_ASSERT(!m_buf[1],
                    "Must not buffer any other node.");
            return;
        }

        EXTPP_ASSERT(m_buf[1] && m_buf[1].address() == m_anchor->last,
                "Must buffer the last node.");
        EXTPP_ASSERT(m_buf[0] && m_buf[0].address() == m_buf[1]->prev,
                "Must buffer the second to last node.");
        EXTPP_ASSERT(m_buf[0].address() == m_buf[1]->prev,
                "Last node must point to second to last node");
        EXTPP_ASSERT(!m_buf[0]->empty(),
                "The first buffered block cannot be empty.");
#endif
    }

    node_type access(node_address node) const { return extpp::access(*m_engine, node); }

private:
    handle<anchor, BlockSize> m_anchor;
    extpp::engine<BlockSize>* m_engine;
    extpp::allocator<BlockSize>* m_alloc;

    /// The top two blocks are pinned and used as buffers.
    /// The topmost block might be empty.
    std::array<node_type, 2> m_buf;
};

template<typename T, u32 B>
class stack<T, B>::visitor {
public:
    visitor() = default;

    /// True if this visitor currently points to a valid stack node.
    bool valid() const { return m_node.valid(); }
    explicit operator bool() const { return valid(); }

    node_address address() const { return node().block().address(); }

    bool has_previous() const { return previous_address(); }
    node_address previous_address() const { return node()->prev; }

    u32 size() const { return node()->count; }

    const value_type& value(u32 index) const {
        EXTPP_ASSERT(index < size(), "index out of bounds");
        return m_node->values[index];
    }

    void move_last() {
        move_node(m_stack->m_anchor->last);
    }

    void move_previous() {
        move_node(previous_address());
    }

    visitor previous() const {
        visitor v = *this;
        v.move_previous();
        return v;
    }

    visitor last() const {
        visitor v = *this;
        v.move_last();
        return v;
    }

private:
    friend class stack;

    visitor(const stack* s)
        : m_stack(s)
    {
        move_node(m_stack->m_anchor->last);
    }

    const node_type& node() const {
        EXTPP_ASSERT(m_node, "this visitor does not point to a valid node");
        return m_node;
    }

    void move_node(node_address index) {
        m_node = index ? m_stack->access(index) : node_type();
    }

private:
    const stack* m_stack = nullptr;
    node_type m_node;
};

template<typename T, u32 B>
class stack<T, B>::iterator : public boost::iterator_facade<
        iterator,
        value_type,
        std::forward_iterator_tag,
        const value_type&
> {
    const stack* m_stack = nullptr;
    node_type m_node;
    u32 m_index = 0;

public:
    iterator() = default;

private:
    friend class stack;

    // past-the-end
    iterator(const stack* s)
        : m_stack(s)
    {}

    // points to a valid element
    iterator(const stack* s, node_type node, u32 index)
        : m_stack(s)
        , m_node(std::move(node))
        , m_index(index)
    {
        EXTPP_ASSERT(m_node, "node must be valid");
        EXTPP_ASSERT(index < m_node->count, "index must be within bounds");
    }

    const stack* get_stack() const { return m_stack; }

    const node_type& node() const {
        EXTPP_ASSERT(m_node, "invalid iterator");
        return m_node;
    }

    u32 index() const {
        EXTPP_ASSERT(m_node, "invalid iterator");
        return m_index;
    }

private:
    friend class boost::iterator_core_access;

    const value_type& dereference() const {
        return node()->values[index()];
    }

    bool equal(const iterator& other) const {
        EXTPP_ASSERT(m_stack == other.m_stack, "The iterators belong to different stacks.");
        return m_node == other.m_node && m_index == other.m_index;
    }

    void increment() {
        EXTPP_ASSERT(m_stack, "incrementing invalid iterator");
        EXTPP_ASSERT(m_node, "incrementing past-the-end iterator");

        if (m_index-- == 0) {
            if (node_address prev = m_node->prev) {
                m_node = m_stack->access(prev);
                m_index = m_node->count - 1;
            } else {
                m_node.reset();
                m_index = 0;
            }
        }
        EXTPP_ASSERT(!m_node || m_index < m_node->count,
                     "either past-the-end or a valid iterator");
    }
};

} // namespace extpp

#endif // EXTPP_STACK_HPP
