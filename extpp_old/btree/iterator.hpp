#ifndef EXTPP_BTREE_ITERATOR_HPP
#define EXTPP_BTREE_ITERATOR_HPP

#include <extpp/address.hpp>
#include <extpp/defs.hpp>

#include <boost/iterator/iterator_facade.hpp>

namespace extpp::btree_detail {

/// An iterator points to a single value inside the btree or past the end.
/// Iterators contain a reference to the leaf node that contains their value; the
/// block will be kept in memory for at least as long as the iterator exists.
template<typename State>
class iterator : public boost::iterator_facade<
    iterator<State>,                    // Derived
    typename State::value_type,         // Value Type
    std::bidirectional_iterator_tag,    // Iterator Category
    const typename State::value_type&   // Reference type
>
{
    using state_type = State;
    using leaf_type = typename state_type::leaf_type;
    using leaf_address = typename state_type::leaf_address;

private:
    const state_type* m_state = nullptr;

    /// Points to a valid leaf or null if this is an invalid iterator.
    leaf_type m_leaf;

    /// If m_leaf points to a valid leaf, then this index is in bounds.
    u32 m_index = 0;

public:
    /// The default-constructed iterator is invalid.
    iterator() = default;

    /// Constructs the past-the-end iterator.
    iterator(const state_type& state)
        : m_state(&state) {}

    /// Constructs a valid iterator to some element.
    iterator(const state_type& state, leaf_type leaf, u32 index)
        : m_state(&state)
        , m_leaf(std::move(leaf))
        , m_index(index)
    {
        EXTPP_ASSERT(m_leaf, "must be a valid leaf pointer");
        EXTPP_ASSERT(m_index < m_leaf.size(), "index must be within bounds");
    }

public:
    const state_type* state() const { return m_state; }

    const leaf_type& leaf() const {
        EXTPP_ASSERT(m_leaf, "invalid iterator");
        return m_leaf;
    }

    leaf_address address() const { return m_leaf.address(); }

    u32 index() const { return m_index; }

private:
    friend class boost::iterator_core_access;

    const typename state_type::value_type& dereference() const {
        return leaf().get(m_index);
    }

    bool equal(const iterator& other) const {
        EXTPP_ASSERT(m_state == other.m_state, "comparing iterator of different trees.");
        return m_leaf.address() == other.m_leaf.address() && m_index == other.m_index;
    }

    void increment() {
        EXTPP_ASSERT(m_state, "incrementing invalid iterator");

        if (m_leaf) {
            if (++m_index == m_leaf.size()) {
                leaf_address next = m_leaf.next();
                if (next) {
                    m_leaf = m_state->access(next);
                    m_index = 0;
                } else {
                    m_leaf = {};
                    m_index = 0;
                }
            }
        } else {
            // Go from the past-the-end iterator to the first entry of the first leaf.
            leaf_address ptr = m_state->get_anchor()->leftmost;
            EXTPP_ASSERT(ptr, "incrementing past-the-end iterator on an empty tree");
            m_leaf = m_state->access(ptr);
            m_index = 0;
        }
        EXTPP_ASSERT(!m_leaf || m_index < m_leaf.size(), "either past-the-end or a valid position");
    }

    void decrement() {
        EXTPP_ASSERT(m_state, "decrementing invalid iterator");

        if (m_leaf) {
            if (m_index-- == 0) {
                leaf_address prev = m_leaf.prev();
                if (prev) {
                    m_leaf = m_state->access(prev);
                    m_index = m_leaf.size() - 1;
                } else {
                    m_leaf = {};
                    m_index = 0;
                }
            }
        } else {
            // Go from the past-the-end iterator to the last entry of the last leaf.
            leaf_address ptr = m_state->get_anchor()->rightmost;
            EXTPP_ASSERT(ptr, "decrementing past-the-end iterator on an empty tree");
            m_leaf = m_state->access(ptr);
            m_index = m_leaf.size() - 1;
        }

        EXTPP_ASSERT(!m_leaf || m_index < m_leaf.size(), "either past-the-end or a valid position");
    }
};

} // namespace extpp::btree_detail

#endif // EXTPP_BTREE_ITERATOR_HPP
