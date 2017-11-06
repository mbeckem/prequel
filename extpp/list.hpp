#ifndef EXTPP_LIST_HPP
#define EXTPP_LIST_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/block.hpp>
#include <extpp/block_allocator.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/raw.hpp>
#include <extpp/type_traits.hpp>

#include <extpp/detail/sequence_insert.hpp>

#include <boost/iterator/iterator_facade.hpp>
#include <boost/optional.hpp>

namespace extpp {

template<typename T, u32 BlockSize>
class list {
public:
    using value_type = T;
    using size_type = u64;
    using difference_type = i64;

    class iterator;
    using const_iterator = iterator;

    class visitor;

    static const u32 block_size = BlockSize;

    static_assert(is_trivial<T>::value, "The value type must be trivial.");

private:
    struct node_block;

    using node_address = address<node_block, BlockSize>;

    struct node_header {
        node_address prev;   /// invalid if first.
        node_address next;   /// invalid if last.
        u32 count = 0;
    };

    struct node_block : make_array_block_t<node_header, raw<value_type>, BlockSize> {};

    using node_type = handle<node_block, BlockSize>;

    static constexpr u32 max_count = node_block::capacity;

    // Can be violated in the first and last node.
    static constexpr u32 min_count = max_count / 2;

private:
    node_type read_node(node_address ptr) const {
        return access(m_engine, ptr);
    }

    node_type create_node() {
        raw_address<BlockSize> ptr = m_alloc.allocate();
        node_type node = construct<node_block>(m_engine, ptr);

        ++m_anchor->nodes;
        m_anchor.dirty();

        return node;
    }

    void free_node(node_address ptr) {
        m_alloc.free(ptr.raw());
        --m_anchor->nodes;
        m_anchor.dirty();
    }

public:
    class anchor {
        /// The number of values in this list.
        u64 size = 0;

        /// The number of list nodes (== blocks).
        u64 nodes = 0;

        /// The first node, or invalid if empty.
        node_address first;

        /// The last node, or invalid if empty.
        node_address last;

        friend class list;
    };

public:
    list(handle<anchor, BlockSize> a, engine<BlockSize>& eng, block_allocator<BlockSize>& alloc)
        : m_anchor(std::move(a))
        , m_engine(eng)
        , m_alloc(alloc)
    {}

    list(const list& other) = delete;
    list(list&& other) noexcept = default;

    list& operator=(const list& other) = delete;
    list& operator=(list&& other) noexcept = default;

    bool empty() const { return m_anchor->size == 0; }
    u64 size() const { return m_anchor->size; }
    u64 nodes() const { return m_anchor->nodes; }

    /// Allows for visiting the list nodes and their values.
    visitor visit() const { return visitor(this); }

    /// Maximum number of values per node.
    static constexpr u32 node_capacity() { return max_count; }

    /// The average fullness of this list's nodes.
    double fill_factor() const {
        return empty() ? 0 : double(size()) / (nodes() * node_capacity());
    }

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const { return nodes() * BlockSize; }

    /// The relative overhead compared to a linear file with entries of type `T`.
    /// Computed by dividing the total size of the list by the total size of its elements.
    /// \note Because nodes are at worst only half full, this value should never be
    /// much greater than 2.
    double overhead() const {
        return empty() ? 0 : double(byte_size()) / (size() * sizeof(value_type));
    }

    void verify() const;

    /// Returns an iterator pointing to the first value,
    /// or returns `end()` if this list is empty.
    iterator begin() const {
        return empty() ? end()
                       : iterator(this, read_node(m_anchor->first), 0);
    }

    /// Returns an iterator pointing one past the end of this list
    iterator end() const { return iterator(this); }

    /// Returns an iterator pointing just before the first value.
    /// \note This iterator must not be dereferenced.
    /// \note `before_begin()` is always equal to `end()`.
    iterator before_begin() const { return end(); }

    /// Returns an iterator pointing to the last value,
    /// or returns `before_begin()` if this list is empty.
    iterator before_end() const {
        if (empty()) return before_begin();
        return --end();
    }

    /// Removes all values from this list.
    /// \post `size() == 0`.
    void clear() {
        node_address ptr = m_anchor->first;
        while (ptr) {
            node_address next = read_node(ptr)->next;
            free_node(ptr);
            ptr = next;
        }

        m_anchor->first = m_anchor->last = {};
        m_anchor->size = 0;
        m_anchor.dirty();
    }

    /// Removes the element pointed to by `pos` and returns
    /// an iterator pointing to the immediate successor of that element.
    /// Returns the end iterator if `pos` pointed to the last element.
    ///
    /// \pre `pos` points to an element of this list.
    /// \note Erasing an element invalidates all pointers and iterators.
    iterator erase(const iterator& pos) {
        EXTPP_ASSERT(pos.get_list() == this, "iterator does not belong to this list");
        EXTPP_ASSERT(pos.valid(), "iterator does not point to a list element");

        const node_type& node = pos.node();
        const u32 index = node.index();
        for (u32 i = index; index < node->count - 1; ++i)
            node->values[i] = node->values[i + 1];
        node->count -= 1;
        node.dirty();

        auto succ = [&](const node_type& node, u32 index) {
            if (index == node->count)
                return node->next ? iterator(this, read_node(node->next), 0) : end();
            return iterator(this, node, index);
        };

        if (node->count >= min_count)
            return succ(node, index);

        // The first and the last node can become completely empty.
        if (node.address() == m_anchor->first || node.address() == m_anchor->last) {
            if (node->count == 0) {
                node_address next_ptr = node->next;
                unlink(node);
                free_node(node.address());
                return next_ptr ? iterator(this, read_node(next_ptr), 0) : end();
            }
            return succ(node, index);
        }

        // Neither the first nor the last node. Either steal a single element, or,
        // if that would leave the successor too empty, merge both nodes.
        // Note: stealing from the last node is fine as long as it does not become empty,
        // even if it has fewer than `min` values.
        node_type next = read_node(node->next);
        if (next->count > min_count || (next.address() == m_anchor->last && next->count > 1)) {
            node->values[node->count] = next->values[0];
            node->count += 1;

            for (u32 i = 0; i < next->count - 1; ++i)
                next->values[i] = next->value[i + 1];
            next->count -= 1;
            next.dirty();
        } else {
            u32 j = node->count;
            for (u32 i = 0; i < next->count; ++i, ++j)
                node->values[j] = next->values[i];
            node->count = j;

            unlink(next);
            free_node(next.address());
        }

        return succ(node, index);
    }

    iterator insert(const iterator& pos, const value_type& value) {
        EXTPP_ASSERT(pos.get_list() == this, "iterator does not belong to this list");

        if (empty())
            return insert_first(value);

        node_type node;
        u32 index;
        if (!pos.valid()) {
            node = read_node(m_anchor->last);
            index = node->count;
        } else {
            node = pos.node();
            index = pos.index();
        }
        return insert_at(std::move(node), index, value);
    }

    iterator insert_after(const iterator& pos, const value_type& value) {
        EXTPP_ASSERT(pos.get_list() == this, "iterator does not belong to this list");

        if (empty())
            return insert_first(value);

        node_type node;
        u32 index;
        if (!pos.valid()) {
            node = read_node(m_anchor->first);
            index = 0;
        } else {
            node = pos.node();
            index = pos.index() + 1;
        }
        return insert_at(std::move(node), index, value);
    }

    void push_back(const value_type& value) {
        if (empty())
            return insert_first(value);

        node_type node = read_node(m_anchor->last);
        u32 index = node->count;
        insert_at(std::move(node), index, value);
    }

    void pop_back() {
        EXTPP_ASSERT(!empty(), "cannot remove from an empty list");

        // Could be optimized by making this a special case (always removing from the last node)
        // but there should be no significant performance difference.
        erase(before_end());
    }

    /// Returns a pointer that points to same element as the one referenced by `pos`.
    /// \pre `pos` points to a valid element of this list.
    handle<T, BlockSize> pointer_to(const iterator& pos) {
        return pointer_to_impl<T>(pos);
    }

    /// Returns a pointer that points to same element as the one referenced by `pos`.
    /// \pre `pos` points to a valid element of this list.
    handle<const T, BlockSize> pointer_to(const iterator& pos) const {
        return pointer_to_impl<const T>(pos);
    }

    /// Returns an iterator pointing to the same list element as `ptr`.
    /// The value *must* be stored in this list, otherwise this is undefined behaviour.
    iterator iterator_to(const handle<T, BlockSize>& ptr) const {
        return iterator_to_impl(ptr);
    }

    /// Returns an iterator pointing to the same list element as `ptr`.
    /// The value *must* be stored in this list, otherwise this is undefined behaviour.
    iterator iterator_to(const handle<const T, BlockSize>& ptr) const {
        return iterator_to_impl(ptr);
    }

private:
    /// Creates the first list node with a single value.
    iterator insert_first(const value_type& value) {
        EXTPP_ASSERT(empty(), "list must be empty");

        node_type node = create_node();
        node->count = 1;
        node->values[0] = value;

        m_anchor->first = m_anchor->last = node.address();
        m_anchor->size = 1;
        m_anchor.dirty();
        return iterator(this, std::move(node), 0);
    }

    /// Insert a new value in the specified node at the given index.
    /// If the node is full, it will be split and its entries will be redistributed.
    /// The insertion of `value` will still take place in a matter
    /// that will keep the relative order between elements.
    iterator insert_at(node_type node, u32 index, const value_type& value) {
        EXTPP_ASSERT(index <= node->count, "index is out of bounds");

        m_anchor->size += 1;
        m_anchor.dirty();

        constexpr u32 max = node_capacity();

        node.dirty();
        if (node->count < max) {
            for (u32 i = node->count; i > index; --index)
                node->values[i] = node->values[i - 1];
            node->values[index] = value;
            node->count += 1;
            return iterator(this, std::move(node), index);
        }
        EXTPP_ASSERT(node->count == max,
                    "Node must be exactly full.");

        node_type new_node = create_node();
        {
            new_node->prev = node.address();
            new_node->next = node->next;

            if (new_node->next) {
                // Current node has a successor.
                node_type next = read_node(new_node->next);
                next->prev = new_node.address();
                next.dirty();
            } else {
                // Current node is the last node.
                m_anchor->last = new_node.address();
                m_anchor.dirty();
            }
            node->next = new_node.address();
        }

        // Number of entries before the insertion.
        const u32 count = node->count;

        // The number of elements that will remain in the old node.
        const u32 mid = [&]{
            if (new_node.address() == m_anchor->last)
                return count; // Start a new node with a single element.
            if (node.address() == m_anchor->first)
                return 1;     // Same, but in the other direction.

            // Else: move half of the values to the new node.
            // +1 because of the insertion, another +1 to round up.
            return (count + 2) / 2;
        }();

        detail::sequence_insert(node->values, new_node->values, count, mid, index, value);
        node->count = mid;
        new_node->count = count + 1 - mid;
        return index < mid ? iterator(this, std::move(node), index)
                           : iterator(this, std::move(new_node), index - mid);
    }

    /// Removes a node from the linked list by altering the references
    /// of the successor / predecessor nodes.
    void unlink(const node_type& node) {
        if (node->prev) {
            node_type prev = read_node(node->prev);
            prev->next = node->next;
            prev.dirty();
        } else {
            EXTPP_ASSERT(node.address() == m_anchor->first,
                        "node must be the first one");
            m_anchor->first = node->next;
            m_anchor.dirty();
        }

        if (node->next) {
            node_type next = read_node(node->next);
            next->prev = node->prev;
            next.dirty();
        } else {
            EXTPP_ASSERT(node.address() == m_anchor->last,
                        "node must be the last one");
            m_anchor->last = node->prev;
            m_anchor.dirty();
        }
    }

    template<typename Target>
    handle<Target, BlockSize> pointer_to_impl(const iterator& pos) const {
        EXTPP_ASSERT(pos.get_list() == this, "iterator does not belong to this list");
        EXTPP_ASSERT(pos.valid(), "iterator does not point to a valid element");
        Target* value = pos().node()->values[pos.index()];
        return pos.node().neighbor(value);
    }

    template<typename Source>
    iterator iterator_to_impl(const handle<Source, BlockSize>& ptr) const {
        node_type node = cast<node_block>(ptr.block());
        u32 index = ptr.get() - node->values;
        return iterator(this, std::move(node), index);
    }

private:
    handle<anchor, BlockSize> m_anchor;
    engine<BlockSize>& m_engine;
    block_allocator<BlockSize>& m_alloc;
};

template<typename T, u32 B>
class list<T, B>::iterator : public boost::iterator_facade<
        iterator,                           // Derived
        value_type,                         // Value Type
        std::bidirectional_iterator_tag,    // Iterator Category
        const value_type&,                  // Reference type
        difference_type                     // Difference Type
    >{
private:
    /// Points to a valid instance unless this iterator has been default constructed.
    /// (Even the end iterator points to its list instance because it has to find the
    /// last node if its decremented).
    const list* m_list = nullptr;

    /// Points to the current node or is invalid if this iterator does not point
    /// to a valid list element.
    node_type m_node;

    /// If m_node points to a valid node, then this index is within bounds.
    u32 m_index = 0;

public:
    iterator() = default;

private:
    friend class list;

    /// End iterator.
    iterator(const list* ls): m_list(ls) {}

    /// Valid position.
    iterator(const list* ls, node_type node, u32 index)
        : m_list(ls)
        , m_node(std::move(node))
        , m_index(index)
    {
        EXTPP_ASSERT(node, "must be a valid node pointer");
    }

    bool valid() const { return m_node; }

    const list* get_list() const { return m_list; }

    const node_type& node() const { return m_node; }

    u32 index() const { return m_index; }

private:
    friend class boost::iterator_core_access;

    const value_type& dereference() const {
        EXTPP_ASSERT(valid(), "dereferencing an invalid iterator");
        return m_node->values[m_index];
    }

    bool equal(const iterator& other) const {
        EXTPP_ASSERT(m_list == other.m_list, "comparing iterators from different lists");
        return m_node == other.m_node && m_index == other.m_index;
    }

    void increment() {
        EXTPP_ASSERT(m_list, "default constructed iterator cannot be incremented");

        if (!m_node) {
            EXTPP_ASSERT(!m_list->empty(), "cannot visit the first element in an empty list");
            m_node = m_list->read_node(m_list->m_anchor->first);
            m_index = 0;
        } else {
            if (++m_index == m_node->count) {
                if (m_node->next) {
                    m_node = m_list->read_node(m_node->next);
                    m_index = 0;
                } else {
                    m_node.reset();
                    m_index = 0;
                }
            }
        }

        EXTPP_ASSERT(!m_node || m_index < m_node->count,
                    "Iterator is either invalid or points to a valid element");
    }

    void decrement() {
        EXTPP_ASSERT(m_list, "default constructed iterator cannot be incremented");

        if (!m_node) {
            EXTPP_ASSERT(!m_list->empty(), "cannot visit the last element in an empty list");
            m_node = m_list->read_node(m_list->m_anchor->last);
            m_index = m_node->count - 1;
        } else {
            if (m_index-- == 0) {
                if (m_node->prev) {
                    m_node = m_list->read_node(m_node->prev);
                    m_index = m_node->count - 1;
                } else {
                    m_node.reset();
                    m_index = 0;
                }
            }
        }

        EXTPP_ASSERT(!m_node || m_index < m_node->count,
                    "iterator is either invalid or points to a valid element");
    }
};

template<typename T, u32 B>
class list<T, B>::visitor {
public:
    /// Returns true if this visitor current points to a valid node.
    bool valid() const { return m_node; }

    /// Returns the address of this node.
    node_address address() const { return node().address(); }

    /// Returns true iff this node has a successor.
    bool has_next() const { return next_address(); }

    /// Returns true iff this node has a predecessor.
    bool has_previous() const { return previous_address(); }

    /// Returns the address of this node's predecessor.
    node_address next_address() const { return node()->next; }

    /// Returns the address of this node's successor.
    node_address previous_address() const { return node()->prev; }

    /// Returns the number of values in this node.
    u32 size() const { return node()->count; }

    /// Returns the value at the given index.
    /// \pre `index < size()`.
    const value_type& value(u32 index) const {
        EXTPP_ASSERT(index < size(), "index out of bounds");
        return node()->values[index];
    }

    /// Moves this visitor to the first node.
    void move_first() {
        move_node(m_list->m_anchor->first);
    }

    /// Moves this visitor to the last node.
    void move_last() {
        move_node(m_list->m_anchor->last);
    }

    /// Moves this visitor to the next node.
    void move_next() {
        move_node(next_address());
    }

    /// Move this visitor to the previous node.
    void move_previous() {
        move_node(previous_address());
    }

    visitor next() const {
        visitor v(*this);
        v.move_next();
        return v;
    }

    visitor previous() const {
        visitor v(*this);
        v.move_previous();
        return v;
    }

    visitor first() const {
        visitor v(*this);
        v.move_first();
        return v;
    }

    visitor last() const {
        visitor v(*this);
        v.move_last();
        return v;
    }

    const list* get_list() const { return m_list; }

private:
    friend class list;

    visitor(const list* ls)
        : m_list(ls)
    {
        move_first();
    }

    void move_node(node_address index) {
        if (index) {
            m_node = m_list->read_node(index);
        } else {
            m_node.reset();
        }
    }

    const node_type& node() const {
        EXTPP_ASSERT(m_node, "this visitor does not point to a valid node");
        return m_node;
    }

private:
    const list* m_list;
    node_type m_node;
};

template<typename T, u32 B>
void list<T, B>::verify() const {
    static constexpr u32 min = min_count;
    static constexpr u32 max = max_count;

    auto error = [&](const char* message) {
        // TODO Own exception type?
        throw std::logic_error(std::string("list::verify(): invariant violated (") + message + ").");
    };

    u64 node_count = 0;
    u64 value_count = 0;

    node_type current_node;
    node_type last_node;
    for (node_address current_address = m_anchor->first; current_address; ) {
        current_node = read_node(current_address);

        if (last_node) {
            // Not the first node.
            if (current_node->prev != last_node.address())
                error("current node does not point to its predecessor");
            if (last_node->next != current_address)
                error("last node does not point to its successor");
        } else {
            if (m_anchor->first != current_address)
                error("first node is not the leftmost node");
            if (current_node->prev)
                error("first node has a predecessor");
        }

        if (current_node->count> max)
            error("a list node is overflowing");
        if (current_node->count == 0)
            error("a list node is empty");
        if (m_anchor->first != current_address && m_anchor->last != current_address && current_node->count < min)
            error("a list node is underflowing");

        node_count += 1;
        value_count += current_node->count;
        current_address = current_node->next;
        last_node = current_node;
    }
    if (last_node.address() != m_anchor->last)
        error("the list's last node wasn't seen as last");

    if (value_count != m_anchor->size)
        error("wrong number of values");
    if (node_count != m_anchor->nodes)
        error("wrong number of nodes");
}

} // namespace extpp

#endif // EXTPP_LIST_HPP
