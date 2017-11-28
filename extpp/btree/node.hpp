#ifndef EXTPP_BTREE_NODE_HPP
#define EXTPP_BTREE_NODE_HPP

#include <extpp/block.hpp>
#include <extpp/defs.hpp>
#include <extpp/address.hpp>
#include <extpp/handle.hpp>
#include <extpp/raw.hpp>
#include <extpp/detail/memory.hpp>
#include <extpp/detail/sequence_insert.hpp>

#include <tuple>

namespace extpp::detail::btree {

template<typename State>
class leaf_node;

template<typename State>
class internal_node;

template<typename State>
class leaf_node {
    struct leaf_block;

public:
    using state_type = State;

    static constexpr u32 block_size = state_type::block_size;

    using value_type = typename state_type::value_type;
    using key_type = typename state_type::key_type;

    using address_type = extpp::address<leaf_block, block_size>;

private:
    struct leaf_header {
        /// Points to the previous leaf node (to the left, lesser keys).
        address_type previous;

        /// Points to the next leaf node (to the right, greater keys).
        address_type next;

        /// The number of values in this node.
        /// \invariant `0 <= count <= max_size.
        u32 count = 0;
    };

    struct leaf_block : make_array_block_t<leaf_header, raw<value_type>, block_size> {
        static_assert(leaf_block::capacity >= 3, "block size too small (or keys too large)");
    };

public:
    using handle_type = handle<leaf_block, block_size>;

    using block_type = leaf_block;

public:
    /// Create an empty node.
    static leaf_node create(state_type& state) {
        auto addr = state.allocate_leaf();
        auto handle = construct<leaf_block>(state.engine(), addr);
        return leaf_node(std::move(handle));
    }

public:
    leaf_node() = default;

    leaf_node(handle_type handle)
        : m_block(std::move(handle))
    {}

    explicit operator bool() const { return m_block.valid(); }

    /// Maximum number of values in a leaf.
    static constexpr u32 max_size() { return leaf_block::capacity; }

    /// The minimum size of a leaf node that is *not* the root or the first or the last node.
    static constexpr u32 min_size() { return max_size() / 2; }

    address_type address() const { return m_block.address(); }

    address_type prev() const { return m_block->previous; }
    void set_prev(address_type prev) const {
        m_block->previous = prev;
        m_block.dirty();
    }

    address_type next() const { return m_block->next; }
    void set_next(address_type next) const {
        m_block->next = next;
        m_block.dirty();
    }

    value_type* begin() const { return m_block->values[0].ptr(); }
    value_type* end() const { return begin() + size(); };

    u32 size() const { return m_block->count; }

    value_type& get(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return m_block->values[index].ref();
    }

    void set(u32 index, const value_type& value) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        m_block->values[index] = value;
        m_block.dirty();
    }

    /// Returns the index of the first value `v` with `v.key >= `key`.
    u32 lower_bound(const state_type& state, const key_type& key) const {
        auto pos = std::lower_bound(begin(), end(), key, [&](const value_type& a, const key_type& b) {
            return state.key_less(state.key(a), b);
        });
        return pos - begin();
    }

    /// Returns the index of the first value `v` with `v.key > `key`.
    u32 upper_bound(const state_type& state, const key_type& key) const {
        auto pos = std::upper_bound(begin(), end(), key, [&](const key_type& a, const value_type& b) {
            return state.key_less(a, state.key(b));
        });
        return pos - begin();
    }

    /// Inserts a value at the given index.
    void insert(u32 index, const value_type& value) const {
        EXTPP_ASSERT(size() < max_size(), "Node is already full.");
        EXTPP_ASSERT(index <= size(), "Index out of bounds.");

        detail::shift(m_block->values + index, m_block->count - index, 1);
        m_block->values[index] = value;
        m_block->count += 1;
        m_block.dirty();
    }

    /// Remove the value at the given index.
    void remove(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        detail::shift(m_block->values + index + 1, m_block->count - index - 1, -1);
        m_block->count -= 1;
        m_block.dirty();
    }

    enum class split_mode {
        leftmost,
        rightmost,
        normal
    };

    /// Split the current leaf. Half of this leaf's entries will be put into the new node.
    /// The new node must be inserted in the parent to the right of this node.
    ///
    /// \pre This node must be full.
    key_type split(state_type& state, const leaf_node& right, split_mode mode) const {
        EXTPP_ASSERT(size() == max_size(), "This node must be full.");
        EXTPP_ASSERT(right.size() == 0, "New node must be empty.");

        u32 left_size = [&]() -> u32 {
            switch (mode) {
            case split_mode::rightmost: return size() - 1;
            case split_mode::leftmost: return 1;
            case split_mode::normal: return (size() + 1) / 2;
            }
            EXTPP_UNREACHABLE("Invalid mode");
        }();
        u32 right_size = size() - left_size;
        detail::copy(m_block->values + left_size, right_size, right.m_block->values);

        m_block->count = left_size;
        m_block.dirty();

        right.m_block->count = right_size;
        right.m_block.dirty();
        return state.key(get(size() - 1));
    }

    /// Takes one element from the left neighbor of this node.
    /// \param parent           The parent of this node.
    /// \param index            The index of this node in its parent.
    /// \param left             The left neighbor of this node.
    void take_left(state_type& state, const internal_node<state_type>& parent, u32 index, const leaf_node& left) const {
        EXTPP_ASSERT(index > 0, "Leaf has no left neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index-1) == left.address().raw(), "Wrong index for left neighbor.");
        EXTPP_ASSERT(left.size() > 1, "Left neighbor would be empty.");

        insert(0, left.get(left.size() - 1));
        left.remove(left.size() - 1);
        parent.set_key(index - 1, state.key(left.get(left.size() - 1)));
    }

    /// Takes one element from the right neighbor of this node.
    /// \param parent           The parent of this node.
    /// \param index            The index of this node in its parent.
    /// \param left             The left neighbor of this node.
    void take_right(state_type& state, const internal_node<state_type>& parent, u32 index, const leaf_node& right) const {
        EXTPP_ASSERT(index < parent.size() - 1, "Leaf has no right neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index+1) == right.address().raw(), "Wrong index for right neighbor.");
        EXTPP_ASSERT(right.size() > 1, "Right neighbor would be empty.");

        insert(size(), right.get(0));
        right.remove(0);
        parent.set_key(index, state.key(get(size() - 1)));
    }

    /// Merges the content of the left node into this node. The left node is not modified.
    void merge_left(state_type& state, const internal_node<State>& parent, u32 index, const leaf_node& left) const {
        EXTPP_ASSERT(index > 0, "Node has no left neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index - 1) == left.address().raw(), "Wrong index for left neighbor.");
        EXTPP_ASSERT(size() + left.size() <= max_size(), "Too many elements for merging.");
        EXTPP_ASSERT(size() > 0, "This node is empty.");
        EXTPP_ASSERT(left.size() > 0, "The other node is empty.");
        unused(state);

        detail::shift(m_block->values, size(), left.size());
        detail::copy(left.m_block->values, left.size(), m_block->values);
        m_block->count += left.size();
        m_block.dirty();
    }

    const handle_type& block() const { return m_block; }


private:
    handle_type m_block;
};

template<typename State>
class internal_node {
    struct internal_block;

public:
    using state_type = State;

    static constexpr u32 block_size = state_type::block_size;

    using key_type = typename state_type::key_type;
    using node_address = typename state_type::node_address;
    using address_type = extpp::address<internal_block, block_size>;

private:
    struct internal_header {
        /// Number of children.
        /// There are `count - 1` keys.
        /// \invariant `2 <= count <= max_size()`.
        u32 count = 0;
    };

    template<u32 N>
    struct internal_proto : internal_header {
        raw<key_type> keys[N - 1];
        raw<node_address> children[N];
    };

    static constexpr u32 internal_size_estimate() {
        constexpr u32 ptr_size = sizeof(node_address);

        static_assert(block_size >= sizeof(internal_header) + ptr_size, "header does not fit into block");
        u32 remaining = block_size - sizeof(internal_header);
        u32 estimate = (remaining + sizeof(key_type)) / (sizeof(key_type) + sizeof(ptr_size));
        return estimate;
    }

    struct internal_block : make_variable_block_t<internal_proto, block_size, internal_size_estimate()> {
        static_assert(internal_block::capacity >= 4, "block size too small (or keys too large)");
    };


public:
    using handle_type = handle<internal_block, block_size>;

    using block_type = internal_block;

    static internal_node create_root(state_type& state, node_address left,
                                     const key_type& split, node_address right) {
        auto node = create(state);
        node.m_block->children[0] = left;
        node.m_block->children[1] = right;
        node.m_block->keys[0] = split;
        node.m_block->count = 2;
        node.m_block.dirty();
        return node;
    }

    static internal_node create(state_type& state) {
        auto addr = state.allocate_internal();
        auto handle = construct<internal_block>(state.engine(), addr);
        return internal_node(std::move(handle));
    }

public:
    internal_node() = default; // Nullptr

    internal_node(handle_type handle)
        : m_block(std::move(handle))
    {}

    explicit operator bool() const { return m_block.valid(); }

    /// Maximum number of children.
    static constexpr u32 max_size() { return internal_block::capacity - 1; }

    /// Minimum number of children.
    static constexpr u32 min_size() { return max_size() / 2; }

    address_type address() const { return m_block.address(); }

    key_type* begin() const { return &m_block->keys[0].value; }

    key_type* end() const {
        EXTPP_ASSERT(size() > 0, "Internal node is empty.");
        return begin() + size() - 1;
    }

    node_address* children_begin() const { return m_block->children[0].ptr(); }

    node_address* children_end() const { return children_begin() + size(); }

    u32 index(key_type* pos) const { return pos - begin(); }

    u32 child_index(node_address* pos) const { return pos - children_begin(); }

    u32 size() const { return m_block->count; }

    /// Returns the position of the (first) child that contains values greater than or equal to `key`.
    u32 lower_bound(const state_type& state, const key_type& key) const {
        EXTPP_ASSERT(size() > 0, "internal node is empty");
        auto pos = std::lower_bound(begin(), end(), key, [&](const key_type& a, const key_type& b) {
            return state.key_less(a, b);
        });
        return index(pos);
    }

    /// Returns the position of the (first) child that contains values greater than `key`.
    u32 upper_bound(const state_type& state, const key_type& key) const {
        EXTPP_ASSERT(size() > 0, "internal node is empty");
        auto pos = std::upper_bound(begin(), end(), key, [&](const key_type& a, const key_type& b) {
            return state.key_less(a, b);
        });
        return index(pos);
    }

    void set_key(u32 index, const key_type& key) const {
        EXTPP_ASSERT(size() > 0, "Internal node is empty.");
        EXTPP_ASSERT(index < size() - 1, "Index out of bounds.");
        m_block->keys[index] = key;
        m_block.dirty();
    }

    key_type& get_key(u32 index) const {
        EXTPP_ASSERT(size() > 0, "Internal node is empty.");
        EXTPP_ASSERT(index < size() - 1, "Index out of bounds.");
        return m_block->keys[index];
    }

    node_address get_child(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return m_block->children[index];
    }

    /// Inserts a new (key, child) pair into the node.
    /// This function is used to apply the result of a node split in the parent.
    /// After calling this function, `keys[index - 1]` will be equal to `key`
    /// and `children[index]` will be equal to `child`.
    ///
    /// \pre `1 <= index <= count`.
    void insert_split_result(u32 index, const key_type& key, node_address child) const {
        EXTPP_ASSERT(m_block->count < max_size(), "inserting into a full node");
        EXTPP_ASSERT(index >= 1 && index <= m_block->count, "index out of bounds");

        // TODO: Clean up.
        auto kp = begin() + index - 1;
        auto ke = end();
        auto cp = children_begin() + index;
        auto ce = children_end();
        std::copy_backward(kp, ke, ke + 1);
        std::copy_backward(cp, ce, ce + 1);
        *kp = key;
        *cp = child;
        ++m_block->count;

        m_block.dirty();
    }

    /// Insert one (key, child) pair at the start.
    void insert_front(const key_type& key, node_address child) const {
        EXTPP_ASSERT(size() < max_size(), "Node is full.");
        detail::shift(m_block->keys, size() - 1, 1);
        detail::shift(m_block->children, size(), 1);
        m_block->keys[0] = key;
        m_block->children[0] = child;
        m_block->count += 1;
        m_block.dirty();
    }

    /// Insert one (key, child) pair at the end.
    void insert_back(const key_type& key, const node_address child) const {
        EXTPP_ASSERT(size() < max_size(), "Node is full.");
        m_block->keys[size() - 1] = key;
        m_block->children[size()] = child;
        m_block->count += 1;
        m_block.dirty();
    }

    /// Removes the first key and child.
    void remove_front() const {
        EXTPP_ASSERT(size() > 1, "Node would be empty.");
        detail::shift(m_block->keys + 1, size() - 2, -1);
        detail::shift(m_block->children + 1, size() - 1, -1);
        m_block->count -= 1;
        m_block.dirty();
    }

    /// Removes the last key and child.
    void remove_back() const {
        EXTPP_ASSERT(size() > 1, "Node would be empty.");
        m_block->children[size() - 1] = node_address();
        m_block->count -= 1;
        m_block.dirty();
    }

    /// Removes the child at the given index.
    /// If there is a key associated with that child, it is also removed.
    void remove(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");

        detail::shift(m_block->children + index + 1, size() - index - 1, -1);
        if (index < size() - 1)
            detail::shift(m_block->keys + index + 1, size() - index - 2, - 1);

        m_block->count -= 1;
        m_block.dirty();
    }

    key_type split(state_type& state, const internal_node& right) const {
        EXTPP_ASSERT(size() == max_size(), "Internal node must be full");
        EXTPP_ASSERT(right.size() == 0, "New node must be empty.");
        unused(state);

        u32 left_size = (size() + 1) / 2;
        u32 right_size = size() - left_size;

        detail::copy(m_block->keys + left_size, right_size - 1, right.m_block->keys);
        detail::copy(m_block->children + left_size, right_size, right.m_block->children);

        m_block->count = left_size;
        m_block.dirty();

        right.m_block->count = right_size;
        right.m_block.dirty();
        return m_block->keys[left_size - 1];
    }

    /// Takes one element from the left neighbor of this node.
    /// \param parent           The parent of this node.
    /// \param index            The index of this node in its parent.
    /// \param left             The left neighbor of this node.
    void take_left(state_type& state, const internal_node& parent, u32 index, const internal_node& left) const {
        EXTPP_ASSERT(index > 0, "Leaf has no left neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index-1) == left.address().raw(), "Wrong index for left neighbor.");
        EXTPP_ASSERT(left.size() > 1, "Left neighbor would be empty.");
        unused(state);

        // Key of the last child in the left neighbor is stored in the parent.
        insert_front(parent.get_key(index - 1), left.get_child(left.size() - 1));
        parent.set_key(index - 1, left.get_key(left.size() - 2));
        left.remove_back();
    }

    /// Takes one element from the right neighbor of this node.
    /// \param parent           The parent of this node.
    /// \param index            The index of this node in its parent.
    /// \param left             The left neighbor of this node.
    void take_right(state_type& state, const internal_node& parent, u32 index, const internal_node& right) const {
        EXTPP_ASSERT(index < parent.size() - 1, "Node has no right neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index+1) == right.address().raw(), "Wrong index for right neighbor.");
        EXTPP_ASSERT(right.size() > 1, "Right neighbor would be empty.");
        unused(state);

        // Key of the last child in this node is stored in the parent.
        insert_back(parent.get_key(index), right.get_child(0));
        parent.set_key(index, right.get_key(0));
        right.remove_front();
    }

    /// Merges the content of the left node into this node. The left node is not modified.
    void merge_left(state_type& state, const internal_node& parent, u32 index, const internal_node& left) const {
        EXTPP_ASSERT(index > 0, "Node has no left neighbor in its parent.");
        EXTPP_ASSERT(parent.get_child(index) == address().raw(), "Wrong index.");
        EXTPP_ASSERT(parent.get_child(index - 1) == left.address().raw(), "Wrong index for left neighbor.");
        EXTPP_ASSERT(size() + left.size() <= max_size(), "Too many elements for merging.");
        EXTPP_ASSERT(size() > 0, "This node is empty.");
        EXTPP_ASSERT(left.size() > 0, "The other node is empty.");
        unused(state);

        detail::shift(m_block->keys, size() - 1, left.size());
        detail::shift(m_block->children, size(), left.size());

        detail::copy(left.m_block->keys, left.size() - 1, m_block->keys);
        m_block->keys[left.size() - 1] = parent.get_key(index - 1);
        detail::copy(left.m_block->children, left.size(), m_block->children);
        m_block->count += left.size();
        m_block.dirty();
    }

private:
    handle_type m_block;
};

} // namespace extpp::detail::btree

#endif // EXTPP_BTREE_NODE_HPP
