#ifndef EXTPP_BTREE_HPP
#define EXTPP_BTREE_HPP

#include <extpp/allocator.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/block.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/raw.hpp>
#include <extpp/type_traits.hpp>

#include <extpp/detail/iter_tools.hpp>
#include <extpp/detail/safe_iterator.hpp>
#include <extpp/detail/sequence_insert.hpp>

#include <boost/container/small_vector.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/optional.hpp>

#include <algorithm>
#include <type_traits>

namespace extpp {

/// An implementation of a disk-based bplus-tree.
///
/// \tparam Value
///     The value type stored by this tree. Values are kept in sorted order.
/// \tparam KeyExtract
///     A function object that, when given a reference to a value, returns
///     a key (or a const-reference to a key) derived from the value.
///     Key objects are used to index the values stored in the tree.
/// \tparam KeyCompare
///     Takes two const-references (a, b) to key objects and returns true iff a < b.
/// \tparam BlockSize
///     The block size of the underlying storage engine. Must be a power of two.
template<typename Value, typename KeyExtract, typename KeyCompare, u32 BlockSize>
class btree {
public:
    using value_type = Value;
    using key_type = std::decay_t<std::result_of_t<KeyExtract(Value)>>;

    class iterator;
    using const_iterator = iterator;

    class cursor;
    using const_cursor = cursor;

    using key_compare = KeyCompare;
    using size_type = u64;

    class visitor;

    static constexpr u32 block_size = BlockSize;

    static_assert(is_trivial<value_type>::value,
                  "The value type must be trivial.");
    static_assert(is_trivial<key_type>::value,
                  "The key type must be trivial.");

private:
    using cursor_map = detail::safe_iterator_map<btree, cursor>;

    using cursor_buffer_t = boost::container::small_vector<cursor*, 16>;

private:
    struct node_header;
    struct leaf_block;
    struct internal_block;

    using node_address = address<node_header, BlockSize>;
    using leaf_address = address<leaf_block, BlockSize>;
    using internal_address = address<internal_block, BlockSize>;

    struct node_header {
        /// Points to the parent of this node (if any).
        node_address parent;
    };

    struct leaf_header : node_header {
        /// Points to the previous leaf node (to the left, lesser keys).
        leaf_address previous;

        /// Points to the next leaf node (to the right, greater keys).
        leaf_address next;

        /// The number of values in this node.
        /// \invariant `0 <= count <= max_size.
        u32 count = 0;
    };

    struct internal_header : node_header {
        /// Number of children.
        /// There are `count - 1` keys.
        /// \invariant `2 <= count <= max_size()`.
        u32 count = 0;
    };

    struct leaf_block : make_array_block_t<leaf_header, raw<value_type>, BlockSize> {
        using iterator = Value*;

        iterator begin() { return this->values[0].ptr(); }

        iterator end() { return begin() + this->count; }

        static constexpr u32 max_size() { return leaf_block::capacity; }

        static constexpr u32 min_size() { return (max_size() + 1) / 2; }

        u32 index(iterator pos) { return pos - begin(); }

        void insert(u32 index, const value_type& value) {
            EXTPP_ASSERT(this->count < max_size(), "node is already full");
            EXTPP_ASSERT(index <= this->count, "index out of bounds");
            shift(this->values + index, this->count - index, 1);
            this->values[index] = value;
            this->count += 1;
        }

        void remove(u32 index) {
            EXTPP_ASSERT(index < this->count, "index out of bounds");
            shift(this->values + index + 1, this->count - index - 1, -1);
            this->count -= 1;
        }
    };

    template<u32 N>
    struct internal_proto : internal_header {
        raw<key_type> keys[N - 1];
        raw<node_address> children[N];
    };

    static constexpr u32 internal_size_estimate() {
        constexpr u32 ptr_size = sizeof(node_address);

        static_assert(BlockSize >= sizeof(internal_header) + ptr_size, "header does not fit into block");
        u32 remaining = BlockSize - sizeof(internal_header);
        u32 estimate = (remaining + sizeof(key_type)) / (sizeof(key_type) + sizeof(ptr_size));
        return estimate;
    }

    struct internal_block : make_variable_block_t<internal_proto, BlockSize, internal_size_estimate()> {
        static_assert(internal_block::capacity >= 2, "block size too small (or keys too large)");

        using iterator = key_type*;

        /// Maximum number of children.
        static constexpr u32 max_size() { return internal_block::capacity - 1; }

        /// Minimum number of children.
        static constexpr u32 min_size() { return (max_size() + 1) / 2; }

        iterator begin() { return &this->keys[0].value; }

        iterator end() { return begin() + this->count - 1; }

        node_address* children_begin() { return this->children[0].ptr(); }

        node_address* children_end() { return children_begin() + this->count; }

        u32 index(iterator pos) { return pos - begin(); }

        u32 child_index(node_address* pos) { return pos - children_begin(); }

        /// Inserts a new (key, child) pair into the node.
        /// This function is used to apply the result of a node split in the parent.
        /// After calling this function, `keys[index - 1]` will be equal to `key`
        /// and `children[index]` will be equal to `child`.
        ///
        /// \pre `1 <= index <= count`.
        void apply_child_split(u32 index, const key_type& key, node_address child) {
            EXTPP_ASSERT(this->count < max_size(), "inserting into a full node");
            EXTPP_ASSERT(index >= 1 && index <= this->count, "index out of bounds");

            // TODO: Clean up.
            auto kp = begin() + index - 1;
            auto ke = end();
            auto cp = children_begin() + index;
            auto ce = children_end();
            std::copy_backward(kp, ke, ke + 1);
            std::copy_backward(cp, ce, ce + 1);
            *kp = key;
            *cp = child;
            ++this->count;
        }

        u32 index_of(node_address child) {
            u32 index = child_index(std::find(children_begin(), children_end(), child));
            EXTPP_CHECK(index != this->count, "child not found in parent");
            return index;
        }

        void remove(u32 index) {
            EXTPP_CHECK(index < this->count, "index out of bounds.");

            if (index < this->count - 1) {
                shift(this->keys + index + 1, this->count - index - 2, -1);
            }
            shift(this->children + index + 1, this->count - index - 1, -1);
            this->count -= 1;
        }
    };

    using node_type = handle<node_header, BlockSize>;

    using leaf_type = handle<leaf_block, BlockSize>;

    using internal_type = handle<internal_block, BlockSize>;

public:
    class anchor {
        /// The number of entries in this tree.
        u64 size = 0;

        /// The number of leaf nodes in this tree.
        u64 leaves = 0;

        /// The number of internal nodes in this tree.
        u32 internals = 0;

        /// The height of this tree.
        /// - 0: empty (no nodes)
        /// - 1: root is a leaf with at least one value
        /// - > 1: root is an internal node with at least one key and two children
        u32 height = 0;

        /// Points to the root node (if any).
        node_address root;

        /// Points to the leftmost leaf (if any).
        leaf_address leftmost;

        /// Points to the rightmost leaf (if any).
        leaf_address rightmost;

        friend class btree;
    };

private:
    node_type read_node(node_address ptr) const {
        return access(*m_engine, ptr);
    }

    internal_type create_internal() {
        raw_address<BlockSize> addr = m_alloc->allocate(1);
        internal_type internal = construct<internal_block>(*m_engine, addr);
        ++m_anchor->internals;
        m_anchor.dirty();
        return internal;
    }

    void free_internal(node_address ptr) {
        m_alloc->free(ptr.raw());
        --m_anchor->internals;
        m_anchor.dirty();
    }

    internal_type read_internal(node_address ptr) const {
        return access(*m_engine, address_cast<internal_block>(ptr));
    }

    leaf_type create_leaf() {
        raw_address<BlockSize> addr = m_alloc->allocate(1);
        leaf_type leaf = construct<leaf_block>(*m_engine, addr);
        ++m_anchor->leaves;
        m_anchor.dirty();
        return leaf;
    }

    void free_leaf(leaf_address ptr) {
        m_alloc->free(ptr.raw());
        --m_anchor->leaves;
        m_anchor.dirty();
    }

    leaf_type read_leaf(node_address ptr) const {
        return access(*m_engine, address_cast<leaf_block>(ptr));
    }

private:
    /// Searches the tree using either lower- or upper bound search.
    /// Returns the position of the requested element (either >= or > key)
    /// or the end position within the leaf if no such element exists.
    /// \pre `!empty()`.
    template<typename Search>
    std::tuple<leaf_type, u32> search(const key_type& key, const Search& search) const {
        EXTPP_ASSERT(!empty(), "tree is empty");

        node_address ptr = m_anchor->root;
        for (u32 level = m_anchor->height - 1; level; --level) {
            internal_type node = read_internal(ptr);
            u32 child_index = search(node, key);
            EXTPP_ASSERT(child_index < node->count, "child index out of bounds");
            ptr = node->children[child_index];
        }

        leaf_type leaf = read_leaf(ptr);
        u32 pos = search(leaf, key);
        return std::make_tuple(std::move(leaf), pos);
    }

    struct lower_bound_search {
        const btree* tree;

        /// Returns the position of the (first) appropriate child for `key`.
        u32 operator()(const internal_type& internal, const key_type& key) const {
            EXTPP_ASSERT(internal->count > 0, "internal node is empty");
            auto pos = std::lower_bound(internal->begin(), internal->end(), key, [&](const key_type& a, const key_type& b) {
                return tree->less(a, b);
            });
            return internal->index(pos);
        }

        /// Returns the position of the (first) value with a key that is
        /// >= than the search key.
        u32 operator()(const leaf_type& leaf, const key_type& key) const {
            EXTPP_ASSERT(leaf->count > 0, "leaf is empty");
            auto pos = std::lower_bound(leaf->begin(), leaf->end(), key, [&](const value_type& a, const key_type& b) {
                return tree->less(tree->extract(a), b);
            });
            return leaf->index(pos);
        }
    };

    struct upper_bound_search {
        const btree* tree;

        /// Returns the position of the (first) child that contains values greater than `key`.
        u32 operator()(const internal_type& internal, const key_type& key) const {
            EXTPP_ASSERT(internal->count > 0, "internal node is empty");
            auto pos = std::upper_bound(internal->begin(), internal->end(), key, [&](const key_type& a, const key_type& b) {
                return tree->less(a, b);
            });
            return internal->index(pos);
        }

        /// Returns the position of the (first) value with a key greater than the search key.
        u32 operator()(const leaf_type& leaf, const key_type& key) const {
            EXTPP_ASSERT(leaf->count > 0, "leaf is empty");
            auto pos = std::upper_bound(leaf->begin(), leaf->end(), key, [&](const key_type& a, const value_type& b) {
                return tree->less(a, tree->extract(b));
            });
            return leaf->index(pos);
        }
    };

    /// True iff a < b.
    bool less(const key_type& a, const key_type& b) const {
        return m_compare(a, b);
    }

    /// True iff a == b.
    bool equal(const key_type& a, const key_type& b) const {
        return !less(a, b) && !less(b, a);
    }

    decltype(auto) extract(const value_type& v) const {
        return m_extract(v);
    }

    /// Recursively clears the subtree rooted at ptr.
    /// Level: the level of the node at ptr. 0 is the leaf level.
    void clear(node_address ptr, u32 level) {
        if (level == 0) {
            free_leaf(address_cast<leaf_block>(ptr));
            return;
        }

        {
            internal_type internal = read_internal(ptr);
            for (auto i = internal->children_begin(), e = internal->children_end();
                 i != e; ++i) {
                clear(*i, level - 1);
            }
        }
        free_internal(ptr);
    }

public:
    btree(anchor_ptr<anchor> anch,
          extpp::engine<BlockSize>& eng,
          allocator<BlockSize>& alloc,
          KeyExtract extract = KeyExtract(), KeyCompare compare = KeyCompare())
        : m_engine(&eng)
        , m_alloc(&alloc)
        , m_extract(std::move(extract))
        , m_compare(std::move(compare))
        , m_anchor(std::move(anch))
    {}

    btree(const btree&) = delete;
    btree(btree&&) noexcept = default;

    btree& operator=(const btree&) = delete;
    btree& operator=(btree&&) noexcept = default;


    extpp::engine<BlockSize>& engine() const { return *m_engine; }
    extpp::allocator<BlockSize>& allocator() const { return *m_alloc; }

    bool empty() const { return height() == 0; }
    u64 size() const { return m_anchor->size; }
    u32 height() const { return m_anchor->height; }
    u64 leaf_nodes() const { return m_anchor->leaves; }
    u64 internal_nodes() const { return m_anchor->internals; }
    u64 nodes() const { return internal_nodes() + leaf_nodes(); }

    /// Maximum number of children per internal node.
    static constexpr u32 internal_fanout() { return internal_block::max_size(); }

    /// Maximum number of values per leaf node.
    static constexpr u32 leaf_fanout() { return leaf_block::max_size(); }

    /// The average leaf fill factor. Computed by dividing the number of elements
    /// by the number of available element slots (leaves * leaf fanout).
    double fill_factor() const {
        return empty() ? 0 : double(size()) / (leaf_nodes() * leaf_fanout());
    }

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const { return nodes() * BlockSize; }

    /// The relative overhead of this datastructure compared to simply
    /// storing all values in a linear file.
    /// Computed by dividing the size of this datastructure on disk (see \ref byte_size())
    /// by the number of bytes required to store `size()` instances of `value_type`.
    double overhead() const {
        return empty() ? 0 : double(byte_size()) / (size() * sizeof(value_type));
    }

    iterator begin() const {
        if (empty())
            return end();
        return iter(m_anchor->leftmost, 0);
    }

    iterator end() const { return iterator(this); }

    visitor visit() const {
        if (empty()) // TODO
            throw std::logic_error("tree is empty");
        return visitor(this);
    }

    /// Searches the tree for the first value greater than or equal to `key` and returns
    /// an iterator pointing to it, or `end()` if no such value was found.
    iterator lower_bound(const key_type& key) const {
        if (empty())
            return end();

        leaf_type leaf;
        u32 index;
        std::tie(leaf, index) = search(key, lower_bound_search{this});
        if (index == leaf->count)
            return end();
        return iter(std::move(leaf), index);
    }

    /// Searches the tree for the first value greater than `key` and returns
    /// an iterator pointing to it, or `end()` if no such value was found.
    iterator upper_bound(const key_type& key) const {
        if (empty())
            return end();

        leaf_type leaf;
        u32 index;
        std::tie(leaf, index) = search(key, upper_bound_search{this});
        if (index == leaf->count)
            return end();
        return iter(std::move(leaf), index);
    }

    std::pair<iterator, iterator> equal_range(const key_type& key) const {
        auto lower = lower_bound(key);
        auto upper = lower;
        if (upper != end() && equal(key, extract(*upper)))
            ++upper;
        return std::make_pair(std::move(lower), std::move(upper));
    }

    /// Searches the tree for a value with the given key.
    /// Returns an iterator that points to that value if the key was found,
    /// or `end()` if no such value exists.
    iterator find(const key_type& key) const {
        auto pos = lower_bound(key);
        if (pos != end() && equal(extract(*pos), key))
            return pos;
        return end();
    }

    /// Removes all elements from this tree.
    void clear() {
        if (empty())
            return;

        clear(m_anchor->root, m_anchor->height - 1);
        m_anchor->root = {};
        m_anchor->leftmost = m_anchor->rightmost = {};
        m_anchor->height = 0;
        m_anchor->size = 0;
        m_anchor.dirty();
    }

    /// Inserts the given value into the tree.
    /// Does not change the tree if a value with the same key already exists.
    ///
    /// \note Inserting a value invalidates *all* other iterators.
    std::pair<iterator, bool> insert(const value_type& value) {
        if (empty()) {
            leaf_type leaf = create_leaf();
            leaf->values[0] = value;
            leaf->count = 1;
            leaf.dirty();

            m_anchor->height = 1;
            m_anchor->size = 1;
            m_anchor->root = address_cast<node_header>(leaf.address());
            m_anchor->leftmost = m_anchor->rightmost = leaf.address();
            m_anchor.dirty();
            return std::make_pair(iter(std::move(leaf), 0), true);
        }

        const key_type& key = extract(value);
        leaf_type leaf;
        u32 index;
        std::tie(leaf, index) = search(key, lower_bound_search{this});
        if (index < leaf->count && equal(key, extract(leaf->values[index]))) {
            return std::make_pair(iter(std::move(leaf), index), false);
        }
        return std::make_pair(insert_at(std::move(leaf), index, value), true);
    }

    /// Searches for a value with the given key and then removes it.
    /// Returns true if such a value existed, false otherwise.
    bool erase(const key_type& key) {
        auto pos = find(key);
        if (pos != end()) {
            erase(pos);
            return true;
        }
        return false;
    }

    /// Removes the value at `pos`.
    /// `pos` must point to a valid element.
    ///
    /// Returns an iterator following the removed element.
    iterator erase(iterator pos) {
        EXTPP_ASSERT(pos.tree() == this, "iterator does not belong to this instance");
        EXTPP_ASSERT(pos != end(), "cannot erase the past-the-end iterator");
        return erase_at(pos.leaf(), pos.index());
    }

    /// Modify the value at `*iter` using the given Operation function.
    /// A mutable reference to the value will be passed to `op` which may in turn
    /// modify the value. However, the key derived from the modified value
    /// *must* be equivalent to the key before the modification.
    ///
    /// \throws std::logic_error If the old and new key are not equivalent.
    /// \note Iterators are not invalidated.
    template<typename Operation>
    void modify(const iterator& iter, Operation&& op) {
        EXTPP_ASSERT(iter.tree() == this, "iterator does not belong to this btree");
        EXTPP_ASSERT(iter != end(), "cannot modify the past-the-end iterator");

        value_type v = *iter;
        key_type k = extract(v);
        op(v);

        if (!equal(k, extract(v))) {
            // TODO: Throw or assert?
            throw std::logic_error("btree::modify: keys are not equivalent");
        }
        iter.leaf()->values[iter.index()] = v;
        iter.leaf().dirty();
    }

    /// Replaces the value at `*iter` with `value`.
    /// Both values must have the same key.
    void replace(const iterator& iter, const value_type& value) {
        return modify(iter, [&](value_type& v) {
            v = value;
        });
    }

    void verify() const;

private:
    /// Insert the given value at the specified leaf position.
    /// Splits the leaf (and its parents) if required.
    iterator insert_at(leaf_type leaf, u32 index, const value_type& value) {
        EXTPP_ASSERT(index <= leaf->count, "index is out of bounds");

        m_anchor->size += 1;
        m_anchor.dirty();

        constexpr u32 max = leaf_block::max_size();

        leaf.dirty();
        if (leaf->count < max) {
            leaf->insert(index, value);
            move_iterators(leaf, index, leaf->count - 1, leaf, index + 1);
            return iter(std::move(leaf), index);
        }
        EXTPP_ASSERT(leaf->count == max,
                     "Leaf must be exactly full.");

        // The new leaf is always created RIGHT of the old one.
        leaf_type new_leaf = create_leaf();
        {
            new_leaf->previous = leaf.address();
            new_leaf->next = leaf->next;
            new_leaf->parent = leaf->parent;

            if (leaf->next) {
                auto next = read_leaf(leaf->next);
                next->previous = new_leaf.address();
                next.dirty();
            } else {
                m_anchor->rightmost = new_leaf.address();
                m_anchor.dirty();
            }
            leaf->next = new_leaf.address();
        }

        iterator result;
        key_type split_key = split(leaf, new_leaf, index, value, result);
        propagate_split(std::move(leaf), std::move(new_leaf), std::move(split_key));
        return result;
    }

    /// Propagates a node split upwards through the tree, splitting internal nodes as required.
    void propagate_split(node_type old_child, node_type new_child, key_type split_key) {
        node_address parent_address = old_child->parent;
        while (parent_address) {
            internal_type node = read_internal(parent_address);
            node.dirty();

            EXTPP_ASSERT(old_child->parent == new_child->parent,
                        "children don't have the same parent");
            EXTPP_ASSERT(old_child->parent == node.address(),
                        "wrong parent pointer");

            const u32 insert_index = node->index_of(old_child.address()) + 1;
            if (node->count < node->max_size()) {
                // child fits into parent.
                node->apply_child_split(insert_index, split_key, new_child.address());
                return;
            }

            // Parent has to be split. The new internal node will be to the RIGHT of the existing one.
            internal_type new_internal = create_internal();
            new_internal->parent = node->parent;
            split_key = split(node, new_internal, insert_index, split_key, new_child.address());

            parent_address = node->parent;
            old_child = std::move(node);
            new_child = std::move(new_internal);
        }

        // old_child was the root and has been split.
        internal_type new_root = create_internal();
        new_root->children[0] = old_child.address();
        new_root->children[1] = new_child.address();
        new_root->keys[0] = split_key;
        new_root->count = 2;

        old_child->parent = new_root.address();
        old_child.dirty();
        new_child->parent = new_root.address();
        new_child.dirty();

        m_anchor->root = new_root.address();
        ++m_anchor->height;
        m_anchor.dirty();
    }

    /// Split the existing values and insert the new value at the appropriate position.
    ///
    /// \param left_leaf        The old (existing) leaf. Must be full.
    /// \param right_leaf       A new leaf without any entries.
    /// \param insert_index     The appropriate index for `value` within the old leaf.
    /// \param value            The new value
    key_type split(const leaf_type& left_leaf, const leaf_type& right_leaf,
                   u32 insert_index, const value_type& value, iterator& pos)
    {
        EXTPP_ASSERT(left_leaf->count == leaf_fanout(), "old_leaf leaf must be full");
        EXTPP_ASSERT(insert_index <= left_leaf->count, "index must be valid");
        EXTPP_ASSERT(right_leaf->count == 0, "new_leaf must be empty");

        // Number of values (without the new one).
        const u32 count = left_leaf->count;

        // Number of values that end up in the left leaf, after the split.
        const u32 mid = [&]{
            if (right_leaf.address() == m_anchor->rightmost)
                return leaf_fanout();
            if (left_leaf.address() == m_anchor->leftmost)
                return u32(1);
            return (count + 2) / 2;
        }();

        detail::sequence_insert(left_leaf->values, right_leaf->values, count, mid, insert_index, value);
        left_leaf->count = mid;
        right_leaf->count = count + 1 - mid;

        pos = insert_index < mid ? iter(left_leaf, insert_index)
                                 : iter(right_leaf, insert_index - mid);
        if (insert_index < mid) {
            // All values with old_index >= insert_index and old_index < mid
            // have shifted one to the right. The others are located in the new node.
            move_iterators(left_leaf, mid - 1, count, right_leaf, 0);
            move_iterators(left_leaf, insert_index, mid - 1, left_leaf, insert_index + 1);
        } else {
            // All values with old_index >= mid are now in the right node. Additionally,
            // those with old_index >= insert_index have shifted one to the right.
            move_iterators(left_leaf, insert_index, count, right_leaf, insert_index - mid + 1);
            move_iterators(left_leaf, mid, insert_index, right_leaf, 0);
        }

        return extract(left_leaf->values[mid - 1]);
    }

    key_type split(const internal_type& left_node, const internal_type& right_node,
                   u32 insert_index, const key_type& new_key, node_address new_child)
    {
        EXTPP_ASSERT(left_node->count == internal_fanout(), "internal node must be full");
        EXTPP_ASSERT(insert_index >= 0 && insert_index <= left_node->count, "index must be valid");
        EXTPP_ASSERT(right_node->count == 0, "new node must be empty");

        // new_key must be inserted at insert_index - 1.
        // new_child must be inserted at insert_index.
        const u32 count = left_node->count;     // Number of children, not counting the new one.
        const u32 mid = ((count + 2) / 2);      // Number of children in the left node, after the split.
        detail::sequence_insert(left_node->keys, right_node->keys, count - 1, mid, insert_index - 1, new_key);
        detail::sequence_insert(left_node->children, right_node->children, count, mid, insert_index, new_child);

        // We retained one more key in the left leaf than required.
        // It will be put into the parent to represent the result of the split.
        left_node->count = mid;
        right_node->count = count - mid + 1;

        // Fixup parent pointers.
        for (u32 i = 0; i < right_node->count; ++i) {
            node_type child = read_node(right_node->children[i]);
            child->parent = right_node.address();
            child.dirty();
        }
        return left_node->keys[mid - 1];
    }

    /// Erases the value at (leaf, index). Returns an iterator to the
    /// element after that value (or end() if no such element exists).
    iterator erase_at(const leaf_type& leaf, u32 index) {
        constexpr u32 min = leaf_block::min_size();

        auto succ = [&](const leaf_type& node, u32 index) {
            if (index == node->count)
                return node->next ? iter(node->next, 0) : end();
            return iter(node, index);
        };

        m_anchor->size -= 1;
        m_anchor.dirty();

        leaf->remove(index);
        leaf.dirty();

        invalidate_iterators(leaf, index);
        move_iterators(leaf, index + 1, leaf->count + 1, leaf, index);

        // The leftmost/rightmost leaves are allowed to become completely empty,
        // in which case they are simply removed.
        if (leaf.address() == m_anchor->leftmost || leaf.address() == m_anchor->rightmost) {
            if (leaf->count == 0) {
                iterator result = leaf->next ? iter(leaf->next, 0) : end();
                if (leaf->parent) {
                    internal_type parent = read_internal(leaf->parent);
                    propagate_erase(parent, parent->index_of(leaf.address()));
                } else {
                    m_anchor->height = 0;
                    m_anchor->root = {};
                    m_anchor.dirty();
                }

                destroy_leaf(leaf);
                return result;
            }
            return succ(leaf, index);
        }

        // All other leaves must remain at least half full.
        if (leaf->count >= min)
            return succ(leaf, index);

        // The node is neither the first, nor the last one and has fewer than `min` elements.
        // Try to steal elements from the left or right neighbor, or, if that is not possible,
        // merge with them.
        // Note: the parent always exists, otherwise this would be the leftmost/rightmost node (case above).
        internal_type parent = read_internal(leaf->parent);
        const u32 parent_index = parent->index_of(leaf.address());
        const node_address left_ptr = parent_index > 0 ?
                    parent->children[parent_index - 1] : node_address();
        const node_address right_ptr = parent_index < parent->count - 1 ?
                    parent->children[parent_index + 1] : node_address();

        leaf_type left, right;
        if (left_ptr) {
            left = read_leaf(left_ptr);
            if (left->count > min || (left.address() == m_anchor->leftmost && left->count > 1)) {
                leaf->insert(0, left->values[left->count - 1]);
                left->remove(left->count - 1);
                left.dirty();

                parent->keys[parent_index - 1] = extract(left->values[left->count - 1]);
                parent.dirty();

                move_iterators(leaf, 0, leaf->count - 1, leaf, 1);
                move_iterators(left, left->count, left->count + 1, leaf, 0);
                return succ(leaf, index + 1);
            }
        }

        if (right_ptr) {
            right = read_leaf(right_ptr);
            if (right->count > min || (right.address() == m_anchor->rightmost && right->count > 1)) {
                leaf->insert(leaf->count, right->values[0]);
                right->remove(0);
                right.dirty();

                parent->keys[parent_index] = extract(leaf->values[leaf->count - 1]);
                parent.dirty();

                move_iterators(right, 0, 1, leaf, leaf->count - 1);
                move_iterators(right, 1, right->count + 1, right, 0);
                return succ(leaf, index);
            }
        }

        if (left) {
            u32 left_count = left->count;
            merge(left, leaf);
            propagate_erase(parent, parent_index - 1);
            destroy_leaf(left);
            return succ(leaf, index + left_count);
        }

        merge(leaf, right);
        right.dirty();
        propagate_erase(parent, parent_index);
        destroy_leaf(leaf);
        return succ(right, index);
    }

    void propagate_erase(internal_type node, u32 removed_child_index) {
        constexpr u32 min = internal_block::min_size();

        while (1) {
            EXTPP_ASSERT(node->count >= 2, "Node is too empty.");

            node->remove(removed_child_index);
            node.dirty();

            if (node.address() == m_anchor->root)
                break;

            if (node->count >= min)
                return;

            // Try to steal one entry from the left/right neighbor,
            // merge nodes if that is not possible.
            internal_type parent = read_internal(node->parent);
            internal_type left, right;
            const u32 parent_index = parent->index_of(node.address());
            const node_address left_ptr = parent_index > 0 ?
                        parent->children[parent_index - 1] : node_address();
            const node_address right_ptr = parent_index < parent->count - 1 ?
                        parent->children[parent_index + 1] : node_address();

            if (left_ptr) {
                left = read_internal(left_ptr);
                if (left->count > min) {
                    shift(node->keys, node->count - 1, 1);
                    shift(node->children, node->count, 1);
                    node->keys[0] = parent->keys[parent_index - 1];
                    node->children[0] = left->children[left->count - 1];
                    node->count += 1;

                    node_type child = read_node(node->children[0]);
                    child->parent = node.address();
                    child.dirty();

                    parent->keys[parent_index - 1] = left->keys[left->count - 2];
                    parent.dirty();

                    left->count -= 1;
                    left.dirty();
                    return;
                }
            }

            if (right_ptr) {
                right = read_internal(right_ptr);
                if (right->count > min) {
                    node->keys[node->count - 1] = parent->keys[parent_index];
                    node->children[node->count] = right->children[0];
                    node->count += 1;

                    node_type child = read_node(node->children[node->count - 1]);
                    child->parent = node.address();
                    child.dirty();

                    parent->keys[parent_index] = right->keys[0];
                    parent.dirty();

                    shift(right->keys + 1, right->count - 2, -1);
                    shift(right->children + 1, right->count - 1, -1);
                    right->count -= 1;
                    right.dirty();
                    return;
                }
            }

            if (left) {
                merge(left, node, parent, parent_index - 1);
                free_internal(left_ptr);
                removed_child_index = parent_index - 1;
            } else {
                merge(node, right, parent, parent_index);
                right.dirty();
                free_internal(node.address());
                removed_child_index = parent_index;
            }

            node = std::move(parent);
        }

        EXTPP_CHECK(node.address() == m_anchor->root, "Must have reached the root.");
        if (node->count == 1) {
            internal_type new_root = read_internal(node->children[0]);
            new_root->parent = {};
            new_root.dirty();

            m_anchor->root = new_root.address();
            m_anchor->height -= 1;
            m_anchor.dirty();

            free_internal(node.address());
        }
    }

    /// Merges the content of the left node into the content of the right node.
    void merge(const leaf_type& left, const leaf_type& right) {
        constexpr u32 max = leaf_block::max_size();

        EXTPP_ASSERT(left->count + right->count <= max,
                     "Too many values for a merge.");
        shift(right->values, right->count, left->count);
        copy(left->values, left->count, right->values);
        right->count += left->count;

        move_iterators(right, 0, right->count - left->count, right, left->count);
        move_iterators(left, 0, left->count, right, 0);
    }

    /// Merges the content of the left node into the right node.
    /// Has to consult the parent node in order to find the key appropriate for the last child of `left`.
    void merge(const internal_type& left, const internal_type& right, const internal_type& parent, u32 left_index) {
        constexpr u32 max = internal_block::max_size();

        EXTPP_ASSERT(left->count + right->count <= max, "Too many values for a merge.");
        EXTPP_ASSERT(left_index + 1 < parent->count, "parent index out of bounds.");
        EXTPP_ASSERT(parent->children[left_index].value == left.address(), "Wrong parent index of left node.");
        EXTPP_ASSERT(parent->children[left_index + 1].value == right.address(), "Right node is not the neighbor to the right.");

        // We move left->count (key, child)-pairs to the right node.
        shift(right->keys, right->count - 1, left->count);
        shift(right->children, right->count, left->count);

        copy(left->keys, left->count - 1, right->keys);
        right->keys[left->count - 1] = parent->keys[left_index];
        copy(left->children, left->count, right->children);
        right->count += left->count;

        for (u32 i = 0; i < left->count; ++i) {
            node_type child = read_node(right->children[i]);
            child->parent = right.address();
            child.dirty();
        }
    }

    /// Removes a leaf node from the linked list of leaf nodes and then frees it.
    /// Note: does not touch the index structure (i.e. the parent nodes).
    void destroy_leaf(const leaf_type& leaf) {
        if (leaf->previous) {
            leaf_type prev = read_leaf(leaf->previous);
            prev->next = leaf->next;
            prev.dirty();
        } else {
            m_anchor->leftmost = leaf->next;
            m_anchor.dirty();
        }

        if (leaf->next) {
            leaf_type next = read_leaf(leaf->next);
            next->previous = leaf->previous;
            next.dirty();
        } else {
            m_anchor->rightmost = leaf->previous;
            m_anchor.dirty();
        }

        free_leaf(leaf.address());
    }

    /// Move the iterator positions from old_leaf, [first_index, last_index)
    /// to new_leaf, [new_first_index, ...).
    void move_iterators(const leaf_type& old_leaf,
                        u32 first_index, u32 last_index,
                        const leaf_type& new_leaf,
                        u32 new_first_index) {
        cursor_buffer_t buffer;
        m_iterator_map.find_iterators(old_leaf.address().raw(), first_index, last_index, buffer);

        for (cursor *i : buffer) {
            u32 offset = i->base().index() - first_index;
            *i = iter(new_leaf, new_first_index + offset);
        }
    }

    void invalidate_iterators(const leaf_type& leaf, u32 index) {
        cursor_buffer_t buffer;
        m_iterator_map.find_iterators(leaf.address().raw(), index, index + 1, buffer);

        for (cursor *i : buffer) {
            i->reset();
        }
    }

    iterator iter(leaf_address leaf, u32 index) const { return iter(read_leaf(leaf), index); }
    iterator iter(leaf_type leaf, u32 index) const { return iterator(this, std::move(leaf), index); }

    /// Copy `count` objects from `source` to `dest`.
    template<typename T>
    static void copy(const T* source, size_t count, T* dest) {
        static_assert(std::is_trivially_copyable<T>::value,
                      "T must be trivially copyable");
        std::memmove(dest, source, sizeof(T) * count);
    }

    /// Shifts `count` objects from `source` by `shift` positions.
    /// E.g. `shift(ptr, 5, 1)` moves 5 objects starting from `ptr` one step to the right.
    template<typename T>
    static void shift(T* source, size_t count, std::ptrdiff_t shift) {
        copy(source, count, source + shift);
    }

private:
    /// The block engine.
    extpp::engine<BlockSize>* m_engine;

    /// Used to allocate blocks for tree nodes.
    extpp::allocator<BlockSize>* m_alloc;

    /// Takes a value and returns the value's search key.
    KeyExtract m_extract;

    /// Takes two keys as parameters. m_compare(a, b) is true iff a < b.
    KeyCompare m_compare;

    /// Persistent tree state.
    anchor_ptr<anchor> m_anchor;

    /// Contains references to safe iterators. These have to be adjusted when
    /// elements are inserted/removed or nodes are split/merged.
    mutable cursor_map m_iterator_map;
};

/// An iterator points to a single value inside the btree or past the end.
/// Iterators contain a reference to the leaf node that contains their value; the
/// block will be kept in memory for at least as long as the iterator exists.
template<typename V, typename K, typename C, u32 B>
class btree<V, K, C, B>::iterator : public boost::iterator_facade<
    iterator,                           // Derived
    value_type,                         // Value Type
    std::bidirectional_iterator_tag,    // Iterator Category
    const value_type&                   // Reference type
>
{
    const btree* m_tree = nullptr;

    /// Points to a valid leaf or null if this is an invalid iterator.
    leaf_type m_leaf;

    /// If m_leaf points to a valid leaf, then this index is in bounds.
    u32 m_index = 0;

public:
    iterator() = default;

private:
    friend class btree;

    // past-the-end.
    iterator(const btree* tree)
        : m_tree(tree) {}

    // points to a valid element.
    iterator(const btree* tree, leaf_type leaf, u32 index)
        : m_tree(tree)
        , m_leaf(std::move(leaf))
        , m_index(index)
    {
        EXTPP_ASSERT(m_leaf, "must be a valid leaf pointer");
        EXTPP_ASSERT(m_index < m_leaf->count, "index must be within bounds");
    }

public:
    const btree* tree() const { return m_tree; }

    const leaf_type& leaf() const {
        EXTPP_ASSERT(m_leaf, "invalid iterator");
        return m_leaf;
    }

    raw_address<B> node_address() const { return m_leaf.address().raw(); }

    u32 index() const {
        return m_index;
    }

private:
    friend class boost::iterator_core_access;

    const value_type& dereference() const {
        return leaf()->values[index()];
    }

    bool equal(const iterator& other) const {
        EXTPP_ASSERT(m_tree == other.m_tree, "comparing iterator of different trees.");
        return m_leaf == other.m_leaf && m_index == other.m_index;
    }

    void increment() {
        EXTPP_ASSERT(m_tree, "incrementing invalid iterator");

        if (m_leaf) {
            if (++m_index == m_leaf->count) {
                leaf_address next = m_leaf->next;
                if (next) {
                    m_leaf = m_tree->read_leaf(next);
                    m_index = 0;
                } else {
                    m_leaf.reset();
                    m_index = 0;
                }
            }
        } else {
            // Go from the past-the-end iterator to the first entry of the first leaf.
            leaf_address ptr = m_tree->m_anchor->leftmost;
            EXTPP_ASSERT(ptr, "incrementing past-the-end iterator on an empty tree");
            m_leaf = m_tree->read_leaf(ptr);
            m_index = 0;
        }
        EXTPP_ASSERT(!m_leaf || m_index < m_leaf->count, "either past-the-end or a valid position");
    }

    void decrement() {
        EXTPP_ASSERT(m_tree, "decrementing invalid iterator");

        if (m_leaf) {
            if (m_index-- == 0) {
                leaf_address prev = m_leaf->previous;
                if (prev) {
                    m_leaf = m_tree->read_leaf(prev);
                    m_index = m_leaf->count - 1;
                } else {
                    m_leaf.reset();
                    m_index = 0;
                }
            }
        } else {
            // Go from the past-the-end iterator to the last entry of the last leaf.
            leaf_address ptr = m_tree->m_anchor->rightmost;
            EXTPP_ASSERT(ptr, "decrementing past-the-end iterator on an empty tree");
            m_leaf = m_tree->read_leaf(ptr);
            m_index = m_leaf->count - 1;
        }

        EXTPP_ASSERT(!m_leaf || m_index < m_leaf->count, "either past-the-end or a valid position");
    }
};

template<typename V, typename K, typename C, u32 B>
class btree<V, K, C, B>::cursor : public detail::safe_iterator_base<btree, iterator, cursor> {
public:
    cursor() = default;

    cursor(iterator iter)
        : cursor::safe_iterator_base{iter.tree()->m_iterator_map, std::move(iter)}
    {}

    cursor(const cursor&) = default;
    cursor(cursor&&) noexcept = default;

    cursor& operator=(iterator iter) {
        const btree* tree = iter.tree();
        cursor::safe_iterator_base::reset(tree->m_iterator_map, std::move(iter));
        return *this;
    }

    cursor& operator=(const cursor&) = default;
    cursor& operator=(cursor&&) noexcept = default;

private:
    friend class btree;
    friend class cursor::safe_iterator_base;

    using cursor::safe_iterator_base::reset;
};

template<typename V, typename K, typename C, u32 B>
class btree<V, K, C, B>::visitor {
public:
    bool is_leaf() const { return m_level == 0; }

    bool is_internal() const { return !is_leaf(); }

    u32 level() const { return m_level; }

    bool has_parent() const { return m_node->parent.valid(); }

    node_address address() const { return m_node.address(); }

    node_address parent_address() const { return m_node->parent; }

    u32 size() const { return is_leaf() ? as_leaf()->count : as_internal()->count; }

    /// \pre `is_leaf() && index < size()`.
    const value_type& value(u32 index) const {
        EXTPP_ASSERT(is_leaf(), "not a leaf node");
        EXTPP_ASSERT(index < size(), "index out of bounds");
        return as_leaf()->values[index];
    }

    /// \pre `is_leaf()`.
    leaf_address predecessor_address() const {
        EXTPP_ASSERT(is_leaf(), "not a leaf node");
        return as_leaf()->previous;
    }

    /// \pre `is_leaf()`.
    leaf_address successor_address() const {
        EXTPP_ASSERT(is_leaf(), "not a leaf node");
        return as_leaf()->next;
    }

    /// \pre `is_internal() && index < size() - 1`.
    const key_type& key(u32 index) const {
        EXTPP_ASSERT(is_internal(), "not an internal node");
        EXTPP_ASSERT(index < size() - 1, "index out of bounds");
        return as_internal()->keys[index];
    }

    /// \pre `is_internal() && index < size()`.
    leaf_address child_address(u32 index) const {
        EXTPP_ASSERT(is_internal(), "not an internal node");
        EXTPP_ASSERT(index < size(), "index out of bounds");
        return as_internal()->children[index];
    }

    /// \pre `is_internal() && index < size()`.
    void move_child(u32 index) {
        EXTPP_ASSERT(is_internal(), "not an internal node");
        EXTPP_ASSERT(index < size(), "index out of bounds");
        m_node = m_tree->read_node(as_internal()->children[index]);
        m_level -= 1;
    }

    /// \pre `is_internal() && index < size()`.
    visitor child(u32 index) const {
        visitor v(*this);
        v.move_child(index);
        return v;
    }

    /// \pre `has_parent()`.
    void move_parent() {
        EXTPP_ASSERT(has_parent(), "current node has no parent");
        m_node = m_tree->read_node(m_node->parent);
        m_level += 1;
    }

    /// \pre `has_parent()`.
    visitor parent() const {
        visitor v(*this);
        v.move_parent();
        return v;
    }

private:
    friend class btree;

    explicit visitor(const btree* tree)
        : m_tree(tree)
    {
        EXTPP_ASSERT(m_tree->height() > 0, "tree height is 0.");
        EXTPP_ASSERT(m_tree->m_anchor->root, "tree has no root.");
        m_level = m_tree->m_anchor->height - 1;
        m_node = m_tree->read_node(m_tree->m_anchor->root);
    }

    leaf_block* as_leaf() const { return static_cast<leaf_block*>(m_node.get()); }
    internal_block* as_internal() const { return static_cast<internal_block*>(m_node.get()); }

private:
    const btree* m_tree;
    u32 m_level;            ///< 0 -> leaf, anything greater: internal node.
    node_type m_node;       ///< Always points to a valid node.
};

template<typename V, typename K, typename C, u32 B>
void btree<V, K, C, B>::verify() const {
    static constexpr u32 min_values = leaf_block::min_size();
    static constexpr u32 max_values = leaf_block::max_size();
    static constexpr u32 min_children = internal_block::min_size();
    static constexpr u32 max_children = internal_block::max_size();

    struct context {
        u32 level = 0;
        node_address parent;
        key_type* lower = nullptr;  /// All values or keys must be greater.
        key_type* upper = nullptr;  /// All values or keys must be lesser or equal.
    };

    struct checker {
        const btree& tree;
        const btree::anchor* anchor;
        leaf_type last_leaf;
        u64 value_count = 0;
        u64 leaf_count = 0;
        u64 internal_count = 0;

        checker(const btree& tree)
            : tree(tree)
            , anchor(tree.m_anchor.get())
        {}

        void run() {
            if (anchor->height != 0) {
                if (!anchor->root)
                    error("non-empty tree does not have a root");

                context root_ctx;
                root_ctx.level = tree.m_anchor->height - 1;
                check(root_ctx, anchor->root);
            }

            if (value_count != anchor->size)
                error("value count does not match the tree's size");
            if (leaf_count != anchor->leaves)
                error("wrong of leaves");
            if (internal_count != anchor->internals)
                error("wrong number of internal nodes");
            if (last_leaf.address() != anchor->rightmost)
                error("last leaf is not the rightmost one");
        }

        void check(const context& ctx, node_address node_index) {
            if (!node_index)
                error("invalid node index");

            node_type node = tree.read_node(node_index);
            if (node->parent != ctx.parent)
                error("parent pointer does not point to parent.");

            if (ctx.level == 0) {
                check(ctx, cast<leaf_block>(node));
            } else {
                check(ctx, cast<internal_block>(node));
            }
        }

        void check(const context& ctx, const leaf_type& leaf) {
            ++leaf_count;

            if (last_leaf) {
                if (leaf->previous != last_leaf.address())
                    error("current leaf does not point to its predecessor");
                if (last_leaf->next != leaf.address())
                    error("last leaf does not point to its successor");
            } else {
                if (anchor->leftmost != leaf.address())
                    error("first leaf is not the leftmost leaf");
                if (leaf->previous)
                    error("the first leaf has a predecessor");
            }

            u32 size = leaf->count;
            if (size == 0)
                error("leaf is empty");
            if (size < min_values
                    && leaf.address() != anchor->root
                    && leaf.address() != anchor->leftmost
                    && leaf.address() != anchor->rightmost)
                error("leaf is underflowing");
            if (size > max_values)
                error("leaf is overflowing");

            for (u32 i = 0; i < size; ++i) {
                check_key(ctx, tree.extract(leaf->values[i]));
                if (i > 0 && !tree.less(leaf->values[i-1], leaf->values[i])) {
                    error("leaf entries are not sorted");
                }
            }

            value_count += size;
            last_leaf = leaf;
        }

        void check(const context& ctx, const internal_type& internal) {
            ++internal_count;

            u32 size = internal->count;
            if (size < min_children && internal.address() != anchor->root)
                error("internal node is underflowing");
            if (size < 2 && internal.address() == anchor->root)
                error("root is too empty");
            if (size > max_children)
                error("internal node is overflowing");

            key_type last_key = tree.extract(internal->keys[0]);
            check_key(ctx, last_key);

            context child_ctx;
            child_ctx.parent = internal.address();
            child_ctx.level = ctx.level - 1;
            child_ctx.lower = ctx.lower;
            child_ctx.upper = std::addressof(last_key);
            check(child_ctx, internal->children[0]);

            for (u32 i = 1; i < size - 1; ++i) {
                key_type current_key = tree.extract(internal->keys[i]);
                check_key(ctx, current_key);

                if (!tree.less(last_key, current_key)) {
                    error("internal node entries are not sorted");
                }

                child_ctx.lower = std::addressof(last_key);
                child_ctx.upper = std::addressof(current_key);
                check(child_ctx, internal->children[i]);
                last_key = current_key;
            }

            child_ctx.lower = std::addressof(last_key);
            child_ctx.upper = ctx.upper;
            check(child_ctx, internal->children[size - 1]);
        }

        void check_key(const context& ctx, const key_type& key) {
            if (ctx.lower && !tree.less(*ctx.lower, key))
                error("key is not greater than the lower bound");
            if (ctx.upper && tree.less(*ctx.upper, key))
                error("key greater than the upper bound");
        }

        void error(const char* message) {
            // TODO Own exception type?
            throw std::logic_error(std::string("btree::verify(): invariant violated (") + message + ").");
        }
    };

    checker(*this).run();
}

} // namespace extpp

#endif // EXTPP_BTREE_HPP
