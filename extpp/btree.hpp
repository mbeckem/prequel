#ifndef EXTPP_BTREE_HPP
#define EXTPP_BTREE_HPP

#include <extpp/allocator.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/identity_key.hpp>
#include <extpp/handle.hpp>
#include <extpp/type_traits.hpp>
#include <extpp/btree/iterator.hpp>
#include <extpp/btree/state.hpp>
#include <extpp/btree/safe_iterator.hpp>
#include <extpp/btree/verify.hpp>
#include <extpp/detail/iter_tools.hpp>

#include <boost/container/small_vector.hpp>

#include <algorithm>
#include <type_traits>
#include <variant>

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
class btree : private btree_detail::state<Value, KeyExtract, KeyCompare, BlockSize>
{
    using state_type = typename btree::state;

public:
    using value_type = Value;
    using key_type = typename state_type::key_type;

    using key_compare = KeyCompare;
    using size_type = u64;

    using iterator = btree_detail::iterator<state_type>;
    using const_iterator = iterator;

    class cursor;
    using const_cursor = cursor;

    class visitor;

    static constexpr u32 block_size = BlockSize;

    using anchor = typename state_type::anchor;

    static_assert(is_trivial<value_type>::value,
                  "The value type must be trivial.");
    static_assert(is_trivial<key_type>::value,
                  "The key type must be trivial.");

private:
    using cursor_map = btree_detail::safe_iterator_map<btree, cursor>;
    using cursor_buffer_t = boost::container::small_vector<cursor*, 16>;

    using node_address = typename state_type::node_address;

    using leaf_type = btree_detail::leaf_node<state_type>;
    using leaf_address = typename leaf_type::address_type;

    using internal_type = btree_detail::internal_node<state_type>;
    using internal_address = typename internal_type::address_type;

    // For each parent: node address and the index within that node.
    struct node_stack_entry {
        internal_address addr;
        u32 child = 0;

        node_stack_entry() = default;
        node_stack_entry(internal_address addr, u32 child): addr(addr), child(child) {}
    };

    using node_stack_type = std::vector<node_stack_entry>;

private:
    const anchor_ptr<anchor>& get_anchor() const { return state().get_anchor(); }

    template<bool UpperBound = false>
    leaf_type find_leaf(const key_type& key) const {
        return find_leaf_impl<UpperBound>(key, [&](auto&&...) {});
    }

    template<bool UpperBound = false>
    leaf_type find_leaf(const key_type& key, node_stack_type& stack) const {
        stack.reserve(height());
        stack.clear();

        return find_leaf_impl<UpperBound>(key, [&](internal_address parent, u32 index) {
            stack.emplace_back(parent, index);
        });
    }

    template<bool UpperBound, typename ParentCallback>
    leaf_type find_leaf_impl(const key_type& key, ParentCallback&& cb) const {
        EXTPP_ASSERT(!empty(), "tree is empty");

        node_address addr = get_anchor()->root;
        for (u32 level = get_anchor()->height - 1; level; --level) {
            internal_type node = state().access(state().cast_internal(addr));
            u32 child_index = UpperBound ? node.upper_bound(state(), key)
                                         : node.lower_bound(state(), key);
            cb(node.address(), child_index);
            addr = node.get_child(child_index);
        }
        return access(this->get_engine(), state().cast_leaf(addr));
    }

    /// Recursively clears the subtree rooted at ptr.
    /// Level: the level of the node at ptr. 0 is the leaf level.
    void clear(node_address ptr, u32 level) {
        if (level == 0) {
            state().free(state().cast_leaf(ptr));
            return;
        }

        {
            internal_type internal = state().access(state().cast_internal(ptr));
            for (auto i = internal.children_begin(), e = internal.children_end();
                 i != e; ++i) {
                clear(*i, level - 1);
            }
            state().free(internal.address());
        }
    }

public:
    btree(anchor_ptr<anchor> anch, allocator<BlockSize>& alloc,
          KeyExtract extract = KeyExtract(), KeyCompare compare = KeyCompare())
        : state_type(std::move(anch), alloc, std::move(extract), std::move(compare))
    {}

    btree(const btree&) = delete;
    btree(btree&&) noexcept = default;

    btree& operator=(const btree&) = delete;
    btree& operator=(btree&&) noexcept = default;

    using state_type::get_allocator;
    using state_type::get_engine;

    bool empty() const { return height() == 0; }
    u64 size() const { return get_anchor()->size; }
    u32 height() const { return get_anchor()->height; }
    u64 leaf_nodes() const { return get_anchor()->leaves; }
    u64 internal_nodes() const { return get_anchor()->internals; }
    u64 nodes() const { return internal_nodes() + leaf_nodes(); }

    /// Maximum number of children per internal node.
    static constexpr u32 internal_fanout() { return internal_type::max_size(); }

    /// Maximum number of values per leaf node.
    static constexpr u32 leaf_fanout() { return leaf_type::max_size(); }

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

    key_type key(const value_type& value) const {
        return state().key(value);
    }

    iterator begin() const {
        if (empty())
            return end();
        return iterator(state(), state().access(get_anchor()->leftmost), 0);
    }

    iterator end() const { return iterator(state()); }

    /// Returns a visitor for the nodes of this tree.
    /// The visitor starts at the root (if any).
    visitor visit() const { return visitor(this); }

    /// Searches the tree for the first value greater than or equal to `key` and returns
    /// an iterator pointing to it, or `end()` if no such value was found.
    iterator lower_bound(const key_type& key) const {
        if (empty())
            return end();

        leaf_type leaf = find_leaf(key);
        u32 index = leaf.lower_bound(state(), key);
        if (index == leaf.size())
            return end();
        return iterator(state(), std::move(leaf), index);
    }

    /// Searches the tree for the first value greater than `key` and returns
    /// an iterator pointing to it, or `end()` if no such value was found.
    iterator upper_bound(const key_type& key) const {
        if (empty())
            return end();

        leaf_type leaf = find_leaf<true>(key);
        u32 index = leaf.upper_bound(state(), key);
        if (index == leaf.size())
            return end();
        return iterator(state(), std::move(leaf), index);
    }

    std::pair<iterator, iterator> equal_range(const key_type& key) const {
        auto lower = lower_bound(key);
        auto upper = lower;
        if (upper != end() && state().key_equal(key, state().key(*upper)))
            ++upper;
        return std::make_pair(std::move(lower), std::move(upper));
    }

    /// Searches the tree for a value with the given key.
    /// Returns an iterator that points to that value if the key was found,
    /// or `end()` if no such value exists.
    iterator find(const key_type& key) const {
        auto pos = lower_bound(key);
        if (pos != end() && state().key_equal(state().key(*pos), key))
            return pos;
        return end();
    }

    /// Removes all elements from this tree. All disk blocks
    /// allocated by this tree are freed.
    ///
    /// \note Invalidates *all* iterators, with the exception of the end iterator.
    void clear() {
        if (empty())
            return;

        clear(get_anchor()->root, get_anchor()->height - 1);
        get_anchor()->root = {};
        get_anchor()->leftmost = get_anchor()->rightmost = {};
        get_anchor()->height = 0;
        get_anchor()->size = 0;
        get_anchor().dirty();
    }

    /// Inserts the given value into the tree.
    /// Does not change the tree if a value with the same key already exists.
    ///
    /// \note Inserting a value invalidates *all* other iterators,
    /// with the exception of the end iterator.
    std::pair<iterator, bool> insert(value_type value) {
        // TODO: Copy of value necessary?
        // It might point into the container itself.
        if (empty()) {
            leaf_type leaf = leaf_type::create(state());
            leaf.insert(0, value);

            get_anchor()->height = 1;
            get_anchor()->size = 1;
            get_anchor()->root = leaf.address();
            get_anchor()->leftmost = get_anchor()->rightmost = leaf.address();
            get_anchor().dirty();
            return std::make_pair(iterator(state(), std::move(leaf), 0), true);
        }

        key_type key = state().key(value);
        leaf_type leaf = get_anchor()->height == 1
                ? get_insertion_leaf(state().access(state().cast_leaf(get_anchor()->root)), key)
                : get_insertion_leaf(state().access(state().cast_internal(get_anchor()->root)), key);

        EXTPP_ASSERT(leaf && leaf.size() < leaf_type::max_size(), "Valid, non-full leaf.");
        u32 index = leaf.lower_bound(state(), key);
        if (index < leaf.size() && state().key_equal(key, state().key(leaf.get(index))))
            return std::make_pair(iterator(state(), std::move(leaf), index), false);

        leaf.insert(index, value);
        move_iterators(leaf, index, leaf.size() - 1, leaf, index + 1);

        get_anchor()->size += 1;
        get_anchor().dirty();
        return std::make_pair(iterator(state(), std::move(leaf), index), true);
    }

    /// Searches for a value with the given key and then removes it.
    /// Returns true if such a value existed, false otherwise.
    ///
    /// \note Invalidates *all* iterators, with the exception of the end iterator.
    bool erase(key_type key) {
        node_stack_type& stack = m_stack_buf;
        leaf_type leaf = find_leaf(key, stack);
        u32 index = leaf.lower_bound(state(), key);
        if (index == leaf.size() || !state().key_equal(key, state().key(leaf.get(index))))
            return false;

        leaf.remove(index);
        get_anchor()->size -= 1;
        get_anchor().dirty();

        invalidate_iterators(leaf, index);
        move_iterators(leaf, index + 1, leaf.size() + 1, leaf, index);

        erase_impl(std::move(leaf), stack);
        return true;
    }

    /// Removes the value at `pos`.
    /// `pos` must point to a valid element.
    ///
    /// Returns an iterator following the removed element.
    ///
    /// \note Invalidates *all* iterators, with the exception of the end iterator
    /// and the returned iterator.
    iterator erase(const iterator& pos) {
        check_valid(pos);
        key_type key = state().key(*pos);
        cursor c = std::next(pos);

        bool removed = erase(key);
        EXTPP_ASSERT(removed, "Value must have been removed.");
        unused(removed);
        return c.iterator();
    }

    /// Erases all elements in the range [first, last).
    /// `last` must be reachable from `first` using forward iteration.
    ///
    /// \note Invalidates *all* iterators, with the exception of the end iterator.
    void erase(iterator first, iterator last) {
        check_instance(first);
        check_instance(last);

        // TODO: Implement bulk erase; this is as slow as it gets.
        cursor current = first;
        cursor end = last;
        for(; current != end;) {
            check_valid(current);

            key_type key = state().key(*current);
            ++current;
            erase(key);
        }
    }

    /// Modify the value at `*iter` using the given Operation function.
    /// A mutable reference to the value will be passed to `op` which may in turn
    /// modify the value. However, the key derived from the modified value
    /// *must* be equivalent to the key before the modification.
    ///
    /// \throws std::logic_error If the old and new key are not equivalent.
    /// \note Iterators are not invalidated.
    template<typename Operation>
    void modify(const iterator& pos, Operation&& op) {
        check_valid(pos);

        value_type v = *pos;
        key_type k = state().key(v);
        op(v);

        if (!state().key_equal(k, state().key(v))) {
            // TODO: Throw or assert?
            throw std::logic_error("btree::modify: keys are not equivalent");
        }
        pos.leaf().set(pos.index(), v);;
    }

    /// Replaces the value at `*iter` with `value`.
    /// Both values must have the same key.
    void replace(const iterator& iter, const value_type& value) {
        return modify(iter, [&](value_type& v) {
            v = value;
        });
    }

    void verify() const {
        btree_detail::verify(state());
    }

    // TODO: Other variants.
    handle<value_type, BlockSize> pointer_to(const iterator& pos) {
        check_valid(pos);
        return pos.leaf().block().neighbor(&pos.leaf().get(pos.index()));
    }

private:
    /// Walk down the tree, searching for the leaf that can hold the key.
    /// Splits every full node on the way, i.e. the leaf is guarenteed
    /// to have enough capacity for one element.
    template<typename Node>
    leaf_type get_insertion_leaf(Node root, const key_type& key) {
        if (root.size() == root.max_size()) {
            auto [new_node, split_key] = split(root);
            internal_type new_root = internal_type::create_root(
                        state(), root.address(), split_key, new_node.address());
            get_anchor()->height += 1;
            get_anchor()->root = new_root.address();
            get_anchor().dirty();
            return get_insertion_leaf_impl(std::move(new_root), key);
        } else {
            return get_insertion_leaf_impl(std::move(root), key);
        }
    }

    leaf_type get_insertion_leaf_impl(leaf_type leaf, const key_type&) {
        return std::move(leaf);
    }

    /// Find a suitable leaf for `key` and split all full nodes on the way.
    leaf_type get_insertion_leaf_impl(internal_type parent, const key_type& key) {
        u32 level = get_anchor()->height - 1;
        EXTPP_ASSERT(level > 0, "Cannot be at leaf level with internal nodes.");

        for (; level > 1; --level) {
            // Level > 1: internal parent, internal child.
            u32 index = parent.lower_bound(state(), key);
            internal_type child = state().access(state().cast_internal(parent.get_child(index)));
            if (child.size() == internal_type::max_size()) {
                auto [new_node, split_key] = split_with_parent(parent, index, child);
                if (state().key_greater(key, split_key))
                    child = std::move(new_node);
            }

            parent = std::move(child);
        }

        // Level == 1: internal parent, leaf child.
        u32 index = parent.lower_bound(state(), key);
        leaf_type leaf = state().access(state().cast_leaf(parent.get_child(index)));
        if (leaf.size() == leaf_type::max_size()) {
            auto [new_node, split_key] = split_with_parent(parent, index, leaf);
            if (state().key_greater(key, split_key))
                leaf = std::move(new_node);
        }
        return leaf;
    }

    /// Splits a leaf node. The new leaf will be linked with the other leaves.
    auto split(const leaf_type& leaf) {
        typename leaf_type::split_mode mode = [&]{
            if (leaf.address() == get_anchor()->rightmost)
                return leaf_type::split_mode::rightmost;
            if (leaf.address() == get_anchor()->leftmost)
                return leaf_type::split_mode::leftmost;
            return leaf_type::split_mode::normal;
        }();

        leaf_type new_leaf = leaf_type::create(state());
        new_leaf.set_prev(leaf.address());
        new_leaf.set_next(leaf.next());
        if (leaf.next()) {
            leaf_type next = state().access(leaf.next());
            next.set_prev(new_leaf.address());
        } else {
            get_anchor()->rightmost = new_leaf.address();
            get_anchor().dirty();
        }
        leaf.set_next(new_leaf.address());

        u32 old_size = leaf.size();
        key_type split_key = leaf.split(state(), new_leaf, mode);
        u32 new_size = leaf.size();

        move_iterators(leaf, new_size, old_size, new_leaf, 0);
        return std::make_tuple(std::move(new_leaf), split_key);
    }

    /// Splits an internal node.
    auto split(const internal_type& internal) {
        internal_type new_internal = internal_type::create(state());
        key_type split_key = internal.split(state(), new_internal);
        return std::make_tuple(std::move(new_internal), split_key);
    }

    /// Split a node that has a parent. The node must be at the given `index` in the `parent`
    /// node. The new node (the result of the split) will be inserted as child `index + 1`.
    /// Returns the split key and the new node.
    template<typename ChildNode>
    auto split_with_parent(const internal_type& parent, u32 index, const ChildNode& node) {
        EXTPP_ASSERT(parent.size() < internal_type::max_size(), "Parent must not be full.");
        EXTPP_ASSERT(parent.get_child(index) == node.address(), "Wrong child index.");

        auto result = split(node);
        auto&& [new_node, split_key] = result;
        parent.insert_split_result(index + 1, split_key, new_node.address());
        return std::move(result);
    }

    /// Keep the tree's invariants after removing a value from a leaf.
    /// Merges leaf nodes and internal nodes until they are at least half full.
    void erase_impl(leaf_type leaf, node_stack_type& stack) {
        if (leaf.address() == get_anchor()->leftmost || leaf.address() == get_anchor()->rightmost) {
            if (leaf.size() == 0) {
                destroy(leaf);
                if (stack.size() > 0) {
                    leaf = {};
                    propagate_erase(stack);
                } else {
                    get_anchor()->root = {};
                    get_anchor()->height = 0;
                    get_anchor().dirty();
                }
            }
            return;
        }

        if (leaf.size() >= leaf_type::min_size())
            return;

        EXTPP_ASSERT(stack.size() > 0, "Must have parents."); // Leaf roots are handled above.
        {
            auto [parent_addr, parent_index] = stack.back();
            internal_type parent = state().access(parent_addr);
            leaf_type left, right;

            if (parent_index > 0) {
                left = state().access(state().cast_leaf(parent.get_child(parent_index - 1)));
                if (left.size() > leaf_type::min_size() || (left.address() == get_anchor()->leftmost && left.size() > 1)) {
                    leaf.take_left(state(), parent, parent_index, left);
                    move_iterators(leaf, 0, leaf.size() - 1, leaf, 1);
                    move_iterators(left, left.size(), left.size() + 1, leaf, 0);
                    return;
                }
            }

            if (parent_index < parent.size() - 1) {
                right = state().access(state().cast_leaf(parent.get_child(parent_index + 1)));
                if (right.size() > leaf_type::min_size() || (right.address() == get_anchor()->rightmost && right.size() > 1)) {
                    leaf.take_right(state(), parent, parent_index, right);
                    move_iterators(right, 0, 1, leaf, leaf.size() - 1);
                    move_iterators(right, 1, right.size() + 1, right, 0);
                    return;
                }
            }

            auto merge = [&](u32 right_index, const leaf_type& right, const leaf_type& left) {
                u32 right_size = right.size();
                u32 left_size = left.size();
                right.merge_left(state(), parent, right_index, left);
                move_iterators(right, 0, right_size, right, left_size);
                move_iterators(left, 0, left_size, right, 0);
            };

            u32& removed_child_index = stack.back().child;
            if (left) {
                merge(parent_index, leaf, left);
                removed_child_index = parent_index - 1;
                destroy(left);
            } else {
                merge(parent_index + 1, right, leaf);
                removed_child_index = parent_index;
                destroy(leaf);
            }
        }
        propagate_erase(stack);
    }

    /// Propagates the deletion of a child node through the tree.
    /// If the iteration reaches the root node and that node ends up with
    /// only 1 child, shrink the tree by one level.
    void propagate_erase(node_stack_type& stack) {
        auto next = [&]{
            EXTPP_ASSERT(stack.size() > 0, "Stack is empty.");
            auto [addr, child] = stack.back();
            stack.pop_back();
            return std::make_tuple(state().access(addr), child);
        };

        auto [node, removed_child] = next();
        while (1) {
            EXTPP_ASSERT(node.size() >= 2, "Node is too empty.");

            node.remove(removed_child);
            if (stack.empty()) {
                EXTPP_ASSERT(node.address() == get_anchor()->root, "Must be at the root.");
                if (node.size() == 1) {
                    get_anchor()->root = node.get_child(0);
                    get_anchor()->height -= 1;
                    get_anchor().dirty();
                    destroy(node);
                }
                break;
            }

            if (node.size() >= internal_type::min_size())
                break;

            auto [parent, parent_index] = next();
            internal_type left, right;

            if (parent_index > 0) {
                left = state().access(state().cast_internal(parent.get_child(parent_index - 1)));
                if (left.size() > internal_type::min_size()) {
                    node.take_left(state(), parent, parent_index, left);
                    break;
                }
            }

            if (parent_index < parent.size() - 1) {
                right = state().access(state().cast_internal(parent.get_child(parent_index + 1)));
                if (right.size() > internal_type::min_size()) {
                    node.take_right(state(), parent, parent_index, right);
                    break;
                }
            }

            if (left) {
                node.merge_left(state(), parent, parent_index, left);
                destroy(left);
                removed_child = parent_index - 1;
            } else {
                right.merge_left(state(), parent, parent_index + 1, node);
                destroy(node);
                removed_child = parent_index;
            }
            node = std::move(parent);
        }
    }

    /// Removes a leaf node from the linked list of leaf nodes and then frees it.
    void destroy(const leaf_type& leaf) {
        if (leaf.prev()) {
            leaf_type prev = state().access(leaf.prev());
            prev.set_next(leaf.next());
        } else {
            get_anchor()->leftmost = leaf.next();
            get_anchor().dirty();
        }

        if (leaf.next()) {
            leaf_type next = state().access(leaf.next());
            next.set_prev(leaf.prev());
        } else {
            get_anchor()->rightmost = leaf.prev();
            get_anchor().dirty();
        }

        state().free(leaf.address());
    }

    /// Destroy an internal node by freeing it.
    void destroy(const internal_type& internal) {
        state().free(internal.address());
    }

    /// Move the iterator positions from old_leaf, [first_index, last_index)
    /// to new_leaf, [new_first_index, ...).
    void move_iterators(const leaf_type& old_leaf,
                        u32 first_index, u32 last_index,
                        const leaf_type& new_leaf,
                        u32 new_first_index) {
        cursor_buffer_t buffer;
        m_iterator_map.find_iterators(old_leaf.address(), first_index, last_index, buffer);

        for (cursor *i : buffer) {
            u32 offset = i->base().index() - first_index;
            *i = iterator(state(), new_leaf, new_first_index + offset);
        }
    }

    void invalidate_iterators(const leaf_type& leaf, u32 index) {
        cursor_buffer_t buffer;
        m_iterator_map.find_iterators(leaf.address(), index, index + 1, buffer);

        for (cursor *i : buffer) {
            i->reset();
        }
    }

    state_type& state() { return *this; }
    const state_type& state() const { return *this; }

    void check_instance(const iterator& iter) const {
        EXTPP_ASSERT(iter.state() == &state(), "Iterator does not belong to this btree instance.");
        unused(iter);
    }

    void check_valid(const iterator& iter) const {
        check_instance(iter);
        EXTPP_ASSERT(iter != end(), "Iterator does not point to a valid element.");
    }

private:
    /// Contains references to safe iterators. These have to be adjusted when
    /// elements are inserted/removed or nodes are split/merged.
    mutable cursor_map m_iterator_map;

    /// Buffer for parents in tree traversals.
    node_stack_type m_stack_buf;
};

template<typename V, typename K, typename C, u32 B>
class btree<V, K, C, B>::cursor : public btree_detail::safe_iterator_base<btree, iterator, cursor> {
public:
    cursor() = default;

    cursor(iterator iter)
        : cursor::safe_iterator_base{get_iterator_map(iter), std::move(iter)}
    {}

    cursor(const cursor&) = default;
    cursor(cursor&&) noexcept = default;

    cursor& operator=(iterator iter) {
        auto& map = get_iterator_map(iter);
        reset(map, std::move(iter));
        return *this;
    }

    cursor& operator=(const cursor&) = default;
    cursor& operator=(cursor&&) noexcept = default;

private:
    friend class btree;
    friend class cursor::safe_iterator_base;

    using cursor::safe_iterator_base::reset;

    static auto& get_iterator_map(const iterator& pos) {
        const state_type* state = pos.state();
        EXTPP_ASSERT(state, "Invalid iterator.");
        return static_cast<const btree*>(state)->m_iterator_map;
    }
};

template<typename V, typename K, typename C, u32 B>
class btree<V, K, C, B>::visitor {
public:
    using value_type = typename btree::value_type;
    using key_type = typename btree::key_type;
    using node_address = typename btree::node_address;

public:
    /// True if the visitor points to a node.
    bool valid() const { return m_stack.size() > 0; }

    /// The height of the tree.
    u32 height() const { return m_state->get_anchor()->height; }

    /// The current level in the tree (leaves are at level 0).
    u32 level() const { return height() - m_stack.size(); }

    bool is_leaf() const { return valid() && level() == 0; }
    bool is_internal() const { return valid() && level() > 0; }
    bool is_root() const { return valid() && level() == height() - 1; }
    bool has_parent() const { return valid() && m_stack.size() > 1; }

    /// Returns the address of the current node.
    /// \pre `valid()`.
    node_address address() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return m_stack.back();
    }

    /// Returns the address of this node's parent node.
    /// \pre `valid()`,
    node_address parent_address() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        if (m_stack.size() <= 1)
            return {};
        return m_stack[m_stack.size() - 2];
    }

    /// Returns the address of this leaf node's successor.
    /// \pre `is_leaf()`.
    node_address successor_address() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return as_leaf().next();
    }

    /// Returns the address of this leaf node's predecessor.
    /// \pre `is_leaf()`.
    node_address predecessor_address() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return as_leaf().prev();
    }

    /// Returns the key at the given index.
    /// \pre `is_internal() && index < size() - 1`.
    key_type key(u32 index) const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return as_internal().get_key(index);
    }

    /// Returns the index at the given key.
    /// \pre `is_internal() && index < size()`.
    node_address child(u32 index) const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return as_internal().get_child(index);
    }

    /// Returns the value at the given index.
    /// \pre `is_leaf() && index < size()`.
    value_type value(u32 index) const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return as_leaf().get(index);
    }

    /// Returns the number of children (for internal nodes) or
    /// the number of values (for leaves).
    /// \pre `valid()`.
    u32 size() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        if (is_leaf()) return as_leaf().size();
        return as_internal().size();
    }

    /// Visits the root of the tree.
    void visit_root() {
        clear();
        if (auto root = m_state->get_anchor()->root) {
            push(root);
        }
    }

    /// Visits the child with the given index.
    /// \pre `is_internal() && index < size()`.
    void visit_child(u32 index) {
        EXTPP_ASSERT(is_internal(), "Must be an internal node.");
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        push(as_internal().get_child(index));
    }

    /// Visits the parent of this node (if any).
    void visit_parent() {
        if (m_stack.size() > 1)
            pop();
    }

private:
    friend class btree;

    visitor(const btree* tree)
        : m_state(&tree->state())
    {
        visit_root();
    }

    void push(node_address addr) {
        m_stack.emplace_back(addr);
        if (m_stack.size() == m_state->get_anchor()->height)
            m_current = m_state->access(m_state->cast_leaf(addr));
        else
            m_current = m_state->access(m_state->cast_internal(addr));
    }

    void pop() {
        m_stack.pop_back();
        if (m_stack.size() > 0) {
            m_current = m_state->access(m_state->cast_internal(m_stack.back()));
        } else {
            m_current = std::monostate();
        }
    }

    void clear() {
        m_stack.clear();
        m_current = std::monostate();
    }

    const leaf_type& as_leaf() const {
        EXTPP_ASSERT(std::holds_alternative<leaf_type>(m_current), "Not a leaf node.");
        return std::get<leaf_type>(m_current);
    }

    const internal_type& as_internal() const {
        EXTPP_ASSERT(std::holds_alternative<internal_type>(m_current), "Not an internal node.");
        return std::get<internal_type>(m_current);
    }

private:
    const state_type* m_state = nullptr;
    std::vector<node_address> m_stack;
    std::variant<std::monostate, leaf_type, internal_type> m_current;
};

} // namespace extpp

#endif // EXTPP_BTREE_HPP
