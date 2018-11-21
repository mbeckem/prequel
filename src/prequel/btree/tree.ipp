#ifndef PREQUEL_BTREE_TREE_IPP
#define PREQUEL_BTREE_TREE_IPP

#include <prequel/btree/tree.hpp>

#include <prequel/detail/fix.hpp>
#include <prequel/exception.hpp>
#include <prequel/formatting.hpp>

#include <fmt/ostream.h>

#include <algorithm>

namespace prequel::detail::btree_impl {

inline tree::tree(anchor_handle<anchor> _anchor, const raw_btree_options& opts, allocator& alloc)
    : uses_allocator(alloc)
    , m_anchor(std::move(_anchor))
    , m_options(opts) {
    if (m_options.value_size == 0)
        PREQUEL_THROW(bad_argument("Zero value size."));
    if (m_options.key_size == 0)
        PREQUEL_THROW(bad_argument("Zero key size."));
    if (m_options.key_size > max_key_size)
        PREQUEL_THROW(
            bad_argument(fmt::format("Key sizes larger than {} are not supported.", max_key_size)));
    if (!m_options.derive_key)
        PREQUEL_THROW(bad_argument("No derive_key function provided."));
    if (!m_options.key_less)
        PREQUEL_THROW(bad_argument("No key_less function provided."));

    m_leaf_capacity = leaf_node::capacity(get_engine().block_size(), value_size());
    m_internal_max_children =
        internal_node::compute_max_children(get_engine().block_size(), key_size());
    m_internal_min_children = internal_node::compute_min_children(m_internal_max_children);

    if (m_leaf_capacity < 2) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Block size {} is too small (cannot fit 2 values into one leaf)",
                        get_engine().block_size())));
    }
    if (m_internal_max_children < 4) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Block size {} is too small (cannot fit 4 children into one internal node)",
                        get_engine().block_size())));
    }
}

inline tree::~tree() {
    // Invalidate all existing cursors.
    for (auto& cursor : m_cursors) {
        cursor.reset_to_invalid();
        cursor.m_tree = nullptr;
    }
}

inline bool tree::value_less(const byte* left_value, const byte* right_value) const {
    key_buffer left_key, right_key;
    derive_key(left_value, left_key.data());
    derive_key(right_value, right_key.data());
    return key_less(left_key.data(), right_key.data());
}

template<tree::seek_bound_t which>
inline void tree::seek_bound(const byte* key, cursor& cursor) const {
    PREQUEL_ASSERT(cursor.m_tree == this, "Cursor does not belong to this tree.");

    if (height() == 0) {
        // Tree is empty.
        cursor.reset_to_invalid();
        return;
    }

    cursor.reset_to_zero();
    cursor.m_flags |= cursor.INPROGRESS;

    // For every level of internal nodes
    cursor.m_parents.resize(height() - 1);
    block_index current = root();
    for (u32 level = height() - 1; level > 0; --level) {
        auto& entry = cursor.m_parents[height() - 1 - level];
        entry.node = read_internal(current);
        if constexpr (which == seek_bound_upper) {
            entry.index = upper_bound(entry.node, key);
        } else /* find and lower */ {
            entry.index = lower_bound(entry.node, key);
        }
        current = entry.node.get_child(entry.index);
    }

    // Search in the leaf node
    cursor.m_leaf = read_leaf(current);
    if constexpr (which == seek_bound_upper) {
        cursor.m_index = upper_bound(cursor.m_leaf, key);
    } else /* find and lower */ {
        cursor.m_index = lower_bound(cursor.m_leaf, key);
    }

    if (cursor.m_index == cursor.m_leaf.get_size()) {
        if constexpr (which == seek_bound_lower || which == seek_bound_upper) {
            // The upper/lower bound must be in the next node.
            // This is because parent keys in the upper levels might be out of date, i.e.
            // they might be larger than the max element in their subtree.
            if (!next_leaf(cursor)) {
                cursor.reset_to_invalid();
                return;
            }
            cursor.m_index = 0;
        } else {
            // Don't do that for normal find though; it must fail here.
            cursor.reset_to_invalid();
            return;
        }
    }

    cursor.m_flags &= ~cursor.INPROGRESS;
}

inline bool tree::next_leaf(cursor& cursor) const {
    // Find a parent that is not yet at it's last index.
    auto rpos =
        std::find_if(cursor.m_parents.rbegin(), cursor.m_parents.rend(), [&](const auto& entry) {
            return entry.index + 1 < entry.node.get_child_count();
        });
    if (rpos == cursor.m_parents.rend()) {
        return false;
    }

    // Increment the index in this parent node, then walk to the leaf level and set all indices to 0.
    // Note that rpos.base() is the element *after* the parent.
    auto pos = std::prev(rpos.base());
    pos->index++;
    for (auto child_pos = pos + 1; child_pos != cursor.m_parents.end(); ++child_pos, ++pos) {
        child_pos->node = read_internal(pos->node.get_child(pos->index));
        child_pos->index = 0;
    }

    cursor.m_leaf = read_leaf(pos->node.get_child(pos->index));
    return true;
}

inline bool tree::prev_leaf(cursor& cursor) const {
    // Find a parent that is not yet at index 0.
    auto rpos = std::find_if(cursor.m_parents.rbegin(), cursor.m_parents.rend(),
                             [&](const auto& entry) { return entry.index > 0; });
    if (rpos == cursor.m_parents.rend()) {
        return false;
    }

    // Decrement the index in this parent node, then walk to the leaf level and set all indices to size - 1.
    // Note that rpos.base() is the element *after* the parent.
    auto pos = std::prev(rpos.base());
    pos->index -= 1;
    for (auto child_pos = pos + 1; child_pos != cursor.m_parents.end(); ++child_pos, ++pos) {
        child_pos->node = read_internal(pos->node.get_child(pos->index));
        child_pos->index = child_pos->node.get_child_count() - 1;
    }

    cursor.m_leaf = read_leaf(pos->node.get_child(pos->index));
    return true;
}

inline void tree::lower_bound(const byte* key, cursor& cursor) const {
    return seek_bound<seek_bound_lower>(key, cursor);
}

inline void tree::upper_bound(const byte* key, cursor& cursor) const {
    return seek_bound<seek_bound_upper>(key, cursor);
}

inline void tree::find(const byte* key, cursor& cursor) const {
    seek_bound<seek_bound_find>(key, cursor);
    if (cursor.at_end())
        return;

    PREQUEL_ASSERT(cursor.m_index < cursor.m_leaf.get_size(), "Invalid index.");
    if (!value_equal_key(cursor.m_leaf.get(cursor.m_index), key)) {
        cursor.reset_to_invalid();
    }
}

inline u32 tree::lower_bound(const leaf_node& leaf, const byte* search_key) const {
    const u32 size = leaf.get_size();
    index_iterator result = std::lower_bound(index_iterator(0), index_iterator(size), search_key,
                                             [&](u32 i, const byte* search_key) {
                                                 key_buffer buffer;
                                                 derive_key(leaf.get(i), buffer.data());
                                                 return key_less(buffer.data(), search_key);
                                             });
    return *result;
}

inline u32 tree::lower_bound(const internal_node& internal, const byte* search_key) const {
    PREQUEL_ASSERT(internal.get_child_count() > 1, "Not enough children in this internal node");
    // internal.get_size() is the number of children, not the number of keys.
    const u32 keys = internal.get_child_count() - 1;
    index_iterator result = std::lower_bound(
        index_iterator(0), index_iterator(keys), search_key,
        [&](u32 i, const byte* search_key) { return key_less(internal.get_key(i), search_key); });
    return *result;
}

inline u32 tree::upper_bound(const leaf_node& leaf, const byte* search_key) const {
    const u32 size = leaf.get_size();
    index_iterator result = std::upper_bound(index_iterator(0), index_iterator(size), search_key,
                                             [&](const byte* search_key, u32 i) {
                                                 key_buffer buffer;
                                                 derive_key(leaf.get(i), buffer.data());
                                                 return key_less(search_key, buffer.data());
                                             });
    return *result;
}

inline u32 tree::upper_bound(const internal_node& internal, const byte* search_key) const {
    PREQUEL_ASSERT(internal.get_child_count() > 1, "Not enough children in this internal node");
    // internal.get_size() is the number of children, not the number of keys.
    const u32 keys = internal.get_child_count() - 1;
    index_iterator result = std::upper_bound(
        index_iterator(0), index_iterator(keys), search_key,
        [&](const byte* search_key, u32 i) { return key_less(search_key, internal.get_key(i)); });
    return *result;
}

// Insert a new value into the btree. Leave the cursor pointing to the position where the value
// was inserted. Do nothing if a value with the same key already existed (the cursor is still repositioned, though).
//
// 1.   Walk down the stack and split internal nodes that are full. This ensures that we can always insert a new entry if we need to.
//      It saves some effort to walk back up the stack in the case that a node has been split and its parent is full.
//      It might be more performant in the future than the alternative because we dont have to lock nodes
//      for an extended period of time and just modify them once. (but node locks are in the very remote future).
//      Note that this in only done for internal nodes, leaf nodes at the lowest level are not split in advance.
// 2.   Perform lower bound search at every node. When we reached the leaf we either found a place
//      where we can insert the new value or we found an existing value with the same key.
//      The leaf might be full and may therefore have to be split. Inserting a new leaf always succeeds because of (1).
// 3.   Keep cursors updated in the meantime. Every time a record is inserted or a node is split we must
//      ensure that the existing cursors keep pointing to their old values.
//
//      Note:
//          sqlite solves this in a different way: when a value moves, the cursor(s) that point to it are invalidated
//          and remember which key they pointed to in their local storage. When the value is required again, they simply
//          traverse the tree and move to the value again. I don't like that solution right now.
inline bool tree::insert(const byte* value, cursor& cursor) {
    if (empty()) {
        leaf_node leaf = create_leaf();
        leaf.set(0, value);
        leaf.set_size(1);

        set_height(1);
        set_size(1);
        set_root(leaf.index());
        set_leftmost(leaf.index());
        set_rightmost(leaf.index());

        cursor.reset_to_zero();
        cursor.m_leaf = std::move(leaf);
        cursor.m_index = 0;
        return true;
    }

    cursor.reset_to_zero();
    cursor.m_flags |= cursor.INPROGRESS;

    // Find the correct position where we could insert the new value.
    // The seek function already splits internal nodes along the way.
    // The leaf node might be full though.
    key_buffer key;
    derive_key(value, key.data());
    seek_insert_location(key.data(), cursor);

    const leaf_node leaf = cursor.m_leaf;
    const u32 insert_index = cursor.m_index;
    const u32 leaf_size = cursor.m_leaf.get_size();
    if (insert_index < leaf_size && value_equal_key(leaf.get(insert_index), key.data())) {
        cursor.m_flags &= ~cursor.INPROGRESS;
        return false; // Equivalent value exists.
    }

    if (leaf_size < leaf.max_size()) {
        // Simple case: enough space in the leaf.
        leaf.insert_nonfull(insert_index, value);
        for (auto& c : m_cursors) {
            if (c.invalid() || c.m_leaf.index() != leaf.index())
                continue;

            if (&c != &cursor && c.m_index >= insert_index)
                c.m_index += 1;
        }
    } else {
        // The leaf is full.
        leaf_node new_leaf = create_leaf();

        // Number of remaining entries in the existing leaf.
        const u32 left_size = [&] {
            if (leaf.index() == rightmost())
                return leaf_size;
            if (leaf.index() == leftmost())
                return u32(1);
            return (leaf_size + 2) / 2;
        }();
        leaf.insert_full(insert_index, value, left_size, new_leaf);

        // The split key and the new leaf pointer must be inserted into the parent.
        key_buffer split_key;
        derive_key(leaf.get(left_size - 1), split_key.data());

        // New leaf is to the right of the old one, leftmost can be ignored.
        if (leaf.index() == rightmost())
            set_rightmost(new_leaf.index());

        // Insert the pointer of the new leaf into the parent node (if any, else create a new root).
        // Then update all cursors so that they remain consistent.
        if (height() == 1) {
            const internal_node new_root =
                create_root(leaf.index(), new_leaf.index(), split_key.data());

            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;

                PREQUEL_ASSERT(c.m_leaf.index() == leaf.index(),
                               "Must point to the existing root.");
                PREQUEL_ASSERT(c.m_parents.empty(), "There cannot be any internal nodes.");

                if (&c != &cursor && c.m_index >= insert_index)
                    c.m_index += 1;

                cursor::internal_entry entry;
                entry.node = new_root;
                if (c.m_index >= left_size) {
                    c.m_leaf = new_leaf;
                    c.m_index -= left_size;
                    entry.index = 1;
                } else {
                    entry.index = 0;
                }
                c.m_parents.push_back(std::move(entry));
            }
        } else {
            // Parent is not full because of preparatory split for internal nodes.
            const internal_node& parent = cursor.m_parents.back().node;
            const u32 index_in_parent = cursor.m_parents.back().index;
            parent.insert_split_result(index_in_parent + 1, split_key.data(), new_leaf.index());

            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;

                auto& parent_entry = c.m_parents.back();
                if (parent_entry.node.index() != parent.index())
                    continue;

                if (parent_entry.index == index_in_parent) {
                    PREQUEL_ASSERT(c.m_leaf.index() == leaf.index(),
                                   "Inconsistent leaf node for parent index.");
                    PREQUEL_ASSERT(parent_entry.node.get_child(index_in_parent) == leaf.index(),
                                   "Inconsistent child pointer.");

                    if (&c != &cursor && c.m_index >= insert_index)
                        c.m_index += 1;

                    if (c.m_index >= left_size) {
                        c.m_leaf = new_leaf;
                        c.m_index -= left_size;
                        parent_entry.index += 1;
                    }
                } else if (parent_entry.index >= index_in_parent + 1) {
                    parent_entry.index += 1;
                }
            }
        }
    }

    cursor.m_flags &= ~cursor.INPROGRESS;
    set_size(size() + 1);
    return true;
}

inline void tree::seek_insert_location(const byte* key, cursor& cursor) {
    PREQUEL_ASSERT(height() > 0, "Tree must not be empty at this point.");

    // For every level of internal nodes.
    block_index current = root();
    for (u32 level = height() - 1; level > 0; --level) {
        internal_node internal = read_internal(current);

        // Find the appropriate child node and push it as a stack entry. The entry might change
        // as the result of a node split in the code below.
        {
            cursor::internal_entry entry;
            entry.node = internal;
            entry.index = lower_bound(entry.node, key);
            cursor.m_parents.push_back(std::move(entry));
        }

        // Split if full, then insert new internal node into the parent.
        if (internal.get_child_count() == internal.max_children()) {
            key_buffer split_key;
            internal_node new_internal = split(internal, split_key.data());

            if (cursor.m_parents.size() == 1) {
                // Root split
                internal_node new_root =
                    create_root(internal.index(), new_internal.index(), split_key.data());
                apply_root_split(new_root, internal, new_internal);
            } else {
                // Split node with a parent.
                const auto& parent_entry = cursor.m_parents[cursor.m_parents.size() - 2];
                const internal_node& parent = parent_entry.node;
                const u32 index_in_parent = parent_entry.index;

                PREQUEL_ASSERT(parent.get_child_count() < parent.max_children(),
                               "Parent must not be full.");
                PREQUEL_ASSERT(parent.get_child(index_in_parent) == internal.index(),
                               "Parent does not point to this node at the given index");
                parent.insert_split_result(index_in_parent + 1, split_key.data(),
                                           new_internal.index());
                apply_child_split(parent, level, index_in_parent, internal, new_internal);
            }
        }

        // Update with (possibly changed) node info.
        const auto& last_entry = cursor.m_parents.back();
        current = last_entry.node.get_child(last_entry.index);
    }

    // Reached the leaf level. Note that the leaf node can be full at this point.
    cursor.m_leaf = read_leaf(current);
    cursor.m_index = lower_bound(cursor.m_leaf, key);
}

inline internal_node
tree::create_root(block_index left_child, block_index right_child, const byte* split_key) {
    // Grow by one level.
    internal_node new_root = create_internal();
    new_root.set_child(0, left_child);
    new_root.set_child(1, right_child);
    new_root.set_key(0, split_key);
    new_root.set_child_count(2);

    set_height(height() + 1);
    set_root(new_root.index());

    return new_root;
}

internal_node tree::split(const internal_node& old_internal, byte* split_key) {
    internal_node new_internal = create_internal();
    old_internal.split(new_internal, split_key);
    return new_internal;
}

inline void tree::apply_root_split(const internal_node& new_root, const internal_node& left_internal,
                                   const internal_node& right_internal) {
    // All children with index >= left_child_count have moved to the right node.
    const u32 left_child_count = left_internal.get_child_count();

    for (auto& cursor : m_cursors) {
        if (cursor.invalid())
            continue;

        PREQUEL_ASSERT(!cursor.m_parents.empty(),
                       "Must have internal nodes on the stack because the root was internal.");
        PREQUEL_ASSERT(cursor.m_parents.front().node.index() == left_internal.index(),
                       "Must point to the old root.");

        auto& root_entry = cursor.m_parents.front();

        cursor::internal_entry new_entry;
        new_entry.node = new_root;

        if (root_entry.index >= left_child_count) {
            root_entry.node = right_internal;
            root_entry.index -= left_child_count;
            new_entry.index = 1;
        } else {
            new_entry.index = 0;
        }

        cursor.m_parents.insert(cursor.m_parents.begin(), std::move(new_entry));
    }
}

inline void
tree::apply_child_split(const internal_node& parent, u32 left_level, u32 left_index,
                        const internal_node& left_internal, const internal_node& right_internal) {
    PREQUEL_ASSERT(left_level > 0, "Left node must not be at leaf level.");
    PREQUEL_ASSERT(left_level < height() - 1, "Left node must not be the root.");
    PREQUEL_ASSERT(parent.get_child(left_index) == left_internal.index(),
                   "Parent must point to the left node.");
    PREQUEL_ASSERT(parent.get_child(left_index + 1) == right_internal.index(),
                   "Parent must point to the right node.");

    // Examine only these indices. Lower levels might not yet be initialized for the seeking cursor.
    const u32 children_stack_index = height() - 1 - left_level;
    const u32 parent_stack_index = children_stack_index - 1;

    // All children with index >= left_child count are now in the right node.
    const u32 left_child_count = left_internal.get_child_count();
    for (auto& cursor : m_cursors) {
        if (cursor.invalid())
            continue;

        PREQUEL_ASSERT(parent_stack_index < cursor.m_parents.size(),
                       "Parent stack index out of bounds.");
        PREQUEL_ASSERT(children_stack_index >= 1 && children_stack_index < cursor.m_parents.size(),
                       "Children stack index out of bounds.");

        // Fixup entries of neighboring internal nodes.
        auto& parent_entry = cursor.m_parents[parent_stack_index];
        if (parent_entry.node.index() != parent.index())
            continue;

        if (parent_entry.index == left_index) {
            // Fixup entries of cursors in the split nodes
            auto& child_entry = cursor.m_parents[children_stack_index];
            PREQUEL_ASSERT(parent_entry.node.get_child(parent_entry.index)
                               == child_entry.node.index(),
                           "Parent must point to the old child.");
            PREQUEL_ASSERT(child_entry.node.index() == left_internal.index(),
                           "Old child must be the left internal node.");

            if (child_entry.index >= left_child_count) {
                child_entry.node = right_internal;
                child_entry.index -= left_child_count;
                parent_entry.index += 1;
            }
        } else if (parent_entry.index > left_index) {
            parent_entry.index += 1;
        }
    }
}

// This is the 'normal' erase algorithm for btrees because the cursor already has references
// to all nodes the stack an we do not need to discover the position of the to-be-deleted value.
// We still have to respect the fact that the preparatory splits during insertion will result
// in slightly less than half full internal nodes.
inline void tree::erase(cursor& cursor) {
    PREQUEL_ASSERT(!(cursor.m_flags & cursor.INVALID), "Cursor must not be invalid.");
    PREQUEL_ASSERT(!(cursor.m_flags & cursor.DELETED),
                   "Cursor must not point to a deleted element.");
    PREQUEL_ASSERT(cursor.m_parents.size() == this->height() - 1,
                   "Not enough nodes on the parent stack.");
    PREQUEL_ASSERT(height() > 0, "The tree cannot be empty.");

    leaf_node leaf = cursor.m_leaf;
    const u32 index = cursor.m_index;

    leaf.remove(index);
    set_size(size() - 1);

    for (auto& c : m_cursors) {
        if (c.invalid() || c.m_leaf.index() != leaf.index())
            continue;
        if (c.m_index == index)
            c.m_flags |= c.DELETED;
        else if (c.m_index > index)
            c.m_index -= 1;
    }

    // Handle the root leaf.
    if (cursor.m_parents.empty()) {
        if (leaf.get_size() == 0) {
            PREQUEL_ASSERT(height() == 1, "Inconsistent tree height.");
            free_leaf(leaf.index());
            set_leftmost({});
            set_rightmost({});
            set_root({});
            set_height(0);
            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;
                c.reset_to_invalid(c.m_flags);
            }
        }
        return;
    }

    // Handle leftmost/rightmost leaf nodes.
    PREQUEL_ASSERT(height() > 1, "We are not at the leaf level.");
    if (leaf.index() == leftmost() || leaf.index() == rightmost()) {

        // Usually empty leftmost/rightmost leaves are only deleted when they become
        // completely empty. This is an optimization for the likely case that the user
        // inserts and deletes at the end or the beginning (splitting is optimized similarily).
        if (leaf.get_size() == 0) {
            // Other nodes remain. Move cursors from this node (they are "deleted")
            // to the left/right neighbor and propagate the node erasure to the parents.
            const internal_node& parent = cursor.m_parents.back().node;
            u32 index_in_parent = cursor.m_parents.back().index;

            u32 neighbor_index;
            leaf_node neighbor;
            u32 index_in_neighbor;
            if (leaf.index() == leftmost()) {
                neighbor_index = index_in_parent + 1;
                neighbor = read_leaf(parent.get_child(neighbor_index));
                index_in_neighbor = 0;
            } else {
                neighbor_index = index_in_parent - 1;
                neighbor = read_leaf(parent.get_child(neighbor_index));
                index_in_neighbor = neighbor.get_size();
            }

            // I'm not a fan of loading the neighbor here because it means an additional I/O
            // just to load the other leaf node in order to move the cursor there.
            for (auto& c : m_cursors) {
                if (c.invalid() || c.m_leaf.index() != leaf.index())
                    continue;
                c.m_leaf = neighbor;
                c.m_index = index_in_neighbor;
                c.m_parents.back().index = neighbor_index;
            }

            if (leaf.index() == leftmost())
                set_leftmost(neighbor.index());
            else
                set_rightmost(neighbor.index());
            free_leaf(leaf.index());
            propagate_leaf_deletion(cursor, leaf.index(), index_in_parent);

            // If there are only two leaves remaining we will merge them back together if both are
            // somewhat empty. Two leaf nodes with a single element each just look too stupid.
        } else if (leaf_nodes() == 2 && size() <= leaf.max_size()) {
            const internal_node& parent = cursor.m_parents.back().node;
            if (leaf.index() == leftmost()) {
                leaf_node right = read_leaf(parent.get_child(1));
                merge_leaf(parent, leaf, 0, right, 1);
                free_leaf(right.index());
                propagate_leaf_deletion(cursor, right.index(), 1);
            } else {
                leaf_node left = read_leaf(parent.get_child(0));
                merge_leaf(parent, leaf, 1, left, 0);
                free_leaf(left.index());
                propagate_leaf_deletion(cursor, left.index(), 0);
            }
        }

        return;
    }

    // Handle all other leaf nodes. Leaf is not the root and not leftmost/rightmost.
    if (leaf.get_size() >= leaf.min_size())
        return;
    PREQUEL_ASSERT(cursor.m_parents.size() > 0, "Must have parents.");
    const internal_node& parent = cursor.m_parents.back().node;
    const u32 index_in_parent = cursor.m_parents.back().index;

    // Attempt to steal entries from the right node.
    leaf_node right;
    if (index_in_parent + 1 < parent.get_child_count()) {
        right = read_leaf(parent.get_child(index_in_parent + 1));
        if (right.get_size() > right.min_size()
            || (right.index() == rightmost() && right.get_size() > 1)) {
            steal_leaf_entry(parent, leaf, index_in_parent, right, index_in_parent + 1);
            return;
        }
    }

    // Attempt to steal entries from the left node.
    leaf_node left;
    if (index_in_parent > 0) {
        left = read_leaf(parent.get_child(index_in_parent - 1));
        if ((left.get_size() > left.min_size()
             || (left.index() == leftmost() && left.get_size() > 1))) {
            steal_leaf_entry(parent, leaf, index_in_parent, left, index_in_parent - 1);
            return;
        }
    }

    // Merge with one of the leaves.
    block_index child_node;
    u32 child_node_index;
    if (right.valid()) {
        merge_leaf(parent, leaf, index_in_parent, right, index_in_parent + 1);
        child_node = right.index();
        child_node_index = index_in_parent + 1;
    } else if (left.valid()) {
        merge_leaf(parent, leaf, index_in_parent, left, index_in_parent - 1);
        child_node = left.index();
        child_node_index = index_in_parent - 1;
    } else {
        PREQUEL_UNREACHABLE("Must have a left or right neighbor.");
    }

    free_leaf(child_node);
    propagate_leaf_deletion(cursor, child_node, child_node_index);
}

// Called when the leaf node `child_node` has been merged with a neighbor and now has to be removed from its parent.
// The internal nodes up the stack might have to be merged as well.
inline void
tree::propagate_leaf_deletion(cursor& cursor, block_index child_node, u32 child_node_index) {
    PREQUEL_ASSERT(!cursor.m_parents.empty(), "There must be internal node parents.");

    // Walk up the stack and merge nodes if necessary.
    u32 stack_index = cursor.m_parents.size() - 1;
    internal_node node = cursor.m_parents[stack_index].node;
    while (1) {
        PREQUEL_ASSERT(cursor.m_parents.at(stack_index).node.index() == node.index(),
                       "Internal node must be at that level.");
        PREQUEL_ASSERT(node.get_child(child_node_index) == child_node,
                       "Parent must point to the child.");
        PREQUEL_ASSERT(node.get_child_count() >= 2, "Node is too empty.");

        // Remove the child and shift cursors to the left.
        node.remove_child(child_node_index);
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& entry = c.m_parents[stack_index];
            if (entry.node.index() == node.index()) {
                PREQUEL_ASSERT(entry.index != child_node_index,
                               "Nobody must point to the deleted child.");
                if (entry.index > child_node_index)
                    entry.index -= 1;
            }
        }

        // We're at the root. If it becomes too empty, shrink the tree by one level.
        if (stack_index == 0) {
            PREQUEL_ASSERT(node.index() == root(), "Must be the root.");
            if (node.get_child_count() == 1) {
                set_root(node.get_child(0));
                set_height(height() - 1);
                free_internal(node.index());

                for (auto& c : m_cursors) {
                    if (c.invalid())
                        continue;
                    PREQUEL_ASSERT(!c.m_parents.empty()
                                       && c.m_parents[0].node.index() == node.index(),
                                   "All cursors must point to the root.");
                    c.m_parents.erase(c.m_parents.begin());
                }
            }
            break;
        }

        // Not the root. We must have a minimum child count (or merge)
        // and we have a parent node.
        if (node.get_child_count() >= node.min_children())
            break;

        internal_node parent = cursor.m_parents[stack_index - 1].node;
        u32 index_in_parent = cursor.m_parents[stack_index - 1].index;

        // Attempt to steal a (key, child) pair from the right node.
        internal_node right;
        if (index_in_parent + 1 < parent.get_child_count()) {
            right = read_internal(parent.get_child(index_in_parent + 1));
            if (right.get_child_count() > right.min_children()) {
                steal_internal_entry(parent, stack_index, node, index_in_parent, right,
                                     index_in_parent + 1);
                break;
            }
        }

        // Attempt to steal a (key, child) pair from the left node.
        internal_node left;
        if (index_in_parent > 0) {
            left = read_internal(parent.get_child(index_in_parent - 1));
            if (left.get_child_count() > left.min_children()) {
                steal_internal_entry(parent, stack_index, node, index_in_parent, left,
                                     index_in_parent - 1);
                break;
            }
        }

        // Merge with one of the neighbors.
        if (left.valid()) {
            merge_internal(parent, stack_index, node, index_in_parent, left, index_in_parent - 1);
            child_node = left.index();
            child_node_index = index_in_parent - 1;
        } else if (right.valid()) {
            merge_internal(parent, stack_index, node, index_in_parent, right, index_in_parent + 1);
            child_node = right.index();
            child_node_index = index_in_parent + 1;
        } else {
            PREQUEL_UNREACHABLE("Must have a left or a right neighbor.");
        }

        free_internal(child_node);
        node = std::move(parent);
        stack_index -= 1;
    }
}

inline void tree::steal_leaf_entry(const internal_node& parent, const leaf_node& leaf,
                                   u32 leaf_index, const leaf_node& neighbor, u32 neighbor_index) {
    const u32 parent_children = parent.get_child_count();
    const u32 leaf_size = leaf.get_size();
    const u32 neighbor_size = neighbor.get_size();

    PREQUEL_ASSERT(neighbor_index < parent.get_child_count(), "Neighbor index out of bounds.");
    PREQUEL_ASSERT(leaf_index != 0 || neighbor_index == 1,
                   "Invalid neighbor index for first child.");
    PREQUEL_ASSERT(leaf_index != parent_children - 1 || neighbor_index == parent_children - 2,
                   "Invalid neighbor index for last child.");
    PREQUEL_ASSERT(parent.get_child(leaf_index) == leaf.index(), "Leaf index wrong.");
    PREQUEL_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");
    PREQUEL_ASSERT(neighbor_size > 1, "At least one value must remain after stealing one.");

    if (leaf_index < parent_children - 1 && neighbor_index == leaf_index + 1) {
        // Move elements
        leaf.insert_nonfull(leaf_size, neighbor.get(0));
        neighbor.remove(0);

        // Update max key of this node in parent.
        key_buffer key;
        derive_key(leaf.get(leaf_size), key.data());
        parent.set_key(leaf_index, key.data());

        // Rewrite all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid() || c.m_leaf.index() != neighbor.index())
                continue;
            if (c.m_index == 0) {
                c.m_leaf = leaf;
                c.m_index = leaf_size;
                c.m_parents.back().index -= 1;
            } else {
                c.m_index -= 1;
            }
        }
    } else if (leaf_index > 0 && neighbor_index == leaf_index - 1) {
        // Move elements
        leaf.insert_nonfull(0, neighbor.get(neighbor_size - 1));
        neighbor.remove(neighbor_size - 1);

        // Update max key of the neighbor node in parent.
        key_buffer key;
        derive_key(neighbor.get(neighbor_size - 2), key.data());
        parent.set_key(leaf_index - 1, key.data());

        // Rewrite all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            if (c.m_leaf.index() == leaf.index()) {
                c.m_index += 1;
            } else if (c.m_leaf.index() == neighbor.index()) {
                if (c.m_index >= neighbor_size - 1) {
                    c.m_leaf = leaf;
                    c.m_index -= neighbor_size - 1;
                    c.m_parents.back().index += 1;
                }
            }
        }
    } else {
        PREQUEL_UNREACHABLE("Not a neighbor node.");
    }
}

// Note: stack_index is the index of the children (node and neighbor).
inline void
tree::steal_internal_entry(const internal_node& parent, u32 stack_index, const internal_node& node,
                           u32 node_index, const internal_node& neighbor, u32 neighbor_index) {
    const u32 parent_children = parent.get_child_count();
    const u32 node_children = node.get_child_count();
    const u32 neighbor_children = neighbor.get_child_count();

    PREQUEL_ASSERT(stack_index > 0, "Cannot steal from the root.");
    PREQUEL_ASSERT(neighbor_index < parent_children, "Neighbor index out of bounds.");
    PREQUEL_ASSERT(node_index != 0 || neighbor_index == 1,
                   "Invalid neighbor index for first child.");
    PREQUEL_ASSERT(node_index != parent_children - 1 || neighbor_index == parent_children - 2,
                   "Invalid neighbor index for last child.");
    PREQUEL_ASSERT(parent.get_child(node_index) == node.index(), "Node index wrong.");
    PREQUEL_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");
    PREQUEL_ASSERT(neighbor_children > neighbor.min_children(),
                   "Enough children must remain after stealing one.");

    if (node_index < parent_children - 1 && neighbor_index == node_index + 1) {
        // Taking from the right neighbor. We have to update our own key after this op.
        node.append_entry(parent.get_key(node_index), neighbor.get_child(0));
        parent.set_key(node_index, neighbor.get_key(0));
        neighbor.remove_child(0);

        // Update cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.m_parents[stack_index - 1];
            auto& entry = c.m_parents[stack_index];
            if (entry.node.index() != neighbor.index())
                continue;

            if (entry.index == 0) {
                entry.node = node;
                entry.index = node_children;
                parent_entry.index -= 1;
            } else {
                entry.index -= 1;
            }
        }
    } else if (node_index > 0 && neighbor_index == node_index - 1) {
        // Taking from the left neighbor. The appropriate key (the max) is stored
        // in the parent node and needs to be taken + replaced.
        node.prepend_entry(parent.get_key(neighbor_index),
                           neighbor.get_child(neighbor_children - 1));
        parent.set_key(neighbor_index, neighbor.get_key(neighbor_children - 2));
        neighbor.remove_child(neighbor_children - 1);

        // Update cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.m_parents[stack_index - 1];
            auto& entry = c.m_parents[stack_index];
            if (entry.node.index() == node.index()) {
                entry.index += 1;
            } else if (entry.node.index() == neighbor.index()) {
                if (entry.index >= neighbor_children - 1) {
                    entry.node = node;
                    entry.index -= neighbor_children - 1;
                    parent_entry.index += 1;
                }
            }
        }
    } else {
        PREQUEL_UNREACHABLE("Not a neighbor node.");
    }
}

inline void tree::merge_leaf(const internal_node& parent, const leaf_node& leaf, u32 leaf_index,
                             const leaf_node& neighbor, u32 neighbor_index) {
    const u32 parent_children = parent.get_child_count();
    const u32 leaf_size = leaf.get_size();
    const u32 neighbor_size = neighbor.get_size();

    PREQUEL_ASSERT(neighbor_index < parent.get_child_count(), "Neighbor index out of bounds.");
    PREQUEL_ASSERT(leaf_index != 0 || neighbor_index == 1,
                   "Invalid neighbor index for first child.");
    PREQUEL_ASSERT(leaf_index != parent_children - 1 || neighbor_index == parent_children - 2,
                   "Invalid neighbor index for last child.");
    PREQUEL_ASSERT(parent.get_child(leaf_index) == leaf.index(), "Leaf index wrong.");
    PREQUEL_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");

    if (leaf_index < parent_children - 1 && neighbor_index == leaf_index + 1) {
        // Merge with the node to the right.
        leaf.append_from_right(neighbor);
        if (rightmost() == neighbor.index())
            set_rightmost(leaf.index());

        // Update the key since the leaf's max value changed.
        if (neighbor_index != parent_children - 1) {
            key_buffer key;
            derive_key(leaf.get(leaf_size + neighbor_size - 1), key.data());
            parent.set_key(leaf_index, key.data());
        }

        for (auto& c : m_cursors) {
            if (c.invalid() || c.m_leaf.index() != neighbor.index())
                continue;

            c.m_leaf = leaf;
            c.m_index += leaf_size;
            c.m_parents.back().index -= 1;
        }
    } else if (leaf_index > 0 && neighbor_index == leaf_index - 1) {
        // Merge with the node to the left.
        leaf.prepend_from_left(neighbor);
        if (leftmost() == neighbor.index())
            set_leftmost(leaf.index());

        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            if (c.m_leaf.index() == leaf.index()) {
                c.m_index += neighbor_size;
            } else if (c.m_leaf.index() == neighbor.index()) {
                c.m_leaf = leaf;
                c.m_parents.back().index += 1;
            }
        }
    } else {
        PREQUEL_UNREACHABLE("Not a neighbor node.");
    }
}

// Note: stack_index is the index of the children (node and neighbor).
inline void
tree::merge_internal(const internal_node& parent, u32 stack_index, const internal_node& node,
                     u32 node_index, const internal_node& neighbor, u32 neighbor_index) {
    const u32 parent_children = parent.get_child_count();
    const u32 node_children = node.get_child_count();
    const u32 neighbor_children = neighbor.get_child_count();

    PREQUEL_ASSERT(stack_index > 0, "Cannot steal from the root.");
    PREQUEL_ASSERT(neighbor_index < parent_children, "Neighbor index out of bounds.");
    PREQUEL_ASSERT(node_index != 0 || neighbor_index == 1,
                   "Invalid neighbor index for first child.");
    PREQUEL_ASSERT(node_index != parent_children - 1 || neighbor_index == parent_children - 2,
                   "Invalid neighbor index for last child.");
    PREQUEL_ASSERT(parent.get_child(node_index) == node.index(), "Node index wrong.");
    PREQUEL_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");

    if (node_index < parent_children - 1 && neighbor_index == node_index + 1) {
        // Merge with the right neighbor.
        node.append_from_right(parent.get_key(node_index), neighbor);

        if (neighbor_index != parent_children - 1) {
            parent.set_key(node_index, parent.get_key(neighbor_index));
        }

        // Update all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.m_parents[stack_index - 1];
            auto& entry = c.m_parents[stack_index];
            if (entry.node.index() == neighbor.index()) {
                entry.node = node;
                entry.index += node_children;
                parent_entry.index -= 1;
            }
        }

    } else if (node_index > 0 && neighbor_index == node_index - 1) {
        // Merge with the left neighbor.
        node.prepend_from_left(parent.get_key(neighbor_index), neighbor);

        // Update all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.m_parents[stack_index - 1];
            auto& entry = c.m_parents[stack_index];
            if (entry.node.index() == neighbor.index()) {
                entry.node = node;
                parent_entry.index += 1;
            } else if (entry.node.index() == node.index()) {
                entry.index += neighbor_children;
            }
        }
    } else {
        PREQUEL_UNREACHABLE("Not a neighbor node.");
    }
}

inline void tree::clear() {
    if (empty())
        return;

    // Invalidate all cursors first.
    for (auto& c : m_cursors) {
        if (c.invalid())
            continue;

        c.reset_to_invalid(c.DELETED);
    }

    const block_index old_root = root();
    const u32 old_height = height();

    set_root({});
    set_leftmost({});
    set_rightmost({});
    set_height(0);
    set_size(0);
    set_internal_nodes(0);
    set_leaf_nodes(0);

    detail::fix visit = [&](auto& self, block_index index, u32 level) -> void {
        if (level > 0) {
            internal_node node = read_internal(index);

            level -= 1;
            const u32 child_count = node.get_child_count();
            for (u32 i = 0; i < child_count; ++i)
                self(node.get_child(i), level);
        }

        get_allocator().free(index, 1);
    };

    if (old_height >= 1) {
        visit(old_root, old_height - 1);
    } else {
        PREQUEL_UNREACHABLE("Invalid height for nonempty tree.");
    }
}

inline void tree::clear_subtree(block_index index, u32 level) {
    if (level > 0) {
        internal_node node = read_internal(index);

        level -= 1;
        const u32 child_count = node.get_child_count();
        for (u32 i = 0; i < child_count; ++i)
            clear_subtree(node.get_child(i), level);
        free_internal(index);
    } else {
        free_leaf(index);
    }
}

inline std::unique_ptr<loader> tree::bulk_load() {
    if (!empty())
        PREQUEL_THROW(bad_operation("Tree must be empty."));

    return std::make_unique<loader>(*this);
}

inline std::unique_ptr<cursor> tree::create_cursor(raw_btree::cursor_seek_t seek) {
    auto c = std::make_unique<cursor>(this);

    switch (seek) {
    case raw_btree::seek_none: break;
    case raw_btree::seek_min: c->move_min(); break;
    case raw_btree::seek_max: c->move_max(); break;
    default: PREQUEL_THROW(bad_argument("Invalid seek value."));
    }

    return c;
}

template<typename Func>
inline void tree::visit_nodes(Func&& fn) const {
    if (height() == 0)
        return;

    detail::fix recurse = [&](auto& self, u32 level, block_index parent,
                              block_index current) -> bool {
        if (level == 0)
            return fn(parent, level, read_leaf(current));

        internal_node node = read_internal(current);
        const u32 children = node.get_child_count();
        if (!fn(parent, level, node))
            return false;

        for (u32 i = 0; i < children; ++i) {
            if (!self(level - 1, node.index(), node.get_child(i)))
                return false;
        }
        return true;
    };
    recurse(height() - 1, block_index(), root());
}

inline void tree::dump(std::ostream& os) const {
    fmt::print(os,
               "Raw btree:\n"
               "  Value size: {}\n"
               "  Key size: {}\n"
               "  Internal node capacity: {}\n"
               "  Leaf node capacity: {}\n"
               "  Height: {}\n"
               "  Size: {}\n"
               "  Internal nodes: {}\n"
               "  Leaf nodes: {}\n",
               value_size(), key_size(), internal_node_max_children(), leaf_node_max_values(),
               height(), size(), internal_nodes(), leaf_nodes());

    if (!empty())
        os << "\n";

    struct visitor {
        std::ostream& os;
        visitor(std::ostream& os)
            : os(os) {}

        bool operator()(block_index parent, u32 level, const internal_node& node) const {
            const u32 child_count = node.get_child_count();
            PREQUEL_ASSERT(child_count > 0, "Invalid child count.");

            fmt::print(os,
                       "Internal node @{}:\n"
                       "  Parent: @{}\n"
                       "  Level: {}\n"
                       "  Children: {}\n",
                       node.index(), parent, level, child_count);

            for (u32 i = 0; i < child_count - 1; ++i) {
                fmt::print(os, "  {}: @{} (<= {})\n", i, node.get_child(i),
                           format_hex(node.get_key(i), node.key_size()));
            }
            fmt::print(os, "  {}: @{}\n", child_count - 1, node.get_child(child_count - 1));
            return true;
        }

        bool operator()(block_index parent, u32 level, const leaf_node& leaf) const {
            unused(level);

            const u32 size = leaf.get_size();
            fmt::print(os,
                       "Leaf node @{}:\n"
                       "  Parent: @{}\n"
                       "  Values: {}\n",
                       leaf.index(), parent, size);
            for (u32 i = 0; i < size; ++i) {
                fmt::print(os, "  {}: {}\n", i, format_hex(leaf.get(i), leaf.value_size()));
            }
            return true;
        }
    };
    visit_nodes(visitor(os));
}

inline void tree::visit(bool (*visit_fn)(const raw_btree::node_view& node, void* user_data),
                        void* user_data) const {
    if (!visit_fn)
        PREQUEL_THROW(bad_argument("Invalid visitation function."));

    struct node_view_impl : raw_btree::node_view {
        virtual bool is_leaf() const { return m_level == 0; }
        virtual bool is_internal() const { return !is_leaf(); }

        virtual u32 level() const { return m_level; }
        virtual block_index address() const { return m_address; }
        virtual block_index parent_address() const { return m_parent; }

        virtual u32 child_count() const { return check_internal().get_child_count(); }
        virtual u32 key_count() const { return check_internal().get_child_count() - 1; }

        virtual const byte* key(u32 index) const {
            const internal_node& node = check_internal();
            if (index >= node.get_child_count() - 1)
                PREQUEL_THROW(bad_argument("Key index out of bounds."));
            return node.get_key(index);
        }

        virtual block_index child(u32 index) const {
            const internal_node& node = check_internal();
            if (index >= node.get_child_count())
                PREQUEL_THROW(bad_argument("Child index out of bounds."));
            return node.get_child(index);
        }

        // For leaf nodes.
        virtual u32 value_count() const { return check_leaf().get_size(); }

        virtual const byte* value(u32 index) const {
            const leaf_node& node = check_leaf();
            if (index >= node.get_size())
                PREQUEL_THROW(bad_argument("Value index out of bounds."));
            return node.get(index);
        }

        const internal_node& check_internal() const {
            auto ptr = std::get_if<internal_node>(&m_node);
            if (!ptr)
                PREQUEL_THROW(bad_argument("Not an internal node."));
            return *ptr;
        }

        const leaf_node& check_leaf() const {
            auto ptr = std::get_if<leaf_node>(&m_node);
            if (!ptr)
                PREQUEL_THROW(bad_argument("Not a leaf node."));
            return *ptr;
        }

        u32 m_level = 0;
        block_index m_parent;
        block_index m_address;
        std::variant<leaf_node, internal_node> m_node;
    };

    struct visitor_t {
        node_view_impl view;
        bool (*visit_fn)(const raw_btree::node_view& node, void* user_data) = nullptr;
        void* user_data = nullptr;

        bool operator()(block_index parent, u32 level, const internal_node& internal) {
            view.m_level = level;
            view.m_parent = parent;
            view.m_address = internal.index();
            view.m_node = internal;
            return visit_fn(view, user_data);
        }

        bool operator()(block_index parent, u32 level, const leaf_node& leaf) {
            view.m_level = level;
            view.m_parent = parent;
            view.m_address = leaf.index();
            view.m_node = leaf;
            return visit_fn(view, user_data);
        }
    } visitor;
    visitor.visit_fn = visit_fn;
    visitor.user_data = user_data;
    visit_nodes(visitor);
}

inline void tree::validate() const {
#define PREQUEL_ERROR(...) PREQUEL_THROW(corruption_error(fmt::format("validate: " __VA_ARGS__)));

    struct context {
        u32 level = 0;
        block_index parent;
        const byte* lower_key = nullptr; // If present: Values or keys must be greater
        const byte* upper_key = nullptr; // If present: Value or keys must be greater or equal
    };

    struct checker {
        const class tree* tree;

        const u32 min_values = tree->m_leaf_capacity / 2;
        const u32 max_values = tree->m_leaf_capacity;
        const u32 min_children = tree->m_internal_max_children / 2;
        const u32 max_children = tree->m_internal_max_children;

        u64 seen_values = 0;
        u64 seen_leaf_nodes = 0;
        u64 seen_internal_nodes = 0;

        checker(const class tree* tree)
            : tree(tree) {}

        void check_key(const context& ctx, const byte* key) {
            if (ctx.lower_key && !tree->key_greater(key, ctx.lower_key))
                PREQUEL_ERROR("Key is not greater than the lower bound.");
            if (ctx.upper_key && tree->key_less(ctx.upper_key, key))
                PREQUEL_ERROR("Key is greater than the upper bound.");
        };

        void check_leaf(const context& ctx, const leaf_node& leaf) {
            if (!ctx.lower_key && tree->leftmost() != leaf.index()) {
                PREQUEL_ERROR("Only the leftmost leaf can have an unbounded lower key.");
            }
            if (tree->leftmost() == leaf.index() && seen_leaf_nodes != 0) {
                PREQUEL_ERROR("The leftmost leaf must be visited first.");
            }
            if (seen_leaf_nodes == tree->leaf_nodes() - 1 && tree->rightmost() != leaf.index()) {
                PREQUEL_ERROR("Expected the rightmost leaf at this index.");
            }

            const u32 size = leaf.get_size();
            if (size == 0) {
                PREQUEL_ERROR("Empty leaf.");
            }
            if (size < min_values && leaf.index() != tree->root()
                && leaf.index() != tree->leftmost() && leaf.index() != tree->rightmost()) {
                PREQUEL_ERROR("Leaf is underflowing.");
            }
            if (size > max_values) {
                PREQUEL_ERROR("Leaf is overflowing.");
            }

            for (u32 i = 0; i < size; ++i) {
                key_buffer key;
                tree->derive_key(leaf.get(i), key.data());
                check_key(ctx, key.data());

                if (i > 0) {
                    key_buffer prev;
                    tree->derive_key(leaf.get(i - 1), prev.data());
                    if (!tree->key_less(prev.data(), key.data())) {
                        PREQUEL_ERROR("Leaf entries are not sorted.");
                    }
                }
            }

            seen_leaf_nodes += 1;
            seen_values += size;
        };

        void check_internal(const context& ctx, const internal_node& node) {
            const u32 child_count = node.get_child_count();
            if (child_count < min_children && node.index() != tree->root()) {
                PREQUEL_ERROR("Internal node is underflowing.");
            }
            if (child_count < 2 && node.index() != tree->root()) {
                PREQUEL_ERROR("Root is too empty.");
            }
            if (child_count > max_children) {
                PREQUEL_ERROR("Internal node is overflowing.");
            }

            check_key(ctx, node.get_key(0));

            context child_ctx;
            child_ctx.parent = node.index();
            child_ctx.level = ctx.level - 1;
            child_ctx.lower_key = ctx.lower_key;
            child_ctx.upper_key = node.get_key(0);
            check_key(child_ctx, node.get_key(0));
            check(child_ctx, node.get_child(0));

            for (u32 i = 1; i < child_count - 1; ++i) {
                check_key(ctx, node.get_key(i));
                if (!tree->key_less(node.get_key(i - 1), node.get_key(i))) {
                    PREQUEL_ERROR("Internal node entries are not sorted.");
                }

                child_ctx.lower_key = node.get_key(i - 1);
                child_ctx.upper_key = node.get_key(i);
                check(child_ctx, node.get_child(i));
            }

            child_ctx.lower_key = node.get_key(child_count - 2);
            child_ctx.upper_key = ctx.upper_key;
            check(child_ctx, node.get_child(child_count - 1));

            seen_internal_nodes += 1;
        };

        void check(const context& ctx, block_index node_index) {
            if (ctx.level == 0) {
                check_leaf(ctx, tree->read_leaf(node_index));
            } else {
                check_internal(ctx, tree->read_internal(node_index));
            }
        }

        void run() {
            if (tree->height() != 0) {
                if (!tree->root()) {
                    PREQUEL_ERROR("Non-empty tree does not have a root.");
                }

                context ctx;
                ctx.level = tree->height() - 1;
                check(ctx, tree->root());
            }

            if (seen_values != tree->size()) {
                PREQUEL_ERROR("Value count does not match the tree's size.");
            }
            if (seen_leaf_nodes != tree->leaf_nodes()) {
                PREQUEL_ERROR("Leaf node count does not match the tree's state.");
            }
            if (seen_internal_nodes != tree->internal_nodes()) {
                PREQUEL_ERROR("internal node count does not match the tree's state.");
            }
        }
    };

    checker(this).run();

#undef PREQUEL_ERROR
}

inline void tree::free_leaf(block_index leaf) {
    PREQUEL_ASSERT(leaf_nodes() > 0, "Invalid state");
    get_allocator().free(leaf, 1);
    set_leaf_nodes(leaf_nodes() - 1);
}

inline void tree::free_internal(block_index internal) {
    PREQUEL_ASSERT(internal_nodes() > 0, "Invalid state");
    get_allocator().free(internal, 1);
    set_internal_nodes(internal_nodes() - 1);
}

inline leaf_node tree::create_leaf() {
    auto index = get_allocator().allocate(1);
    set_leaf_nodes(leaf_nodes() + 1);

    auto block = get_engine().overwrite_zero(index);
    auto node = leaf_node(std::move(block), value_size(), m_leaf_capacity);
    node.init();
    return node;
}

inline internal_node tree::create_internal() {
    auto index = get_allocator().allocate(1);
    set_internal_nodes(internal_nodes() + 1);

    auto block = get_engine().overwrite_zero(index);
    auto node = internal_node(std::move(block), key_size(), m_internal_max_children);
    node.init();
    return node;
}

inline leaf_node tree::as_leaf(block_handle handle) const {
    return leaf_node(std::move(handle), value_size(), m_leaf_capacity);
}

inline internal_node tree::as_internal(block_handle handle) const {
    return internal_node(std::move(handle), key_size(), m_internal_max_children);
}

inline leaf_node tree::read_leaf(block_index index) const {
    return as_leaf(get_engine().read(index));
}

inline internal_node tree::read_internal(block_index index) const {
    return as_internal(get_engine().read(index));
}

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_TREE_IPP
