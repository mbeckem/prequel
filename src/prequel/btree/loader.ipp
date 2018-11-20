#ifndef PREQUEL_BTREE_LOADER_IPP
#define PREQUEL_BTREE_LOADER_IPP

#include <prequel/btree/loader.hpp>

#include <prequel/btree/tree.hpp>
#include <prequel/detail/deferred.hpp>
#include <prequel/exception.hpp>

namespace prequel::detail::btree_impl {

inline loader::loader(btree_impl::tree& tree)
    : m_tree(tree)
    , m_internal_min_children(m_tree.internal_node_min_chlidren())
    , m_internal_max_children(m_tree.internal_node_max_children())
    , m_leaf_max_values(m_tree.leaf_node_max_values())
    , m_value_size(m_tree.value_size())
    , m_key_size(m_tree.key_size()) {}

inline void loader::insert(const byte* values, size_t count) {
    if (count == 0)
        return;
    if (!values)
        PREQUEL_THROW(bad_argument("Values are null."));
    if (m_state == STATE_ERROR)
        PREQUEL_THROW(bad_operation("A previous operation on this loader failed."));
    if (m_state == STATE_FINALIZED)
        PREQUEL_THROW(bad_operation("This loader was already finalized."));

    detail::deferred guard = [&] { m_state = STATE_ERROR; };

    while (count > 0) {
        if (!m_leaf.valid()) {
            m_leaf = m_tree.create_leaf();
        }

        u32 leaf_size = m_leaf.get_size();
        if (leaf_size == m_leaf_max_values) {
            flush_leaf();
            m_leaf = m_tree.create_leaf();
            leaf_size = 0;
        }

        u32 take = m_leaf_max_values - leaf_size;
        if (take > count)
            take = count;
        PREQUEL_ASSERT(take > 0, "Leaf must not be full.");

        m_leaf.append_nonfull(values, take);
        m_size += take;
        count -= take;
    }

    guard.disable();
}

inline void loader::finish() {
    if (!m_tree.empty())
        PREQUEL_THROW(bad_operation("The tree must be empty."));

    if (m_size == 0)
        return; // Nothing to do, the tree remains empty.

    if (m_leaf.valid()) {
        PREQUEL_ASSERT(m_leaf.get_size() > 0, "Leaves must not be empty.");
        flush_leaf();
        m_leaf = leaf_node();
    }

    // Loop over all internal nodes and flush them to the next level.
    // The parents vector will grow as needed because of the insert_child(index + 1) calls.
    // After this loop terminates, the highest level of the tree contains exactly one child, the root.
    for (size_t index = 0; index < m_parents.size(); ++index) {
        proto_internal_node& node = *m_parents[index];

        if (index == m_parents.size() - 1) {
            if (node.size == 1) {
                break; // Done, this child is our root.
            }
            // Continue with flush, make the tree grow in height.
        } else {
            // Not the last entry, this means that there must be enough entries for one node.
            PREQUEL_ASSERT(node.size >= m_internal_min_children,
                           "Not enough entries for one internal node.");
        }

        if (node.size > m_internal_max_children) {
            flush_internal(index, node, (node.size + 1) / 2);
        }
        flush_internal(index, node, node.size);
    }

    PREQUEL_ASSERT(m_parents.size() > 0 && m_parents.back()->size == 1,
                   "The highest level must contain one child.");

    m_tree.set_height(m_parents.size());
    m_tree.set_root(m_parents.back()->children[0]);
    m_tree.set_size(m_size);
    m_tree.set_leftmost(m_leftmost_leaf);
    m_tree.set_rightmost(m_rightmost_leaf);
    m_state = STATE_FINALIZED;
}

inline void loader::discard() {
    if (m_state == STATE_OK)
        m_state = STATE_FINALIZED;

    if (m_leaf.valid()) {
        m_tree.clear_subtree(m_leaf.index(), 0);
        m_leaf = leaf_node();
    }

    // The level of the child entries in the following nodes.
    u32 level = 0;
    for (auto& nodeptr : m_parents) {
        proto_internal_node& node = *nodeptr;
        for (size_t child = 0; child < node.size; ++child) {
            m_tree.clear_subtree(node.children[child], level);
        }
        node.size = 0;
        ++level;
    }
}

// Note: Invalidates references to nodes on the parent stack.
inline void loader::flush_leaf() {
    PREQUEL_ASSERT(m_leaf.valid(), "Leaf must be valid.");
    PREQUEL_ASSERT(m_leaf.get_size() > 0, "Leaf must not be empty.");

    // Insert the leaf into its (proto-) parent. If that parent becomes full,
    // emit a new node and register that one it's parent etc.
    key_buffer child_key;
    m_tree.derive_key(m_leaf.get(m_leaf.get_size() - 1), child_key.data());
    insert_child(0, child_key.data(), m_leaf.index());

    if (!m_leftmost_leaf) {
        m_leftmost_leaf = m_leaf.index();
    }
    m_rightmost_leaf = m_leaf.index();
    m_leaf = leaf_node();
}

// Note: Invalidates references to nodes on the parent stack.
inline void loader::insert_child(size_t index, const byte* key, block_index child) {
    PREQUEL_ASSERT(index <= m_parents.size(), "Invalid parent index.");

    if (index == m_parents.size()) {
        m_parents.push_back(std::make_unique<proto_internal_node>(make_internal_node()));
    }

    proto_internal_node& node = *m_parents[index];
    if (node.size == node.capacity) {
        flush_internal(index, node, m_internal_max_children);
    }
    insert_child_nonfull(node, key, child);
}

// Flush count entries from the node to the next level.
// Note: Invalidates references to nodes on the parent stack.
inline void loader::flush_internal(size_t index, proto_internal_node& node, u32 count) {
    PREQUEL_ASSERT(index < m_parents.size(), "Invalid node index.");
    PREQUEL_ASSERT(m_parents[index].get() == &node, "Node address mismatch.");
    PREQUEL_ASSERT(count <= node.size, "Cannot flush that many elements.");
    PREQUEL_ASSERT(count <= m_internal_max_children, "Too many elements for a tree node.");

    internal_node tree_node = m_tree.create_internal();
    detail::deferred cleanup = [&] { m_tree.free_internal(tree_node.index()); };

    // Copy first `count` entries into the real node, then forward max key and node index
    // to the next level.
    tree_node.set_entries(node.keys.data(), node.children.data(), count);
    insert_child(index + 1, node.keys.data() + m_key_size * (count - 1), tree_node.index());
    cleanup.disable();

    // Shift `count` values to the left.
    std::copy_n(node.children.begin() + count, node.size - count, node.children.begin());
    std::memmove(node.keys.data(), node.keys.data() + count * m_key_size,
                 (node.size - count) * m_key_size);
    node.size = node.size - count;
}

inline void
loader::insert_child_nonfull(proto_internal_node& node, const byte* key, block_index child) {
    PREQUEL_ASSERT(node.size < node.capacity, "Node is full.");

    std::memcpy(node.keys.data() + node.size * m_key_size, key, m_key_size);
    node.children[node.size] = child;
    node.size += 1;
}

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_LOADER_IPP
