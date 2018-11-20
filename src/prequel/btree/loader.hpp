#ifndef PREQUEL_BTREE_LOADER_HPP
#define PREQUEL_BTREE_LOADER_HPP

#include <prequel/block_index.hpp>
#include <prequel/btree/base.hpp>
#include <prequel/btree/internal_node.hpp>
#include <prequel/btree/leaf_node.hpp>

#include <memory>
#include <vector>

namespace prequel::detail::btree_impl {

// Future: Implement bulk loading for non-emtpy trees (i.e. all keys must be > max).
class loader {
public:
    loader(btree_impl::tree& tree);

    loader(const loader&) = delete;
    loader& operator=(const loader&) = delete;

    void insert(const byte* values, size_t count);

    void finish();

    void discard();

private:
    // Every node is represented by its max key and its pointer.
    // Enough memory for keys and indices for (children + min_children) children.
    // This scheme ensures that we never emit internal nodes that are too empty.
    // Note that we can emit *leaf*-nodes that are too empty because the tree
    // already has a special case for them.
    struct proto_internal_node {
        std::vector<byte> keys;
        std::vector<block_index> children;
        u32 size = 0;
        u32 capacity = 0;
    };

    proto_internal_node make_internal_node() {
        proto_internal_node node;
        node.capacity = m_internal_max_children + m_internal_min_children;
        node.keys.resize(node.capacity * m_key_size);
        node.children.resize(node.capacity);
        return node;
    }

    void insert_child(size_t index, const byte* key, block_index child);
    void flush_internal(size_t index, proto_internal_node& node, u32 count);

    void insert_child_nonfull(proto_internal_node& node, const byte* key, block_index child);
    void flush_leaf();

    enum state_t { STATE_OK, STATE_ERROR, STATE_FINALIZED };

private:
    // Constants
    btree_impl::tree& m_tree;
    const u32 m_internal_min_children;
    const u32 m_internal_max_children;
    const u32 m_leaf_max_values;
    const u32 m_value_size;
    const u32 m_key_size;
    state_t m_state = STATE_OK;

    // Track these values while we build the tree.
    block_index m_leftmost_leaf;  // First leaf created.
    block_index m_rightmost_leaf; // Last leaf created.
    u64 m_size = 0;               // Total number of values inserted.

    // One proto node for every level of internal nodes.
    // The last entry is the root. Unique pointer for stable addresses.
    std::vector<std::unique_ptr<proto_internal_node>> m_parents;
    leaf_node m_leaf;
};

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_LOADER_HPP
