#ifndef EXTPP_BTREE_INTERNAL_NODE_HPP
#define EXTPP_BTREE_INTERNAL_NODE_HPP

#include <extpp/defs.hpp>
#include <extpp/block_index.hpp>
#include <extpp/handle.hpp>
#include <extpp/serialization.hpp>

#include <cstring>

namespace extpp::detail::btree_impl {

// Node layout:
// - Header
// - Array of search keys (N - 1)
// - Array of child pointers (N)
//
// Keys are in sorted order. There are N child pointers and N - 1 keys.
// The subtree at child[i] contains values `<= key[i]`.
// The subree at child[N - 1] contains values that are greater than all the other keys.
class internal_node {
    // Size of a child pointer.
    static constexpr auto block_index_size = serialized_size<block_index>();

    struct header {
        u32 size = 0;   // Number of children in this node <= capacity.

        static constexpr auto get_binary_format() {
            return make_binary_format(&header::size);
        }
    };

public:
    internal_node() = default;

    internal_node(block_handle block, u32 key_size, u32 max_children)
        : m_handle(std::move(block), 0)
        , m_key_size(key_size)
        , m_max_children(max_children)
    {
        EXTPP_ASSERT(key_size > 0, "Invalid key size");
        EXTPP_ASSERT(max_children > 1, "Invalid capacity");
        EXTPP_ASSERT(compute_size(max_children, key_size) <= m_handle.block().block_size(),
                     "Node is too large.");
    }

    bool valid() const { return m_handle.valid(); }
    const block_handle& block() const { return m_handle.block(); }
    block_index index() const { return block().index(); }

    void init() { m_handle.set(header()); }

    u32 get_child_count() const { return m_handle.get<&header::size>(); }
    void set_child_count(u32 new_size) const {
        EXTPP_ASSERT(new_size <= m_max_children, "Invalid size");
        m_handle.set<&header::size>(new_size);
    }

    void set_key(u32 index, const byte* key) const {
        m_handle.block().write(offset_of_key(index), key, m_key_size);
    }

    const byte* get_key(u32 index) const {
        return m_handle.block().data() + offset_of_key(index);
    }

    void set_child(u32 index, block_index child) const {
        m_handle.block().set(offset_of_child(index), child);
    }

    block_index get_child(u32 index) const {
        return m_handle.block().get<block_index>(offset_of_child(index));
    }

    // Pre: 1 <= index <= get_child_count
    // Post: keys[index - 1] == split_key, children[index] == new_child.
    // Other keys and children will be shifted to the right.
    void insert_split_result(u32 index, const byte* split_key, block_index new_child) const;

    // Insert a (key, value)-pair at the front.
    void prepend_entry(const byte* key, block_index child) const;

    // Insert a (key, value)-pair at the back.
    void append_entry(const byte* key, block_index child) const;

    // Sets the content (child_count - 1 keys and child_count children) of this node.
    // Used during bulk loading.
    void set_entries(const byte* keys, const block_index* children, u32 child_count);

    // Removes the child at the given index (and its key, if there is one).
    // All children and keys with a higher index move one to the left.
    void remove_child(u32 index) const;

    // Merge with the right neighbor. The split key is the key that currently
    // represents this node in the parent.
    void append_from_right(const byte* split_key, const internal_node& neighbor) const;

    // Merge with the left neighbor. The split key is the key that currently
    // represents the neighbor in the parent.
    void prepend_from_left(const byte* split_key, const internal_node& neighbor) const;

    /// Moves half of this node's keys and children into the right node.
    /// Sets the split key to the key that should go in the middle.
    void split(const internal_node& right, byte* split_key) const;

    u32 min_children() const { return compute_min_children(max_children()); }
    u32 max_children() const { return m_max_children; }

    u32 max_keys() const { return m_max_children - 1; }
    u32 key_size() const { return m_key_size; }

public:
    static u32 compute_max_children(u32 block_size, u32 key_size) {
        u32 hdr_size = serialized_size<header>();
        u32 ptr_size = block_index_size;
        if (block_size < hdr_size)
            return 0;

        return (block_size - hdr_size + key_size) / (key_size + ptr_size);
    }

    static u32 compute_min_children(u32 max_children) {
        return max_children / 2;
    }

    static u32 compute_size(u32 max_children, u32 key_size) {
        EXTPP_ASSERT(max_children > 1, "Invalid node capacity.");

        u32 hdr_size = serialized_size<header>();
        u32 ptr_size = block_index_size;
        return hdr_size + (max_children - 1) * key_size + max_children * ptr_size;
    }

private:
    u32 offset_of_child(u32 index) const {
        EXTPP_ASSERT(index <= max_children(), "Child index out of bounds");
        return serialized_size<header>()
                + (max_keys() * m_key_size)
                + (index * block_index_size);
    }

    u32 offset_of_key(u32 index) const {
        EXTPP_ASSERT(index <= max_keys(), "Key index ouf bounds");
        return serialized_size<header>() + m_key_size * index;
    }

private:
    handle<header> m_handle;
    u32 m_key_size = 0;         // Size of a search key
    u32 m_max_children = 0;     // Number of CHILDREN per node (there can be `capacity - 1` keys).
};

} // namespace extpp::detail::btree_impl

#endif // EXTPP_BTREE_INTERNAL_NODE_HPP
