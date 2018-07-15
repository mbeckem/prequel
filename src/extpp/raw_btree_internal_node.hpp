#ifndef EXTPP_RAW_BTREE_INTERNAL_NODE_HPP
#define EXTPP_RAW_BTREE_INTERNAL_NODE_HPP

#include <extpp/defs.hpp>
#include <extpp/block_index.hpp>
#include <extpp/handle.hpp>
#include <extpp/serialization.hpp>

#include <cstring>

namespace extpp {

// Node layout:
// - Header
// - Array of search keys (N - 1)
// - Array of child pointers (N)
//
// Keys are in sorted order. There are N child pointers and N - 1 keys.
// The subtree at child[i] contains values `<= key[i]`.
// The subree at child[N - 1] contains values that are greater than all the other keys.
class raw_btree_internal_node {
    // Size of a child pointer.
    static constexpr auto block_index_size = serialized_size<block_index>();

    struct header {
        u32 size = 0;   // Number of children in this node <= capacity.

        static constexpr auto get_binary_format() {
            return make_binary_format(&header::size);
        }
    };

public:
    raw_btree_internal_node() = default;

    raw_btree_internal_node(block_handle block, u32 key_size, u32 capacity)
        : m_handle(std::move(block), 0)
        , m_key_size(key_size)
        , m_capacity(capacity)
    {
        EXTPP_ASSERT(key_size > 0, "Invalid key size");
        EXTPP_ASSERT(capacity > 1, "Invalid capacity");
    }

    bool valid() const { return m_handle.valid(); }
    const block_handle& block() const { return m_handle.block(); }
    block_index index() const { return block().index(); }

    void init() { m_handle.set(header()); }

    u32 get_child_count() const { return m_handle.get<&header::size>(); }
    void set_child_count(u32 new_size) const {
        EXTPP_ASSERT(new_size <= m_capacity, "Invalid size");
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
    void insert_split_result(u32 index, const byte* split_key, block_index new_child) const {
        EXTPP_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");
        EXTPP_ASSERT(index >= 1 && index <= get_child_count(), "Index out of bounds");

        const u32 child_count = get_child_count();
        byte* const data = m_handle.block().writable_data();

        // Shift keys to the right, then update key[index - 1]
        byte* const key_begin = data + offset_of_key(index - 1);
        std::memmove(key_begin + m_key_size, key_begin, m_key_size * ((child_count - 1) - (index - 1)));
        std::memmove(key_begin, split_key, m_key_size);

        // Shift children to the right, then update children[index]
        byte* const child_begin = data + offset_of_child(index);
        std::memmove(child_begin + block_index_size, child_begin, block_index_size * (child_count - index));
        extpp::serialize(new_child, child_begin);

        set_child_count(child_count + 1);
    }

    // Insert a (key, value)-pair at the front.
    void prepend_entry(const byte* key, block_index child) const {
        EXTPP_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");

        const u32 child_count = get_child_count();
        byte* const data = m_handle.block().writable_data();

        // Shift all keys and children to the right.
        std::memmove(data + offset_of_key(1), data + offset_of_key(0), m_key_size * (child_count - 1));
        std::memmove(data + offset_of_child(1), data + offset_of_child(0), block_index_size * child_count);
        std::memmove(data + offset_of_key(0), key, m_key_size);
        extpp::serialize(child, data + offset_of_child(0));

        set_child_count(child_count + 1);
    }

    // Insert a (key, value)-pair at the back.
    void append_entry(const byte* key, block_index child) const {
        EXTPP_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");

        const u32 child_count = get_child_count();
        byte* const data = m_handle.block().writable_data();

        std::memmove(data + offset_of_key(child_count - 1), key, m_key_size);
        extpp::serialize(child, data + offset_of_child(child_count));

        set_child_count(child_count + 1);
    }

    // Sets the content (child_count - 1 keys and child_count children) of this node.
    // Used during bulk loading.
    void set_entries(const byte* keys, const block_index* children, u32 child_count) {
        EXTPP_ASSERT(get_child_count() == 0, "Can only be used on empty nodes.");
        EXTPP_ASSERT(child_count <= max_children(), "Too many children.");
        EXTPP_ASSERT(child_count >= 2, "Invalid number of children.");

        byte* data = m_handle.block().writable_data();

        // Insert the keys and the child pointers.
        std::memmove(data + offset_of_key(0), keys, m_key_size * (child_count - 1));
        {
            byte* child_cursor = data + offset_of_child(0);
            for (u32 i = 0; i < child_count; ++i) {
                child_cursor = extpp::serialize(children[i], child_cursor);
            }
        }
        set_child_count(child_count);
    }

    // Removes the child at the given index (and its key, if there is one).
    // All children and keys with a higher index move one to the left.
    void remove_child(u32 index) const {
        EXTPP_ASSERT(index < get_child_count(), "Child index out of bounds.");

        const u32 child_count = get_child_count();
        byte* const data = m_handle.block().writable_data();

        std::memmove(data + offset_of_child(index),
                     data + offset_of_child(index + 1),
                     block_index_size * (child_count - index - 1));
        if (index != child_count - 1) {
            std::memmove(data + offset_of_key(index),
                         data + offset_of_key(index + 1),
                         m_key_size * (child_count - index - 2));
        }
        set_child_count(child_count - 1);
    }

    // Merge with the right neighbor. The split key is the key that currently
    // represents this node in the parent.
    void append_from_right(const byte* split_key, const raw_btree_internal_node& neighbor) const {
        EXTPP_ASSERT(get_child_count() + neighbor.get_child_count() <= max_children(),
                     "Too many children.");
        EXTPP_ASSERT(key_size() == neighbor.key_size(), "Key size missmatch.");

        const u32 child_count = get_child_count();
        const u32 neighbor_child_count = neighbor.get_child_count();

        byte* data = m_handle.block().writable_data();
        const byte* neighbor_data = neighbor.m_handle.block().data();

        std::memmove(data + offset_of_key(child_count - 1), split_key, key_size());
        std::memmove(data + offset_of_key(child_count),
                     neighbor_data + offset_of_key(0),
                     (neighbor_child_count - 1) * key_size());
        std::memmove(data + offset_of_child(child_count),
                     neighbor_data + offset_of_child(0),
                     neighbor_child_count * block_index_size);

        set_child_count(child_count + neighbor_child_count);
    }

    // Merge with the left neighbor. The split key is the key that currently
    // represents the neighbor in the parent.
    void prepend_from_left(const byte* split_key, const raw_btree_internal_node& neighbor) const {
        EXTPP_ASSERT(get_child_count() + neighbor.get_child_count() <= max_children(),
                     "Too many children.");
        EXTPP_ASSERT(key_size() == neighbor.key_size(), "Key size missmatch.");

        const u32 child_count = get_child_count();
        const u32 neighbor_child_count = neighbor.get_child_count();

        byte* data = m_handle.block().writable_data();
        const byte* neighbor_data = neighbor.m_handle.block().data();

        // Shift existing keys and children to the right.
        std::memmove(data + offset_of_key(neighbor_child_count),
                     data + offset_of_key(0),
                     (child_count - 1) * key_size());
        std::memmove(data + offset_of_child(neighbor_child_count),
                     data + offset_of_child(0),
                     child_count * block_index_size);

        // Insert keys and children from the left node.
        std::memmove(data + offset_of_key(0),
                     neighbor_data + offset_of_key(0),
                     (neighbor_child_count - 1) * key_size());
        std::memmove(data + offset_of_key(neighbor_child_count - 1), split_key, key_size());
        std::memmove(data + offset_of_child(0),
                     neighbor_data + offset_of_child(0),
                     neighbor_child_count * block_index_size);

        set_child_count(child_count + neighbor_child_count);
    }

    /// Moves half of this node's keys and children into the right node.
    /// Sets the split key to the key that should go in the middle.
    void split(const raw_btree_internal_node& right, byte* split_key) const {
        EXTPP_ASSERT(get_child_count() == max_children(), "Node must be full.");
        EXTPP_ASSERT(right.get_child_count() == 0, "Right node must be empty.");
        EXTPP_ASSERT(key_size() == right.key_size(), "Key size missmatch.");
        EXTPP_ASSERT(max_children() == right.max_children(), "Capacity missmatch.");

        const u32 child_count = get_child_count();
        const u32 left_count = (child_count + 1) / 2;
        const u32 right_count = child_count - left_count;

        byte* right_data = right.block().writable_data();
        const byte* left_data = block().data();

        std::memmove(right_data + offset_of_key(0),
                     left_data + offset_of_key(left_count),
                     key_size() * (right_count - 1));
        std::memmove(right_data + offset_of_child(0),
                     left_data + offset_of_child(left_count),
                     block_index_size * right_count);

        // Rescue split key.
        std::memmove(split_key, left_data + offset_of_key(left_count - 1), key_size());

        set_child_count(left_count);
        right.set_child_count(right_count);
    }

    u32 min_children() const { return compute_min_children(max_children()); }
    u32 max_children() const { return m_capacity; }

    u32 max_keys() const { return m_capacity - 1; }
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

private:
    u32 offset_of_child(u32 index) const {
        EXTPP_ASSERT(index < max_children(), "Child index out of bounds");
        return serialized_size<header>()
                + (max_keys() * m_key_size)
                + (index * block_index_size);
    }

    u32 offset_of_key(u32 index) const {
        EXTPP_ASSERT(index < max_keys(), "Key index ouf bounds");
        return serialized_size<header>() + m_key_size * index;
    }

private:
    handle<header> m_handle;
    u32 m_key_size = 0;         // Size of a search key
    u32 m_capacity = 0;         // Number of CHILDREN per node (there can be `capacity - 1` keys).
};

} // namespace extpp

#endif // EXTPP_RAW_BTREE_INTERNAL_NODE_HPP
