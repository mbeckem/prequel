#ifndef PREQUEL_BTREE_INTERNAL_NODE_IPP
#define PREQUEL_BTREE_INTERNAL_NODE_IPP

#include "internal_node.hpp"

namespace prequel::detail::btree_impl {

void internal_node::insert_split_result(u32 index, const byte* split_key,
                                        block_index new_child) const {
    PREQUEL_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");
    PREQUEL_ASSERT(index >= 1 && index <= get_child_count(), "Index out of bounds");

    const u32 child_count = get_child_count();
    byte* const data = m_handle.block().writable_data();

    // Shift keys to the right, then update key[index - 1]
    byte* const key_begin = data + offset_of_key(index - 1);
    std::memmove(key_begin + m_key_size, key_begin, m_key_size * ((child_count - 1) - (index - 1)));
    std::memmove(key_begin, split_key, m_key_size);

    // Shift children to the right, then update children[index]
    byte* const child_begin = data + offset_of_child(index);
    std::memmove(child_begin + block_index_size, child_begin,
                 block_index_size * (child_count - index));
    prequel::serialize(new_child, child_begin);

    set_child_count(child_count + 1);
}

void internal_node::prepend_entry(const byte* key, block_index child) const {
    PREQUEL_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");

    const u32 child_count = get_child_count();
    byte* const data = m_handle.block().writable_data();

    // Shift all keys and children to the right.
    std::memmove(data + offset_of_key(1), data + offset_of_key(0), m_key_size * (child_count - 1));
    std::memmove(data + offset_of_child(1), data + offset_of_child(0),
                 block_index_size * child_count);
    std::memmove(data + offset_of_key(0), key, m_key_size);
    prequel::serialize(child, data + offset_of_child(0));

    set_child_count(child_count + 1);
}

void internal_node::append_entry(const byte* key, block_index child) const {
    PREQUEL_ASSERT(get_child_count() < max_children(), "Inserting into a full node.");

    const u32 child_count = get_child_count();
    byte* const data = m_handle.block().writable_data();

    std::memmove(data + offset_of_key(child_count - 1), key, m_key_size);
    prequel::serialize(child, data + offset_of_child(child_count));

    set_child_count(child_count + 1);
}

void internal_node::set_entries(const byte* keys, const block_index* children, u32 child_count) {
    PREQUEL_ASSERT(get_child_count() == 0, "Can only be used on empty nodes.");
    PREQUEL_ASSERT(child_count <= max_children(), "Too many children.");
    PREQUEL_ASSERT(child_count >= 2, "Invalid number of children.");

    byte* data = m_handle.block().writable_data();

    // Insert the keys and the child pointers.
    std::memmove(data + offset_of_key(0), keys, m_key_size * (child_count - 1));
    {
        byte* child_cursor = data + offset_of_child(0);
        for (u32 i = 0; i < child_count; ++i) {
            prequel::serialize(children[i], child_cursor);
            child_cursor += serialized_size<block_index>();
        }
    }
    set_child_count(child_count);
}

void internal_node::remove_child(u32 index) const {
    PREQUEL_ASSERT(index < get_child_count(), "Child index out of bounds.");

    const u32 child_count = get_child_count();
    byte* const data = m_handle.block().writable_data();

    std::memmove(data + offset_of_child(index), data + offset_of_child(index + 1),
                 block_index_size * (child_count - index - 1));
    if (index != child_count - 1) {
        std::memmove(data + offset_of_key(index), data + offset_of_key(index + 1),
                     m_key_size * (child_count - index - 2));
    }
    set_child_count(child_count - 1);
}

void internal_node::append_from_right(const byte* split_key, const internal_node& neighbor) const {
    PREQUEL_ASSERT(get_child_count() + neighbor.get_child_count() <= max_children(),
                   "Too many children.");
    PREQUEL_ASSERT(key_size() == neighbor.key_size(), "Key size missmatch.");

    const u32 child_count = get_child_count();
    const u32 neighbor_child_count = neighbor.get_child_count();

    byte* data = m_handle.block().writable_data();
    const byte* neighbor_data = neighbor.m_handle.block().data();

    std::memmove(data + offset_of_key(child_count - 1), split_key, key_size());
    std::memmove(data + offset_of_key(child_count), neighbor_data + offset_of_key(0),
                 (neighbor_child_count - 1) * key_size());
    std::memmove(data + offset_of_child(child_count), neighbor_data + offset_of_child(0),
                 neighbor_child_count * block_index_size);

    set_child_count(child_count + neighbor_child_count);
}

void internal_node::prepend_from_left(const byte* split_key, const internal_node& neighbor) const {
    PREQUEL_ASSERT(get_child_count() + neighbor.get_child_count() <= max_children(),
                   "Too many children.");
    PREQUEL_ASSERT(key_size() == neighbor.key_size(), "Key size missmatch.");

    const u32 child_count = get_child_count();
    const u32 neighbor_child_count = neighbor.get_child_count();

    byte* data = m_handle.block().writable_data();
    const byte* neighbor_data = neighbor.m_handle.block().data();

    // Shift existing keys and children to the right.
    std::memmove(data + offset_of_key(neighbor_child_count), data + offset_of_key(0),
                 (child_count - 1) * key_size());
    std::memmove(data + offset_of_child(neighbor_child_count), data + offset_of_child(0),
                 child_count * block_index_size);

    // Insert keys and children from the left node.
    std::memmove(data + offset_of_key(0), neighbor_data + offset_of_key(0),
                 (neighbor_child_count - 1) * key_size());
    std::memmove(data + offset_of_key(neighbor_child_count - 1), split_key, key_size());
    std::memmove(data + offset_of_child(0), neighbor_data + offset_of_child(0),
                 neighbor_child_count * block_index_size);

    set_child_count(child_count + neighbor_child_count);
}

void internal_node::split(const internal_node& right, byte* split_key) const {
    PREQUEL_ASSERT(get_child_count() == max_children(), "Node must be full.");
    PREQUEL_ASSERT(right.get_child_count() == 0, "Right node must be empty.");
    PREQUEL_ASSERT(key_size() == right.key_size(), "Key size missmatch.");
    PREQUEL_ASSERT(max_children() == right.max_children(), "Capacity missmatch.");

    const u32 child_count = get_child_count();
    const u32 left_count = (child_count + 1) / 2;
    const u32 right_count = child_count - left_count;

    byte* right_data = right.block().writable_data();
    const byte* left_data = block().data();

    std::memmove(right_data + offset_of_key(0), left_data + offset_of_key(left_count),
                 key_size() * (right_count - 1));
    std::memmove(right_data + offset_of_child(0), left_data + offset_of_child(left_count),
                 block_index_size * right_count);

    // Rescue split key.
    std::memmove(split_key, left_data + offset_of_key(left_count - 1), key_size());

    set_child_count(left_count);
    right.set_child_count(right_count);
}

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_INTERNAL_NODE_IPP
