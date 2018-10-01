#ifndef EXTPP_BTREE_LEAF_NODE_IPP
#define EXTPP_BTREE_LEAF_NODE_IPP

#include <extpp/btree/leaf_node.hpp>

namespace extpp::detail::btree_impl {

inline void leaf_node::insert_nonfull(u32 index, const byte* value) const {
    EXTPP_ASSERT(index < m_max_children, "Index out of bounds.");
    EXTPP_ASSERT(index <= get_size(), "Unexpected index (not in range).");

    u32 size = get_size();
    byte* data = m_handle.block().writable_data();
    std::memmove(data + offset_of_value(index + 1),
                 data + offset_of_value(index),
                 (size - index) * m_value_size);
    std::memmove(data + offset_of_value(index), value, m_value_size);
    set_size(size + 1);
}

inline void leaf_node::append_nonfull(const byte* values, u32 count) const {
    EXTPP_ASSERT(count > 0, "Useless call.");
    EXTPP_ASSERT(count <= m_max_children, "Count out of bounds.");
    EXTPP_ASSERT(get_size() <= m_max_children - count, "Insert range out of bounds.");

    const u32 old_size = get_size();
    byte* data = m_handle.block().writable_data();
    std::memmove(data + offset_of_value(old_size), values, m_value_size * count);
    set_size(old_size + count);
}

inline void leaf_node::insert_full(u32 index, const byte* value, u32 mid, const leaf_node& new_leaf) const {
    EXTPP_ASSERT(mid <= m_max_children, "Mid out of bounds.");
    EXTPP_ASSERT(m_value_size == new_leaf.m_value_size, "Value size missmatch.");
    EXTPP_ASSERT(m_max_children == new_leaf.m_max_children, "Capacity missmatch.");
    EXTPP_ASSERT(new_leaf.get_size() == 0, "New leaf must be empty.");
    EXTPP_ASSERT(get_size() == m_max_children, "Old leaf must be full.");

    byte* left = m_handle.block().writable_data() + offset_of_value(0);
    byte* right = new_leaf.block().writable_data() + offset_of_value(0);
    sequence_insert(m_value_size, left, right, m_max_children, mid, index, value);
    set_size(mid);
    new_leaf.set_size(m_max_children + 1 - mid);
}

inline void leaf_node::remove(u32 index) const {
    EXTPP_ASSERT(index < m_max_children, "Index out of bounds.");
    EXTPP_ASSERT(index < get_size(), "Unexpected index (not in range).");

    u32 size = get_size();
    byte* data = m_handle.block().writable_data();
    std::memmove(data + offset_of_value(index),
                 data + offset_of_value(index + 1),
                 (size - index - 1) * m_value_size);
    set_size(size - 1);
}

inline void leaf_node::append_from_right(const leaf_node& neighbor) const {
    EXTPP_ASSERT(get_size() + neighbor.get_size() <= m_max_children,  "Too many values.");
    EXTPP_ASSERT(value_size() == neighbor.value_size(), "Value size missmatch.");

    u32 size = get_size();
    u32 neighbor_size = neighbor.get_size();

    byte* data = m_handle.block().writable_data();
    const byte* neighbor_data = neighbor.m_handle.block().data();

    std::memmove(data + offset_of_value(size),
                 neighbor_data + offset_of_value(0),
                 neighbor_size * value_size());
    set_size(size + neighbor_size);
}

inline void leaf_node::prepend_from_left(const leaf_node& neighbor) const {
    EXTPP_ASSERT(get_size() + neighbor.get_size() <= m_max_children,  "Too many values.");
    EXTPP_ASSERT(value_size() == neighbor.value_size(), "Value size missmatch.");

    u32 size = get_size();
    u32 neighbor_size = neighbor.get_size();

    byte* data = m_handle.block().writable_data();
    const byte* neighbor_data = neighbor.m_handle.block().data();

    std::memmove(data + offset_of_value(neighbor_size),
                 data + offset_of_value(0),
                 size * value_size());
    std::memmove(data + offset_of_value(0),
                 neighbor_data + offset_of_value(0),
                 neighbor_size * value_size());
    set_size(size + neighbor_size);
}

inline void leaf_node::sequence_insert(u32 value_size, byte* left, byte* right, u32 count,
                                       u32 mid, u32 insert_index, const byte* value)
{
    EXTPP_ASSERT(mid > 0 && mid <= count, "index can't be used as mid");
    EXTPP_ASSERT(insert_index <= count, "index out of bounds");

    // Move values by index from source to dest.
    auto move = [value_size](const byte* src, u32 src_index, byte* dst, u32 dst_index, u32 n) {
        std::memmove(dst + dst_index * value_size, src + src_index * value_size, n * value_size);
    };

    if (insert_index < mid) {
        // Element ends up in the left node.
        move(left, mid - 1, right, 0, count - mid + 1);
        move(left, insert_index, left, insert_index + 1, mid - 1 - insert_index);
        move(value, 0, left, insert_index, 1);
    } else {
        // Put element in the right node.
        u32 right_insert_index = insert_index - mid;

        move(left, mid, right, 0, right_insert_index);
        move(value, 0, right, right_insert_index, 1);
        move(left, mid + right_insert_index, right, right_insert_index + 1, count - mid - right_insert_index);
    }
}

} // namespace extpp::detail::btree_impl

#endif // EXTPP_BTREE_LEAF_NODE_IPP
