#ifndef PREQUEL_BTREE_LEAF_NODE_HPP
#define PREQUEL_BTREE_LEAF_NODE_HPP

#include <prequel/defs.hpp>
#include <prequel/handle.hpp>
#include <prequel/serialization.hpp>

#include <cstring>

namespace prequel::detail::btree_impl {

// Node layout:
// - Header
// - Array of values (N)
//
// Values are ordered by their key.
class leaf_node {
private:
    // Note: no next/prev pointers, no type tag, no depth info.
    struct header {
        u32 size = 0; // Number of values in this node <= capacity.

        static constexpr auto get_binary_format() { return make_binary_format(&header::size); }
    };

public:
    leaf_node() = default;

    leaf_node(block_handle block, u32 value_size, u32 max_children)
        : m_handle(std::move(block), 0)
        , m_value_size(value_size)
        , m_max_children(max_children) {}

    bool valid() const { return m_handle.valid(); }
    const block_handle& block() const { return m_handle.block(); }
    block_index index() const { return block().index(); }

    void init() { m_handle.set(header()); }

    u32 get_size() const { return m_handle.get<&header::size>(); }
    void set_size(u32 new_size) const {
        PREQUEL_ASSERT(new_size <= m_max_children, "Invalid size");
        m_handle.set<&header::size>(new_size);
    }

    u32 min_size() const { return m_max_children / 2; }
    u32 max_size() const { return m_max_children; }
    u32 value_size() const { return m_value_size; }

    void set(u32 index, const byte* value) const {
        PREQUEL_ASSERT(index < m_max_children, "Index out of bounds.");
        m_handle.block().write(offset_of_value(index), value, m_value_size);
    }

    const byte* get(u32 index) const {
        PREQUEL_ASSERT(index < m_max_children, "Index out of bounds.");
        return m_handle.block().data() + offset_of_value(index);
    }

    // Insert the new value at the given index and shift values to the right.
    void insert_nonfull(u32 index, const byte* value) const;

    // Insert a range of values at the end. For bulk loading.
    void append_nonfull(const byte* values, u32 count) const;

    // Perform a node split and insert the new value at the appropriate position.
    // `mid` is the size of *this, after the split (other values end up in new_leaf).
    // If index < mid, then the new value is in the left node, at the given index.
    // Otherwise the new value is in new_leaf, at `index - mid`.
    void insert_full(u32 index, const byte* value, u32 mid, const leaf_node& new_leaf) const;

    // Removes the value at the given index and shifts all values after it to the left.
    void remove(u32 index) const;

    // Append all values from the right neighbor.
    void append_from_right(const leaf_node& neighbor) const;

    // Prepend all values from the left neighbor.
    void prepend_from_left(const leaf_node& neighbor) const;

public:
    static u32 capacity(u32 block_size, u32 value_size) {
        u32 hdr = serialized_size<header>();
        if (block_size < hdr)
            return 0;
        return (block_size - hdr) / value_size;
    }

private:
    u32 offset_of_value(u32 index) const {
        return serialized_size<header>() + m_value_size * index;
    }

    /// Insert a value into a sequence and perform a split at the same time.
    /// Values exist in `left`, and `right` is treated as empty.
    /// After the insertion, exactly `mid` entries will remain in `left` and the remaining
    /// entries will have been copied over into `right`.
    ///
    /// \param value_size       The size (in bytes) of a single value.
    /// \param left             The left sequence.
    /// \param right            The right sequence.
    /// \param count            The current size of the left sequence, without the new element.
    /// \param mid              The target size of the left sequence, after the split.
    /// \param insert_index     The target insertion index of `value` in the left sequence.
    /// \param value            The value to insert.
    ///
    /// \pre `0 <= insert_index <= count`.
    /// \pre `mid > 0 && mid <= count`.
    ///
    /// \post If `insert_index < mid`, then the new value will be stored in the left sequence at index `insert_index`.
    /// Otherwise, the value will be located in the right sequence, at index `insert_index - mid`.
    ///
    /// \note This function does not apply the new size to either sequence, it only moves elements.
    static void sequence_insert(u32 value_size, byte* left, byte* right, u32 count, u32 mid,
                                u32 insert_index, const byte* value);

private:
    handle<header> m_handle;
    u32 m_value_size = 0;   // Size of a single value
    u32 m_max_children = 0; // Max number of values per node
};

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_LEAF_NODE_HPP
