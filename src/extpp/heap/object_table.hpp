#ifndef EXTPP_HEAP_OBJECT_TABLE_HPP
#define EXTPP_HEAP_OBJECT_TABLE_HPP

#include "base.hpp"

#include <extpp/address.hpp>
#include <extpp/heap.hpp>
#include <extpp/serialization.hpp>
#include <extpp/stream.hpp>

#include <variant>

namespace extpp::heap_detail {

struct object_entry {
public:
    static constexpr u64 invalid_index = (u64(1) << 63) - 1;

private:
    /*
     * This value contains a union of (reference_entry, free_entry).
     * Bit layout (from most to least significant):
     *  - 1 Bit (0 = reference, 1 = free)
     *
     *  - if reference:
     *      2 bit:  tag value 0..3
     *      61 bit: cell address (raw byte address divided by cell size)
     *
     *  - if free:
     *      63 bit: next free object table entry, or invalid_index if this is the last entry.
     *              TODO: use a sorted tree for compact object table?
     */
    u64 m_value;

private:
    static constexpr u64 free_mask = (u64(1) << 63);
    static constexpr u64 tag_mask = ((u64(1) << 62) | (u64(1) << 61));
    static constexpr u64 cell_mask = (u64(1) << 61) - 1;
    static constexpr u64 next_mask = (u64(1) << 63) - 1;

    object_entry() = default;

    void _set_free(u64 next) {
        EXTPP_ASSERT(next <= invalid_index, "Next out of range.");
        m_value = (u64(1) << 63) | next;
    }

    void _set_reference(int tag, u64 cell) {
        EXTPP_ASSERT(tag >= 0 && tag <= 3, "Tag out of range.");
        EXTPP_ASSERT(cell < (u64(1) << 61), "Cell out of range.");
        m_value = (u64(tag) << 61) | cell;
    }

    bool _get_free() const {
        return (m_value & free_mask) != 0;
    }

    int _get_tag() const {
        return (m_value & tag_mask) >> 61;
    }

    u64 _get_cell() const {
        return m_value & cell_mask;
    }

    u64 _get_next() const {
        return m_value & next_mask;
    }

public:
    static object_entry make_reference(int tag, raw_address address) {
        EXTPP_ASSERT(address.valid(), "Address must be valid.");
        EXTPP_ASSERT(address.value() % cell_size == 0, "Address must be aligned on a cell boundary.");

        object_entry ent;
        ent._set_reference(tag, address.value() / cell_size);
        return ent;
    }

    static object_entry make_free(u64 next) {
        object_entry ent;
        ent._set_free(next);
        return ent;
    }

    bool is_free() const { return _get_free(); }
    bool is_reference() const { return !is_free(); }

    u64 next() const {
        EXTPP_ASSERT(is_free(), "Must be free.");
        return _get_next();
    }

    int tag() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return _get_tag();
    }

    raw_address address() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return raw_address(_get_cell() * cell_size);
    }

private:
    friend class binary_format_access;

    static constexpr auto get_binary_format() {
        return make_binary_format(&object_entry::m_value);
    }
};

static_assert(sizeof(object_entry) == sizeof(u64), "Compact representation.");
static_assert(serialized_size<object_entry>() == serialized_size<u64>(), "Compact serialized representation.");

class object_table {
private:
    static constexpr u64 invalid_index = object_entry::invalid_index;

public:
    class anchor {
        /// Index of the first table entry that can be used for new reference.
        /// Invalid index if there is none.
        u64 first_free_index = invalid_index;

        /// Storage of the object table itself.
        stream<object_entry>::anchor table;

    private:
        friend class object_table;
        friend class binary_format_access;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::first_free_index, &anchor::table);
        }
    };
public:
    object_table(anchor_handle<anchor> _anchor, allocator& alloc)
        : m_anchor(std::move(_anchor))
        , m_table(m_anchor.member<&anchor::table>(), alloc)
    {}

    // TODO: Stream cursor

    /**
     * Returns true iff the reference index is valid, i.e. if it points
     * to an entry that is both in bounds and that contains a pointer
     * to an object.
     */
    bool valid(u64 index) const {
        return index < m_table.size() && m_table[index].is_reference();
    }

    /**
     * Allocates a new slot within the object table for the given entry.
     * Returns the index of that slot or throws an exception.
     */
    u64 insert(object_entry entry) {
        EXTPP_ASSERT(entry.is_reference(), "Must be a reference entry.");

        u64 index = m_anchor.get<&anchor::first_free_index>();
        if (index != invalid_index) {
            // There is at least one free entry available for reuse.
            object_entry free_entry = m_table[index];
            EXTPP_ASSERT(free_entry.is_free(), "Entry must be free.");

            m_table.set(index, entry);
            m_anchor.set<&anchor::first_free_index>(free_entry.next());
            return index;
        }

        // Create a new entry at the end.
        m_table.push_back(entry);
        return m_table.size() - 1;
    }

    /**
     * Replaces the entry at the given entry. Called when the object was relocated.
     */
    void replace(u64 index, object_entry entry) {
        EXTPP_ASSERT(entry.is_reference(), "Must be a reference entry.");
        EXTPP_ASSERT(index < m_table.size(), "Index is out of bounds.");
        EXTPP_ASSERT(m_table[index].is_reference(), "Index must point to a reference.");
        m_table.set(index, entry);
    }

    /**
     * Returns the object entry associated with that index.
     * Note: access to freed entries is forbidden.
     */
    object_entry get(u64 index) const {
        EXTPP_ASSERT(index < m_table.size(), "Index is out of bounds.");

        object_entry ent = m_table[index];
        EXTPP_ASSERT(ent.is_reference(), "Index must point to a reference.");
        return ent;
    }

    /**
     * Removes the given entry from the object table (it will be reused by future allocations).
     */
    void remove(u64 index) {
        EXTPP_ASSERT(index < m_table.size(), "Index is out of bounds.");
        EXTPP_ASSERT(!m_table[index].is_free(), "Entry was already free.");

        u64 next = m_anchor.get<&anchor::first_free_index>();
        m_table.set(index, object_entry::make_free(next));
        m_anchor.set<&anchor::first_free_index>(index);
    }

private:
    anchor_handle<anchor> m_anchor;
    stream<object_entry> m_table;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_OBJECT_TABLE_HPP
