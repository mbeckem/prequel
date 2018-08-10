#ifndef EXTPP_HEAP_OBJECT_TABLE_HPP
#define EXTPP_HEAP_OBJECT_TABLE_HPP

#include <extpp/heap/base.hpp>

#include <extpp/address.hpp>
#include <extpp/heap.hpp>
#include <extpp/serialization.hpp>
#include <extpp/stream.hpp>

#include <variant>

namespace extpp::heap_detail {

struct object_entry {
public:
    static constexpr u64 max_free_index = u64(1) << 63;

private:
    struct common_t {
        u64 free: 1;
    };

    struct array_reference_t {
        u64 free: 1;        // 0 for references
        u64 tag: 3;
        u64 address: 60;
        u64 size;           // 64 bits is overkill but 32 would not be enough.
    };

    struct free_t {
        u64 free: 1;        // 1 for free entries
        u64 next: 63;
        u64 unused;
    };

    union {
        common_t common;
        free_t free;
        array_reference_t reference;
    };

public:
    static object_entry make_reference(int tag, raw_address address, u64 size) {
        EXTPP_ASSERT(address.valid(), "Address must be valid.");
        EXTPP_ASSERT(address.value() % cell_size == 0, "Address must be aligned on a cell boundary.");
        EXTPP_ASSERT(tag >= 0 && tag < 8, "Invalid tag.");

        object_entry ent;
        ent.reference.free = 0;
        ent.reference.tag = tag;
        ent.reference.address = address.value() / cell_size;
        ent.reference.size = size;
        return ent;
    }

    static object_entry make_free(u64 next) {
        EXTPP_ASSERT(next <= max_free_index, "Next index out of range.");

        object_entry ent;
        ent.free.free = 1;
        ent.free.next = next;
        ent.free.unused = 0;
        return ent;
    }

    bool is_free() const { return common.free; }
    bool is_reference() const { return !is_free(); }

    u64 next() const {
        EXTPP_ASSERT(is_free(), "Must be free.");
        return free.next;
    }

    int tag() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return reference.tag;
    }

    raw_address address() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return raw_address(reference.address * cell_size);
    }

    u64 size() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return reference.size;
    }

public:
    struct binary_serializer {
    private:
        static constexpr u64 free_bit = u64(1) << 63;

        static constexpr u64 tag_shift = 60;
        static constexpr u64 tag_mask = u64(7) << tag_shift;

    public:
        static constexpr size_t serialized_size() { return 16; }

        static void serialize(const object_entry& entry, byte* b) {
            u64 repr[2];

            if (entry.common.free) {
                const free_t& free = entry.free;
                repr[0] = free_bit | free.next;
                repr[1] = 0;
            } else {
                const array_reference_t& ref = entry.reference;
                repr[0] = (u64(ref.tag) << tag_shift) | ref.address;
                repr[1] = ref.size;
            }
            extpp::serialize(repr, b);
        }

        static void deserialize(object_entry& entry, const byte* b) {
            u64 repr[2];
            extpp::deserialize(repr, b);

            if (repr[0] & free_bit) {
                free_t& free = entry.free;
                free.free = 1;
                free.next = repr[0] & ~free_bit;
                free.unused = 0;
            } else {
                array_reference_t& ref = entry.reference;
                ref.free = 0;
                ref.tag = repr[0] >> tag_shift;
                ref.address = repr[0] & ~tag_mask;
                ref.size = repr[1];
            }
        }
    };
};

static_assert(serialized_size<object_entry>() == 2 * serialized_size<u64>(), "Compact serialized representation.");

class object_table {
public:
    class anchor {
        /// Index of the first table entry that can be used for new reference.
        /// Invalid index if there is none.
        u64 first_free_index = object_entry::max_free_index;

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
        if (index != object_entry::max_free_index) {
            // There is at least one free entry available for reuse.
            object_entry free_entry = m_table[index];
            EXTPP_ASSERT(free_entry.is_free(), "Entry must be free.");

            m_table.set(index, entry);
            m_anchor.set<&anchor::first_free_index>(free_entry.next());
            return index;
        }

        // Create a new entry at the end.
        if (m_table.size() == object_entry::max_free_index)
            EXTPP_THROW(bad_alloc("Object table is exhausted."));

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
