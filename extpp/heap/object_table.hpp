#ifndef EXTPP_HEAP_OBJECT_TABLE_HPP
#define EXTPP_HEAP_OBJECT_TABLE_HPP

#include <extpp/address.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/defs.hpp>
#include <extpp/stream.hpp>
#include <extpp/heap/base.hpp>

namespace extpp::heap_detail {

/// An entry for an object within the object table.
/// Free entries are reused and linked together in a free list.
template<u32 BlockSize>
union object_entry {
public:
    static constexpr u64 invalid_index = (u64(1) << 63) - 1;

private:
    struct reference_t {
        u64 free: 1;            // Type discriminator.
        u64 unused: 3;
        u64 address: 60;        // On disk address divided by the cell size.
    } ref;

    struct free_t {
        u64 free: 1;
        u64 next: 63;
    } free;

public:
    static object_entry make_reference(raw_address addr) {
        EXTPP_ASSERT(addr.value() % cell_size == 0, "Address must be aligned correctly.");

        object_entry e;
        e.ref.free = 0;
        e.ref.unused = 0;
        e.ref.address = addr.value() / cell_size;
        return e;
    }

    static object_entry make_free(u64 next) {
        EXTPP_ASSERT(next <= invalid_index, "Index too large.");

        object_entry e;
        e.free.free = 1;
        e.free.next = next;
        return e;
    }

public:
    bool is_free() const { return free.free == 1; }
    bool is_reference() const { return ref.free == 0; }

    raw_address get_address() const {
        return raw_address::byte_address(as_reference().address * cell_size);
    }

    void set_address(raw_address addr) {
        EXTPP_ASSERT(addr.value() % cell_size == 0, "Address must be aligned correctly.");
        as_reference().address = addr.value() / cell_size;
    }

    u64 get_next() const { return as_free().next; }

    void set_next(u64 next) {
        as_free().next = next;
    }

private:
    object_entry() {} // Not initialized.

    reference_t& as_reference() {
        return const_cast<reference_t&>(
                    const_cast<const object_entry*>(this)->as_reference());
    }

    free_t& as_free() {
        return const_cast<free_t&>(
                    const_cast<const object_entry*>(this)->as_free());
    }

    const reference_t& as_reference() const {
        EXTPP_ASSERT(is_reference(), "Must be a reference.");
        return ref;
    }

    const free_t& as_free() const {
        EXTPP_ASSERT(is_free(), "Must be a freelist entry.");
        return free;
    }
};

template<u32 BlockSize>
class object_table {
public:
    using object_entry_type = object_entry<BlockSize>;

    static_assert(sizeof(object_entry_type) == sizeof(u64), "Object table entry size too large.");

private:
    using storage_type = stream<object_entry_type, BlockSize>;

    static constexpr u64 invalid_index = object_entry_type::invalid_index;

public:
    class anchor {
        typename storage_type::anchor objects;

        u64 first_free_index = invalid_index;

        friend class object_table;
    };

    // TODO: Destroy

    using iterator = typename storage_type::iterator;

public:
    object_table(anchor_ptr<anchor> anc, allocator<BlockSize>& alloc)
        : m_anchor(std::move(anc))
        , m_objects(m_anchor.member(&anchor::objects), alloc)
    {}

    iterator begin() const { return m_objects.begin(); }
    iterator end() const { return m_objects.end(); }

    /// Returns true if this reference is valid, i.e. if
    /// it points to an entry that is not free (and within bounds).
    bool valid(reference ref) const {
        return ref && ref.value() < m_objects.size() && m_objects[ref.value()].is_reference();
    }

    /// Allocate a slot in the object table for the new entry.
    /// Returns the index of that slot wrapped in a reference.
    reference insert(object_entry_type entry) {
        EXTPP_ASSERT(entry.is_reference(), "Must be a reference entry.");

        u64 index = m_anchor->first_free_index;
        if (index != invalid_index) {
            auto pos = m_objects.begin() + index;

            m_anchor->first_free_index = pos->get_next();
            m_anchor.dirty();

            m_objects.replace(pos, entry);
            return reference(index);
        }

        m_objects.push_back(entry);
        return reference(m_objects.size() - 1);
    }

    reference to_reference(const iterator& pos) const {
        EXTPP_ASSERT(pos != end(), "End iterator.");
        EXTPP_ASSERT(!pos->is_free(), "Must not form references to free table entries.");
        return reference(pos - begin());
    }

    /// Modify the object table entry at that position.
    template<typename Fn>
    void modify(iterator pos, Fn&& fn) {
        m_objects.modify(pos, std::forward<Fn>(fn));
    }

    /// Remove an object entry from the table and link it into the free list.
    void remove(iterator pos) {
        EXTPP_ASSERT(!pos->is_free(), "Must not be free already.");

        m_objects.replace(pos, object_entry_type::make_free(m_anchor->first_free_index));
        m_anchor->first_free_index = pos - begin();
        m_anchor.dirty();
    }

    /// Returns the entry for that reference.
    /// The reference must be valid.
    /// \pre `valid(ref)`.
    object_entry_type operator[](reference ref) const {
        EXTPP_ASSERT(valid(ref), "Invalid reference.");
        return m_objects[ref.value()];
    }

private:
    anchor_ptr<anchor> m_anchor;
    storage_type m_objects;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_OBJECT_TABLE_HPP
