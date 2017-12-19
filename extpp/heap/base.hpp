#ifndef EXTPP_HEAP_BASE_HPP
#define EXTPP_HEAP_BASE_HPP

#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/detail/operators.hpp>

#include <functional>

namespace extpp::heap_detail {

/// The minimum unit of allocation.
struct cell {
    byte data[16];
};

inline constexpr u64 cell_size = 16;
inline constexpr u64 cell_size_log = log2(cell_size);

template<u32 BlockSize>
class object_table;

template<u32 BlockSize>
class segregated_free_list;

class type_set;

} // namespace extpp::heap_detail

namespace extpp {

/// A reference points to an object managed by the heap.
class reference : public detail::make_comparable<reference> {
public:
    /// The raw value of the invalid reference.
    static constexpr u64 invalid_value = u64(-1);

public:
    /// The default-constructed reference is invalid.
    /// Only the heap can hand out valid references.
    reference() = default;

    /// Returns true if this reference stores a valid index into the object table.
    bool valid() const { return m_index != invalid_value; }

    /// Returns true if this reference stores a valid index into the object table.
    explicit operator bool() const { return valid(); }

    /// The raw value of this reference.
    u64 value() const { return m_index; }

    bool operator<(const reference& other) const {
        // +1 so that the invalid references is lesser than all others.
        return (m_index + 1) < (other.m_index + 1);
    }

    bool operator==(const reference& other) const {
        return m_index == other.m_index;
    }

private:
    template<u32 BlockSize>
    friend class heap_detail::object_table;

    explicit reference(u64 index): m_index(index) {}

private:
    u64 m_index = invalid_value;
};

static_assert(sizeof(reference) == sizeof(u64), "Requires EBO.");

/// A type index uniquely identifies a type in a heap.
/// Type indices have a underlying numeric value that can be
/// set by the user (some values are reserved).
///
/// Type indices are serialized to disk and must be the same across all runs
/// of the program.
class type_index : public detail::make_comparable<type_index> {
public:
    /// The default constructed type index is invalid.
    constexpr type_index() = default;

    /// Construct a type index with a custom value.
    /// \note The invalid type index has the value 0, chose values
    /// `>= 1` for your types.
    constexpr type_index(u32 value)
        : m_value(value)
    {}

    constexpr u32 value() const { return m_value; }
    constexpr bool valid() const { return m_value != 0; }
    constexpr explicit operator bool() const { return valid(); }

    constexpr bool operator==(const type_index& other) const {
        return m_value == other.m_value;
    }

    constexpr bool operator<(const type_index& other) const {
        return m_value < other.m_value;
    }

private:
    u32 m_value = 0;
};

static_assert(sizeof(type_index) == sizeof(u32), "Requires EBO.");

class reference_visitor {
public:
    virtual ~reference_visitor() = default;
    virtual void visit(reference) = 0;
};

/// A type_info instance gives metadata about objects on disk.
///
/// For an object of a given type, we can figure out how large
/// it is and whether it contains references to other objects.
///
/// Types contain runtime information that cannot be serialized
/// (such as dynamic functions) and have to be re-registered
/// every time the heap is loaded. The type index must
/// always be exactly the same.
class type_info {
public:
    /// The unique type index. Must be initialized by the user.
    type_index index;

    /// Whether objects of this type may contain references at all.
    /// Set this to false if the type describes a blob-like structure
    /// that never contains any references (e.g. a string).
    /// This allows us to skip visiting the object during gc.
    bool contains_references = true;

    /// The static size of objects of this type (in bytes).
    /// For object types that do not have a dynamic size, this
    /// field describes the size of *all* objects of that type.
    /// If this type has dynamic size, this value encodes
    /// the *minimum* size of all objects of that type.
    u64 size = 0;

    /// True if objects of this type have variable size (for example: arrays, strings).
    bool dynamic_size = false;

    /// This function gets passed a reference to some object of this type.
    /// It must make sure that all references from that object to
    /// other objects within the same heap are passed to the visitor.
    /// This function can read, but not alter the object's data.
    std::function<void(reference, reference_visitor&)> visit_references;

    /// Called when the referenced object is about to be destroyed.
    /// The finalizer must make sure that no part of the system holds
    /// on to the object and that all resources used by the object
    /// are cleaned up. This includes, for example, purging
    /// references to that object from auxilary datastructures.
    ///
    /// The finalizer may access the referenced object but cannot alter it.
    /// The order in which finalizers are invoked is unspecified. In other words,
    /// the objects referenced by this object may have already been finalized.
    std::function<void(reference)> finalizer;

    void validate() const {
        if (!index)
            EXTPP_ABORT("Type index is unset.");
        if (!dynamic_size && size == 0)
            EXTPP_ABORT("Non-dynamic objects must have non-zero size.");
        if (contains_references) {
            if (!visit_references)
                EXTPP_ABORT("Objects may contain references but visit_references is unset.");
        } else {
            if (visit_references)
                EXTPP_ABORT("Objects do not contain references but visit_references is set.");
        }
    }
};

} // namespace extpp

namespace std {

template<>
struct hash<extpp::reference> {
    std::size_t operator()(const extpp::reference& ref) const {
        return std::hash<extpp::u64>()(ref.value());
    }
};

template<>
struct hash<extpp::type_index> {
    std::size_t operator()(const extpp::type_index& index) const {
        return std::hash<extpp::u64>()(index.value());
    }
};

} // namespace std

#endif // EXTPP_HEAP_BASE_HPP
