#ifndef EXTPP_HEAP_HPP
#define EXTPP_HEAP_HPP

#include <extpp/binary_format.hpp>
#include <extpp/defs.hpp>
#include <extpp/exception.hpp>
#include <extpp/detail/operators.hpp>

#include <functional>
#include <memory>

namespace extpp {

class heap;
class heap_impl;

class heap {
public:
    /**
     * Refers to an object stored in the heap.
     */
    class reference : public detail::make_comparable<reference> {
    public:
        /// Raw value of an invalid reference.
        static constexpr u64 invalid_value = u64(-1);

    public:
        constexpr reference() = default;

        /// Returns true if the reference points to a valid object.
        /// @{
        constexpr bool valid() const { return m_index != invalid_value; }
        constexpr explicit operator bool() const { return valid(); }
        /// @

        /// Returns the current internal value of the reference.
        /// Values should not be interpreted in any way other than
        /// the fact that they uniquely identify objects if they
        /// are not equal to @c invalid_value.
        constexpr u64 value() const { return m_index; }

        constexpr bool operator<(const reference& other) const {
            // +1: the invalid reference is smaller than all valid ones.
            return (m_index + 1) < (other.m_index + 1);
        }

        constexpr bool operator==(const reference& other) const {
            return m_index == other.m_index;
        }


    public:
        static constexpr auto get_binary_format() {
            return make_binary_format(&reference::m_index);
        }

    private:
        friend class heap;
        friend class heap_impl;

        explicit reference(u64 index): m_index(index) {}

    private:
        /// Points into the object table.
        u64 m_index = invalid_value;
    };

    /**
     * A type index uniquely identifies a type in a heap.
     * Type indices have a underlying numeric value that can be
     * set by the user (some values are reserved).
     *
     * Type indices are serialized to disk and must be the same across all runs
     * of the program.
     */
    class type_index {
    public:
        static constexpr u32 invalid_value = u32(-1);

    public:
        /// The default constructed type index is invalid.
        constexpr type_index() = default;

        /// Construct a type index with a custom value.
        constexpr type_index(u32 value)
            : m_value(value)
        {}

        constexpr u32 value() const { return m_value; }
        constexpr bool valid() const { return m_value != invalid_value; }
        constexpr explicit operator bool() const { return valid(); }

        constexpr bool operator<(const type_index& other) const {
            return (m_value + 1) < (other.m_value + 1);
        }

        constexpr bool operator==(const type_index& other) const {
            return m_value == other.m_value;
        }

    public:
        static constexpr auto get_binary_format() {
            return make_binary_format(&type_index::m_value);
        }

    private:
        u32 m_value = invalid_value;
    };

    /**
     * Visits the references within an object.
     */
    class reference_visitor {
    public:
        virtual ~reference_visitor() = default;

        /**
         * Must be called by the user for every reference
         * contained within an object.
         */
        virtual void visit(reference object) = 0;
    };

    /**
     * A type_info instance gives metadata about objects on disk.
     *
     * For an object of a given type, we can figure out how large
     * it is and whether it contains references to other objects.
     *
     * Types contain runtime information that cannot be serialized
     * (such as dynamic functions) and have to be re-registered
     * every time the heap is loaded. The type index must
     * always be exactly the same.
     */
    class type_info {
    public:
        /// The unique type index. Must be initialized by the user.
        type_index index;

        /// Whether objects of this type may contain references at all.
        /// Set this to false if the type describes a blob-like structure
        /// that never contains any references (e.g. a string).
        /// This allows us to completely skip visiting the object during gc.
        bool contains_references = true;

        /// The static size of objects of this type (in bytes).
        /// For object types that do not have a dynamic size, this
        /// field describes the size of *all* objects of that type.
        u64 size = 0;

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
        /// It follows that you MUST NOT follow any references that might still be
        /// set inside this object, unless you know for certain that the referenced
        /// object is still alive.
        std::function<void(reference)> finalizer;

        // TODO: Move to cpp
        void validate() const {
            if (!index)
                EXTPP_THROW(bad_argument("Type index is unset."));

            if (contains_references) {
                if (!visit_references)
                    EXTPP_THROW(bad_argument("Objects may contain references but visit_references is unset."));
            } else {
                if (visit_references)
                    EXTPP_THROW(bad_argument("Objects do not contain references but visit_references is set."));
            }
        }
    };


public:
    ~heap();

    heap(const heap&) = delete;
    heap(heap&&) noexcept;

    heap& operator=(const heap&) = delete;
    heap&& operator=(heap&&) noexcept;

public:
    /**
     * Insert a new object into the heap. The type of the object must have
     * been previously registered using @ref register_type().
     *
     * The reference returned by this function can be load in successive
     * calls to @ref load() to access the object's data and @ref update()
     * to change it.
     *
     * @param type
     *      The type of the object.
     * @param object_data
     *      The object's raw data.
     * @param object_size
     *      The object's size, in bytes.
     * @return
     *      A reference to the new object.
     */
    reference create(type_index type, const void* object_data, u64 object_size);

    /**
     * Loads the object from disk into the given buffer.
     */
    // TODO: Raw api
    void load(reference object, std::vector<byte>& buffer);

    /**
     * Update the object on disk with `object_data`.
     * `object_data` must point to at least `object_size` readable bytes.
     * Note that `object_size` must be exactly the size the object
     * currently occupies on disk.
     */
    // TODO: API for partial reads and writes.
    void update(reference object, const void* object_data, u64 object_size);

    /**
     * Returns the type of the given object.
     */
    type_index type_of(reference object) const;

    /**
     * Returns the size of the given object, in bytes.
     */
    u64 size_of(reference object) const;

private:
    std::unique_ptr<heap_impl> m_impl;
};

} // namespace extpp

namespace std {

template<> struct hash<extpp::heap::reference> {
    std::size_t operator()(const extpp::heap::reference& ref) const {
        return std::hash<extpp::u64>()(ref.value());
    }
};

template<> struct hash<extpp::heap::type_index> {
    std::size_t operator()(const extpp::heap::type_index& index) const {
        return std::hash<extpp::u32>()(index.value());
    }
};

} // namespace std

#endif // EXTPP_HEAP_HPP
