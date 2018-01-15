#ifndef EXTPP_HEAP_TYPE_SET_HPP
#define EXTPP_HEAP_TYPE_SET_HPP

#include <extpp/heap/base.hpp>

#include <unordered_map>

namespace extpp::heap_detail {

/// Contains the runtime type information for objects
/// within a heap.
/// Types have to be registered because their definition
/// relies on non-serializable data such as visitor functions.
class type_set {
public:
    /// Type info can only be registered once and must be valid.
    /// Types must be registered before they are required by the heap,
    /// i.e. before objects of that type are created/used or
    /// a garbage collection occurs.
    void register_type(const type_info& type) {
        type.validate();

        auto [pos, inserted] = m_types.emplace(type.index, type);
        EXTPP_CHECK(inserted, "The type index was not unique.");
        unused(pos);
    }

    /// Returns the type info associated with the given type index.
    /// Throws if no type info was registered.
    const type_info& get(const type_index& index) const {
        EXTPP_ASSERT(index, "Invalid type index.");

        auto pos = m_types.find(index);
        EXTPP_CHECK(pos != m_types.end(),
                    "Could not find type information for a type. "
                    "Did you forget to register it?");
        return pos->second;
    }

private:
    std::unordered_map<type_index, type_info> m_types;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_TYPE_SET_HPP
