#ifndef EXTPP_DETAIL_MEMORY_HPP
#define EXTPP_DETAIL_MEMORY_HPP

#include <extpp/defs.hpp>
#include <extpp/type_traits.hpp>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <type_traits>

namespace extpp::detail {

/// Copy `count` objects from `source` to `dest`.
template<typename T>
static void copy(const T* source, size_t count, T* dest) {
    static_assert(std::is_trivially_copyable<T>::value,
                  "T must be trivially copyable");
    std::memmove(dest, source, sizeof(T) * count);
}

/// Shifts `count` objects from `source` by `shift` positions.
/// E.g. `shift(ptr, 5, 1)` moves 5 objects starting from `ptr` one step to the right.
template<typename T>
static void shift(T* source, size_t count, std::ptrdiff_t shift) {
    copy(source, count, source + shift);
}

// We need a dummy object (not initialized), should be optimized out.
template<typename T> struct dummy_object { static T value; };

// This code uses old-style compile time programming by design to get
// around constexpr restrictions (i.e. forbidden casts at compile time).
// Base must be a (non-virtual) base class of Derived.
//
// Inspired by the blog post at
// https://thecppzoo.blogspot.de/2016/10/constexpr-offsetof-practical-way-to.html
template<typename Derived, typename Base>
struct offset_of_base_helper {
    using holder = dummy_object<Derived>;

    enum {
        // The pointer offset for Derived -> Base casts.
        // Note: this does not work with MSVC.
        value = (char *)((Base *) std::addressof(holder::value)) - (char *) std::addressof(holder::value)
    };

// This technique doesn't seem to be required.
//    char for_sizeof[
//        (char *)((Base *) &holder::value) -
//        (char *)&holder::value
//    ];
// later:
//    sizeof(...::for_sizeof).
};

template<typename T, typename U, typename V, V U::*Member>
struct offset_of_member_helper {
    using holder = dummy_object<T>;

    enum {
        value = (char *) (std::addressof(holder::value.*Member)) - (char *) std::addressof(holder::value)
    };
};

/// Returns the offset of `Base` within an object of type `Derived`.
template<typename Derived, typename Base>
constexpr ptrdiff_t offset_of_base() {
    return offset_of_base_helper<remove_cvref_t<Derived>, Base>::value;
}

struct member_helpers {
    template<typename T, typename V>
    static T object_type_of(V T::*);

    template<typename T, typename V>
    static V value_type_of(V T::*);
};

/// Returns the offset of `Member` in `T`.
template<typename T, auto Member>
constexpr ptrdiff_t offset_of_member() {
    static_assert(std::is_member_object_pointer<decltype(Member)>::value,
                  "Must pass a member pointer.");
    using type = remove_cvref_t<T>;
    using member_object_type = remove_cvref_t<decltype(member_helpers::object_type_of(Member))>;
    using member_value_type = decltype(member_helpers::value_type_of(Member));

    static_assert(std::is_base_of<member_object_type, type>::value,
                  "Member does not belong to the specified type.");
    return offset_of_member_helper<type, member_object_type, member_value_type, Member>::value;
}

} // namespace extpp::detail

#endif // EXTPP_DETAIL_MEMORY_HPP
