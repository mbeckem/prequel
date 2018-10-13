#ifndef PREQUEL_TYPE_TRAITS_HPP
#define PREQUEL_TYPE_TRAITS_HPP

#include <prequel/defs.hpp>

#include <type_traits>

namespace prequel {

template<typename T>
struct type_t {
    using type = T;
};

template<typename... T>
using void_t = void;

template<typename T>
struct is_trivial {
    static constexpr bool value =
                std::is_trivially_copyable<T>::value;
};

template<typename T>
struct always_false : std::false_type {};

template<bool c, typename T = void>
using disable_if_t = std::enable_if_t<!c, T>;

template<typename T>
using remove_cvref_t = std::remove_cv_t<std::remove_reference_t<T>>;

template<typename T, typename U>
using DisableSelf = disable_if_t<
    std::is_base_of<T, remove_cvref_t<U>>::value
>;

// Performs a static pointer cast and preserves the constness.
template<typename To, typename From>
To* const_pointer_cast(From* from) { return static_cast<To*>(from); }

template<typename To, typename From>
const To* const_pointer_cast(const From* from) { return static_cast<const To*>(from); }


namespace detail {

template<typename Ptr>
struct member_ptr_traits;

template<typename Object, typename Member>
struct member_ptr_traits<Member Object::*> {
    using object_type = Object;
    using member_type = Member;
};

} // namespace detail

/**
 * Returns the class type when given the type of a member data.
 *
 * Example:
 *  struct ex {
 *      int x;
 *  };
 *
 *  object_type_t<decltype(&ex::x)> is ex.
 */
template<typename MemberPtr>
using object_type_t = typename detail::member_ptr_traits<MemberPtr>::object_type;

/**
 * Returns the class member type when given the type of a member data.
 *
 * Example:
 *  struct ex {
 *      int x;
 *  };
 *
 *  object_type_t<decltype(&ex::x)> is int.
 */
template<typename MemberPtr>
using member_type_t = typename detail::member_ptr_traits<MemberPtr>::member_type;

} // namespace prequel

#endif // PREQUEL_TYPE_TRAITS_HPP
