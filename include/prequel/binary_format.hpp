#ifndef PREQUEL_BINARY_FORMAT_HPP
#define PREQUEL_BINARY_FORMAT_HPP

#include <prequel/defs.hpp>
#include <prequel/type_traits.hpp>

#include <tuple>
#include <utility>

namespace prequel {

/// \defgroup binary_format Binary Format

/// Stores a series of member data pointers in order to reflect
/// over the members of a class. Only used at compile time.
///
/// \ingroup binary_format
template<typename T, typename... V>
class binary_format {
public:
    constexpr binary_format(V T::*... fields)
        : m_fields(fields...) {}

    /// Returns the description of the classes fields,
    /// as a tuple of member data pointers.
    constexpr const std::tuple<V T::*...>& fields() const { return m_fields; }

    /// Returns the number of fields.
    constexpr size_t field_count() const { return sizeof...(V); }

private:
    std::tuple<V T::*...> m_fields;
};

/// Create a binary_format that describes a user defined structure.
/// Use this function to provide the body of the special `get_binary_format()`
/// static function that enables a type for serialzation.
///
/// Example
/// -------
///
/// \code{.cpp}
///     struct my_type {
///         u32 a = 0;
///         u32 b = 1;
///         my_other_type c;
///
///         // This function allows the library to understand
///         // the layout of the struct. This is required because C++ lacks reflection.
///         // Every member that shall be serialized must be listed exactly once,
///         // in any order. The order of the members in this list defines the
///         // order of the fields on disk and must not change if the binary format
///         // should remain stable.
///         //
///         // All members listed here must be serializable as well. Integer types of fixed
///         // size (i.e. std::[u]intN_t) and arrays/pairs/tuples of serializable types are
///         // supported by default.
///         static constexpr auto get_binary_format() {
///             return make_binary_format(&my_type::a, &my_type::b, &my_type::c);
///         }
///     };
/// \endcode
///
/// \ingroup binary_format
template<typename T, typename... V>
constexpr binary_format<T, V...> make_binary_format(V T::*... members) {
    return binary_format<T, V...>(members...);
}

/// Declare this class as a friend if you wish to keep the `get_binary_format()`
/// function private.
///
/// \ingroup binary_format
class binary_format_access {
private:
    template<typename T>
    static auto test_binary_format(T*) -> decltype((void) T::get_binary_format(), std::true_type());

    static std::false_type test_binary_format(...);

public:
    template<typename T>
    static constexpr bool has_binary_format() {
        using type = remove_cvref_t<T>;
        using result = decltype(test_binary_format(static_cast<type*>(nullptr)));
        return result::value;
    }

    template<typename T>
    static constexpr auto get_binary_format() {
        using type = remove_cvref_t<T>;
        static_assert(has_binary_format<type>(),
                      "The type does not implement the get_binary_format() function.");
        return type::get_binary_format();
    }
};

/// Returns true iff the type implements the get_binary_format() static function.
///
/// \ingroup binary_format
template<typename T>
constexpr bool has_binary_format() {
    return binary_format_access::has_binary_format<T>();
}

/// Returns a binary format instance that describes the type T.
/// Results in a compile-time error if T does not implement the get_binary_format() static function.
///
/// \ingroup binary_format
template<typename T>
constexpr auto get_binary_format() {
    if constexpr (!has_binary_format<T>()) {
        static_assert(has_binary_format<T>(),
                      "The type must implement the get_binary_format function.");
    }
    return binary_format_access::get_binary_format<T>();
}

} // namespace prequel

#endif // PREQUEL_BINARY_FORMAT_HPP
