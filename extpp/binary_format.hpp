#ifndef EXTPP_BINARY_FORMAT_HPP
#define EXTPP_BINARY_FORMAT_HPP

#include <extpp/defs.hpp>
#include <extpp/type_traits.hpp>
#include <extpp/detail/iter_tools.hpp>

#include <tuple>
#include <utility>

namespace extpp {

template<typename T, typename... V>
class binary_format {
public:
    constexpr binary_format(V T::*... fields)
        : m_fields(fields...)
    {}

    /// Visits all fields in the order in which they have been defined.
    template<typename Visitor>
    constexpr void visit_fields(Visitor&& v) const {
        detail::tuple_for_each(m_fields, [&](auto ptr) {
            v(ptr);
        });
    }

    /// Returns the number of fields.
    constexpr size_t fields() const {
        return sizeof...(V);
    }

private:
    std::tuple<V T::*...>  m_fields;
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
/// \ingroup serialization
template<typename T, typename... V>
constexpr binary_format<T, V...> make_binary_format(V T::*... members) {
    return binary_format<T, V...>(members...);
}

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

template<typename T>
constexpr bool has_binary_format() {
    return binary_format_access::has_binary_format<T>();
}

template<typename T>
constexpr auto get_binary_format() {
    return binary_format_access::get_binary_format<T>();
}

} // namespace extpp

#endif // EXTPP_BINARY_FORMAT_HPP
