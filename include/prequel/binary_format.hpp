#ifndef PREQUEL_BINARY_FORMAT_HPP
#define PREQUEL_BINARY_FORMAT_HPP

#include <prequel/defs.hpp>
#include <prequel/type_traits.hpp>

#include <tuple>
#include <utility>

namespace prequel {

/**
 * Describes the binary format of a C++ datatype.
 * The binary format class internally stores a seriies of member data pointers
 * in order to reflect over the relevant content of a datatype.
 * This is required because C++ lacks reflection, so we needed to roll our own.
 *
 * In order to implement simple binary serialization for your own C++ datatype,
 * implement the `static constexpr get_binary_format()` function in your type like
 * in the following example:
 *
 *  \code{.cpp}
 *      struct my_type {
 *          u32 a = 0;
 *          u32 b = 1;
 *          optional<i32> c;
 *
 *          // This function allows the library to understand
 *          // the layout of the struct.
 *          static constexpr auto get_binary_format() {
 *              return binary_format(&my_type::a, &my_type::b, &my_type::c);
 *          }
 *      };
 * \endcode
 *
 * The example defines a type with a binary serialization format that contains of
 * two unsigned 32-bit integers (in big endian format) and an optional signed 32-bit
 * integer, again in big endian format. The optional type is supported by this library,
 * as long as the type within the optinal can also be serialized using this library.
 * When serialized, the size of the struct above is 13 bytes (12 for the three integers,
 * 1 byte overhead for the optional).
 *
 * Important guidelines when using the binary format facilities:
 * - List every relevant member of your type (i.e. the ones you want serialized) exactly once.
 *   The library can only serialized members that it knows about, so forgetting a member will
 *   result in data loss.
 * - The order in which define the members in the binary_format will specify the exact order in
 *   which these members will be serialized. For example, the member `b` in the example will
 *   always start at offset 4, because it is listed after `a`, which occupies offsets 0 to 3.
 * - All types listed in your binary format must be serializable through this library as well.
 *   This means that they must either either implement their own `get_binary_format()` function
 *   or that they have to be otherwise support by this library, like primitive values, arrays, optional<T>,
 *   variant<T> or entirely custom serialization code.
 * - Changing the binary format of your types will result in binary incompatibilities when reading
 *   files created using an old version of your code. Make sure to have a system in place to handle
 *   changes within your file format.
 */
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

template<typename T, typename... V>
binary_format(V T::*... members)->binary_format<T, V...>;

/// Declare this class as a friend if you wish to keep the `get_binary_format()`
/// function private.
///
/// \relates binary_format
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
/// \relates binary_format
template<typename T>
constexpr bool has_binary_format() {
    return binary_format_access::has_binary_format<T>();
}

/// Returns a binary format instance that describes the type T.
/// Results in a compile-time error if T does not implement the get_binary_format() static function.
///
/// \relates binary_format
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
