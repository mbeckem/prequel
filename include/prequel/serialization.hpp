#ifndef PREQUEL_SERIALIZATION_HPP
#define PREQUEL_SERIALIZATION_HPP

#include <prequel/assert.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>
#include <prequel/detail/iter_tools.hpp>
#include <prequel/exception.hpp>
#include <prequel/type_traits.hpp>

#include <boost/endian/conversion.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <optional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>

namespace prequel {

template<typename T>
constexpr size_t serialized_size();

template<typename T>
constexpr size_t serialized_size(T&&);

template<typename T>
void serialize(const T& v, byte* buffer);

template<typename T>
T deserialize(const byte* buffer);

namespace detail {

template<typename T>
byte* serialize_impl(const T& v, byte* buffer);

template<typename T>
const byte* deserialize_impl(T& v, const byte* buffer);

// True if the type is a C-style array
template<typename T>
struct detect_array : std::false_type {};

template<typename T, size_t N>
struct detect_array<T[N]> : std::true_type {
    using value_type = T;
};

// True if the type is a std::array<T, N>
template<typename T>
struct detect_std_array : std::false_type {};

template<typename T, size_t N>
struct detect_std_array<std::array<T, N>> : std::true_type {};

// True if the type is byte-sized.
// This library only works when CHAT_BIT == 8 so char, signed char unsigned char are
// treated the same as u8 and i8.
template<typename T>
constexpr bool is_byte_v =
    std::is_same_v<
        T,
        char> || std::is_same_v<T, signed char> || std::is_same_v<T, unsigned char> || std::is_same_v<T, u8> || std::is_same_v<T, i8>;

// Use the trivial serializer (i.e. memcpy) if type is a byte or made up of bytes (recursively).
template<typename T>
constexpr bool use_trivial_serializer() {
    if constexpr (is_byte_v<T>) {
        return true;
    } else if constexpr (detect_array<T>::value) {
        return use_trivial_serializer<typename detect_array<T>::value_type>();
    } else if constexpr (detect_std_array<T>::value) {
        return use_trivial_serializer<typename T::value_type>();
    } else {
        return false;
    }
}

// The trivial serializer uses memcpy for serialization and deserialization.
template<typename T>
struct trivial_serializer {
    static constexpr size_t serialized_size = sizeof(T);

    PREQUEL_ALWAYS_INLINE
    static void serialize(const T& v, byte* b) { std::memcpy(b, std::addressof(v), sizeof(T)); }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(T& v, const byte* b) { std::memcpy(std::addressof(v), b, sizeof(T)); }
};

template<typename T, typename = void>
struct has_explicit_serializer : std::false_type {};

template<typename T>
struct has_explicit_serializer<T, void_t<typename T::binary_serializer>> : std::true_type {};

// The default serializer implements binary serialization for fixed size primitive values
// and some common standard types. Arrays, tuples, etc. of serialized types are themselves
// serializable.
template<typename T, typename Enable = void>
struct default_serializer : std::false_type {};

// Serialization of numeric types.
template<typename T, size_t size>
struct big_endian_serializer {
    static_assert(sizeof(T) == size, "Unexpected datatype size.");

    static constexpr size_t serialized_size = sizeof(T);

    PREQUEL_ALWAYS_INLINE
    PREQUEL_FLATTEN
    static void serialize(T v, byte* b) {
        boost::endian::native_to_big_inplace(v);
        std::memcpy(b, &v, sizeof(T));
    }

    PREQUEL_ALWAYS_INLINE
    PREQUEL_FLATTEN
    static void deserialize(T& v, const byte* b) {
        std::memcpy(&v, b, sizeof(T));
        boost::endian::big_to_native_inplace(v);
    }
};

template<>
struct default_serializer<u16> : big_endian_serializer<u16, 2> {};
template<>
struct default_serializer<u32> : big_endian_serializer<u32, 4> {};
template<>
struct default_serializer<u64> : big_endian_serializer<u64, 8> {};

// std::intN_t types are specified to use 2s complement.
template<>
struct default_serializer<i16> : big_endian_serializer<i16, 2> {};
template<>
struct default_serializer<i32> : big_endian_serializer<i32, 4> {};
template<>
struct default_serializer<i64> : big_endian_serializer<i64, 8> {};

// Endianess for floating points is a complete mess if one wants to be
// truly cross-platform. There are apparently machine that have different
// integer and float endianess rules. IEEE 754 does not specify the endianness
// of floating point numbers. The following code only works on platforms
// where the float endianess is the same as the integer endianess (which is the
// rule in modern systems).
//
// This class serializes floating point numbers by copying them into an unsigned value
// of the same size and then performing the appropriate serilization on the unsigned value.
// For example, a float is copied into a u32 which in turn gets serialized as a big endian value.
template<typename T, typename U>
struct float_serializer {
    static_assert(sizeof(T) == sizeof(U), "Unexpected size of floating point type.");
    static_assert(std::numeric_limits<T>::is_iec559,
                  "The floating point type must conform to IEEE 754.");

    static constexpr size_t serialized_size = sizeof(T);

    PREQUEL_ALWAYS_INLINE
    PREQUEL_FLATTEN
    static void serialize(T value, byte* b) {
        U num;
        std::memcpy(&num, &value, sizeof(T));
        serialize_impl(num, b);
    }

    PREQUEL_ALWAYS_INLINE
    PREQUEL_FLATTEN
    static void deserialize(T& value, const byte* b) {
        U num;
        deserialize_impl(num, b);
        std::memcpy(&value, &num, sizeof(T));
    }
};

template<>
struct default_serializer<float> : float_serializer<float, u32> {};
template<>
struct default_serializer<double> : float_serializer<double, u64> {};

template<>
struct default_serializer<bool> {
    static constexpr size_t serialized_size = 1;

    PREQUEL_ALWAYS_INLINE
    static void serialize(bool v, byte* b) { b[0] = v; }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(bool& v, const byte* b) { v = b[0]; }
};

// One-dimensional arrays of static size are serialized by serializing each element.
template<typename T, size_t N>
struct default_serializer<T[N]> {
    static constexpr size_t serialized_size = N * prequel::serialized_size<T>();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const T (&v)[N], byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = serialize_impl(v[i], b);
        }
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(T (&v)[N], const byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = deserialize_impl(v[i], b);
        }
    }
};

// std::arrays use the same serilization format as c style arrays.
template<typename T, size_t N>
struct default_serializer<std::array<T, N>> {
    static constexpr size_t serialized_size = N * prequel::serialized_size<T>();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const std::array<T, N>& v, byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = serialize_impl(v[i], b);
        }
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(std::array<T, N>& v, const byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = deserialize_impl(v[i], b);
        }
    }
};

// pair<T1, T2> is serialized by serializing the first, then the second value.
template<typename T1, typename T2>
struct default_serializer<std::pair<T1, T2>> {
    static constexpr size_t serialized_size =
        prequel::serialized_size<T1>() + prequel::serialized_size<T2>();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const std::pair<T1, T2>& v, byte* b) {
        b = serialize_impl(v.first, b);
        serialize_impl(v.second, b);
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(std::pair<T1, T2>& v, const byte* b) {
        b = deserialize_impl(v.first, b);
        deserialize_impl(v.second, b);
    }
};

// tuple serialization is implemented by serializing the types in the order in which they appear
// in the tuple declaration.
template<typename... T>
struct default_serializer<std::tuple<T...>> {
    static constexpr size_t serialized_size = (prequel::serialized_size<T>() + ...);

    PREQUEL_ALWAYS_INLINE
    static void serialize(const std::tuple<T...>& v, byte* b) {
        tuple_for_each(v, [&](const auto& elem) { b = serialize_impl(elem, b); });
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(std::tuple<T...>& v, const byte* b) {
        tuple_for_each(v, [&](auto& elem) { b = deserialize_impl(elem, b); });
    }
};

// Returns the size of the largest type.
template<typename... T>
constexpr size_t max_size() {
    size_t max = 0;
    size_t values[] = {sizeof(T)...};
    for (size_t v : values) {
        if (v > max)
            max = v;
    }
    return max;
}

// Optionals contain a single byte (1 or 0) that tells us whether they contain a value or not,
// followed by the serialized value or zeroes, if there is no value.
template<typename T>
struct default_serializer<std::optional<T>> {
    static constexpr size_t serialized_size = 1 + prequel::serialized_size<T>();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const std::optional<T>& v, byte* b) {
        if (v) {
            b[0] = 1;
            serialize_impl(*v, b + 1);
        } else {
            std::memset(b, 0, serialized_size);
        }
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(std::optional<T>& v, const byte* b) {
        if (b[0] == 0) {
            v = std::nullopt;
        } else if (b[0] == 1) {
            T value;
            deserialize_impl(value, b + 1);
            v = std::move(value);
        } else {
            PREQUEL_THROW(corruption_error(
                fmt::format("Invalid has_value flag (expected 0 or 1): {}", (int) b[0])));
        }
    }
};

// Variants contain a single byte tag at the start that indicates the kind of value they carry.
// The rest of the storage must be interpreted according to that tag.
// For example, a std::variant<int, double, bool> that carries a double will have the tag `1` (zero indexed)
// and the following bytes will contain the serialized double value.
template<typename... T>
struct default_serializer<std::variant<T...>> {
    static constexpr size_t alternatives = sizeof...(T);

    static constexpr size_t max_alternatives = 16;

    static_assert(alternatives <= max_alternatives, "Too many types in this variant");

    static constexpr size_t serialized_size = 1 + max_size<T...>();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const std::variant<T...>& v, byte* b) {
        if (v.valueless_by_exception())
            PREQUEL_THROW(bad_argument("Cannot serialize a valueless_by_exception variant."));

        // Remember for the zeroing at the end.
        byte* const end = b + serialized_size;

        // Serialize the `which` tag, then the actual value.
        const byte which = static_cast<byte>(v.index());
        b[0] = which;
        ++b;

        switch (which) {
// Don't make the compiler generate a god damn function pointer table for this.
#define PREQUEL_DETAIL_VARIANT_CASE(i)         \
case i: {                                      \
    if constexpr (i < alternatives) {          \
        b = serialize_impl(std::get<i>(v), b); \
        break;                                 \
    }                                          \
}

            PREQUEL_DETAIL_VARIANT_CASE(0)
            PREQUEL_DETAIL_VARIANT_CASE(1)
            PREQUEL_DETAIL_VARIANT_CASE(2)
            PREQUEL_DETAIL_VARIANT_CASE(3)
            PREQUEL_DETAIL_VARIANT_CASE(4)
            PREQUEL_DETAIL_VARIANT_CASE(5)
            PREQUEL_DETAIL_VARIANT_CASE(6)
            PREQUEL_DETAIL_VARIANT_CASE(7)
            PREQUEL_DETAIL_VARIANT_CASE(8)
            PREQUEL_DETAIL_VARIANT_CASE(9)
            PREQUEL_DETAIL_VARIANT_CASE(10)
            PREQUEL_DETAIL_VARIANT_CASE(11)
            PREQUEL_DETAIL_VARIANT_CASE(12)
            PREQUEL_DETAIL_VARIANT_CASE(13)
            PREQUEL_DETAIL_VARIANT_CASE(14)
            PREQUEL_DETAIL_VARIANT_CASE(15)

#undef PREQUEL_DETAIL_VARIANT_CASE
        }

        // Zero the remainder of the variant.
        std::memset(b, 0, static_cast<size_t>(end - b));
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(std::variant<T...>& v, const byte* b) {
        const byte which = b[0];
        ++b;

        if (which >= alternatives) {
            PREQUEL_THROW(corruption_error(
                fmt::format("Invalid value for variant alternative index: {}", which)));
        }

        switch (which) {
// Switch case for the type at the given index. The constexpr-if hides the body
// of the if statement if the current variant type does not have that many alternatives.
// This approach is better than a function table generated at compile time because
// it (probably) does not cause excessive binary bloat.
#define PREQUEL_DETAIL_VARIANT_CASE(i)                           \
case i: {                                                        \
    if constexpr (i < alternatives) {                            \
        std::variant_alternative_t<i, std::variant<T...>> value; \
        deserialize_impl(value, b);                              \
        v.template emplace<i>(std::move(value));                 \
        break;                                                   \
    }                                                            \
};

            PREQUEL_DETAIL_VARIANT_CASE(0)
            PREQUEL_DETAIL_VARIANT_CASE(1)
            PREQUEL_DETAIL_VARIANT_CASE(2)
            PREQUEL_DETAIL_VARIANT_CASE(3)
            PREQUEL_DETAIL_VARIANT_CASE(4)
            PREQUEL_DETAIL_VARIANT_CASE(5)
            PREQUEL_DETAIL_VARIANT_CASE(6)
            PREQUEL_DETAIL_VARIANT_CASE(7)
            PREQUEL_DETAIL_VARIANT_CASE(8)
            PREQUEL_DETAIL_VARIANT_CASE(9)
            PREQUEL_DETAIL_VARIANT_CASE(10)
            PREQUEL_DETAIL_VARIANT_CASE(11)
            PREQUEL_DETAIL_VARIANT_CASE(12)
            PREQUEL_DETAIL_VARIANT_CASE(13)
            PREQUEL_DETAIL_VARIANT_CASE(14)
            PREQUEL_DETAIL_VARIANT_CASE(15)

#undef PREQUEL_DETAIL_VARIANT_CASE
        }
    }
};

template<>
struct default_serializer<std::monostate> {
    static constexpr size_t serialized_size = 0;

    static void serialize(const std::monostate&, byte*) {}
    static void deserialize(std::monostate&, const byte*) {}
};

template<typename T, typename U, typename V>
decltype(auto) get_member(T&& instance, V U::*member) {
    return std::forward<T>(instance).*member;
}

// Serializer for types that provide the get_binary_format function.
// The type is serialized by serializing every member (in the order
// in which they are defined by the function).
template<typename T>
struct binary_format_serializer {
    static constexpr auto format = get_binary_format<T>();

    static constexpr size_t compute_serialized_size() {
        size_t size = 0;

        detail::tuple_for_each(format.fields(), [&](auto field) {
            size += prequel::serialized_size<member_type_t<decltype(field)>>();
        });
        return size;
    }

    static constexpr size_t serialized_size = compute_serialized_size();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const T& v, byte* b) {
        detail::tuple_for_each(format.fields(), [&](auto field) PREQUEL_ALWAYS_INLINE {
            const auto& ref = get_member(v, field);
            b = serialize_impl(ref, b);
        });
    }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(T& v, const byte* b) {
        detail::tuple_for_each(format.fields(), [&](auto field) PREQUEL_ALWAYS_INLINE {
            auto& ref = get_member(v, field);
            b = deserialize_impl(ref, b);
        });
    }
};

// Serializer for types that provide their own binary_serializer type.
// All calls are forwarded to the custom implementation.
template<typename T>
struct explicit_serializer {
    using impl = typename T::binary_serializer;

    // We use a function call here because function local classes
    // cannot define static data members (even if they are constexpr...)
    static constexpr size_t serialized_size = impl::serialized_size();

    PREQUEL_ALWAYS_INLINE
    static void serialize(const T& v, byte* b) { return impl::serialize(v, b); }

    PREQUEL_ALWAYS_INLINE
    static void deserialize(T& v, const byte* b) { return impl::deserialize(v, b); }
};

enum class serializer_kind {
    // Trivial serialization is implemented as memcpy for types such as byte arrays.
    trivial_serialization,

    // Default serialization implements serialialization for builin primitive types
    // and some fixed size std containers (e.g. variant, array).
    default_serialization,

    // Automatic serialization for classes that provide a get_binary_format() function.
    binary_format_serialization,

    // Serialization for classes that have their own serialization implementation.
    explicit_serialization,
};

template<typename T>
constexpr auto which_serializer() {
    if constexpr (use_trivial_serializer<T>()) {
        return serializer_kind::trivial_serialization;
    } else if constexpr (!std::is_base_of_v<std::false_type, default_serializer<T>>) {
        return serializer_kind::default_serialization;
    } else {
        if constexpr (has_explicit_serializer<T>::value) {
            return serializer_kind::explicit_serialization;
        } else if constexpr (has_binary_format<T>()) {
            return serializer_kind::binary_format_serialization;
        } else {
            static_assert(
                always_false<T>::value,
                "The type cannot be serialized. You must either implement get_binary_format() or "
                "explicit serialization protocols.");
        }
    }
}

/*
 * Select the appropriate serializer implementation for the given type,
 * based on the return value of the which_serializer() function.
 */
template<typename T, serializer_kind Kind = which_serializer<T>()>
struct select_serializer;

template<typename T>
struct select_serializer<T, serializer_kind::trivial_serialization> {
    using type = trivial_serializer<T>;
};

template<typename T>
struct select_serializer<T, serializer_kind::default_serialization> {
    using type = default_serializer<T>;
};

template<typename T>
struct select_serializer<T, serializer_kind::binary_format_serialization> {
    using type = binary_format_serializer<T>;
};

template<typename T>
struct select_serializer<T, serializer_kind::explicit_serialization> {
    using type = explicit_serializer<T>;
};

template<typename T>
using serializer_t = typename select_serializer<remove_cvref_t<T>>::type;

template<typename T>
inline byte* serialize_impl(const T& v, byte* buffer) {
    detail::serializer_t<T>::serialize(v, buffer);
    return buffer + serialized_size(v);
}

template<typename T>
inline const byte* deserialize_impl(T& v, const byte* buffer) {
    serializer_t<T>::deserialize(v, buffer);
    return buffer + serialized_size(v);
}

} // namespace detail

/// \defgroup serialization Binary Serialization
///
/// Functions for serialization and deserialization of values.
/// This library establishes a bi-directional mapping between structs in memory
/// and in secondary storage. Instances are serialized from their usual in-RAM representation
/// into byte buffers of fixed size (in big endian format) using the \ref serialize
/// function or related helper functions.
/// Values can be read back using the \ref deserialize function (and its associated helpers).
///
/// Every supported type `T` has a \ref serialized_size (a constexpr value)
/// that is similar to `sizeof(T)`, it is the size of that struct after it has been serialized.
///
/// The following types are supported by the (de-) serialization library:
///     - Primitive values of fixed size (i.e. uintX_t, intX_t, char types and bool)
///     - User defined classes that implement the \ref get_binary_format protocol
///     - Types that implement their own custom (de-) serialization (TODO describe explicit_serializer)
///     - Standard aggregate or fixed-size containers like T[N], std::array<T, N>, std::tuple or std::variant
///       of types that are themselves supported (e.g., uint32_t[4] is supported because uint32_t is supported).
///
/// Note that these functions are used as a low level building block for fixed-size data types. If you need
/// to model variable size data you should use one of the container classes provided by this library or
/// implement your own one.

/// Returns the exact size of the serialized representation of `T`.
/// This function can run at compile-time.
///
/// \ingroup serialization
template<typename T>
constexpr size_t serialized_size() {
    return detail::serializer_t<T>::serialized_size;
}

/// Equivalent to `serialized_size<T>()`. The argument is never used
/// and is just present to make typing easier.
///
/// \ingroup serialization
template<typename T>
constexpr size_t serialized_size(T&&) {
    return serialized_size<T>();
}

/// Serializes `v` into the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// \ingroup serialization
template<typename T>
PREQUEL_ALWAYS_INLINE inline void serialize(const T& v, byte* buffer) {
    detail::serialize_impl(v, buffer);
}

/// Serializes `v` into the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// A debug-mode assertion checks that the buffer is large enough.
///
/// \ingroup serialization
template<typename T>
PREQUEL_ALWAYS_INLINE inline void serialize(const T& v, byte* buffer, size_t buffer_size) {
    PREQUEL_ASSERT(buffer_size >= serialized_size(v), "The provided buffer is too small.");
    unused(buffer_size);
    serialize(v, buffer);
}

/**
 * Deserialization functions have to construct a valid instance of an object
 * in order to fill its state, even if the object type is not default constructible.
 *
 * Non default-constructible classes will be passed an instance of this class.
 * They should construct themselves into an arbitrary state - they will be overwritten
 * by the deserialized values immediately, anyway.
 *
 * Example:
 *      struct example {
 *          // non default-constructible for normal code, for whatever reason
 *          example() = delete;
 *
 *          // the other object follows isn't default constructible either, but
 *          // follows the same convention. just forward the tag.
 *          example(prequel::serialization_tag t)
 *              : other(t)
 *          {}
 *
 *
 *          int x = 0;
 *          other_non_default_constructible_class other;
 *      };
 *
 *      // Given a byte buffer that contains a serialized object:
 *      const byte* buffer = ...;
 *
 *      // We can still obtain the deserialized value.
 *      example ex = deserialize<example>(buffer);
 */
class deserialization_tag {
private:
    template<typename T>
    friend T deserialize(const byte* buffer);

    deserialization_tag() = default;
};

/// Deserializes `v` from the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// \ingroup serialization
template<typename T>
inline T deserialize(const byte* buffer) {
    if constexpr (std::is_default_constructible_v<T>) {
        T v;
        detail::deserialize_impl(v, buffer);
        return v;
    } else if constexpr (std::is_constructible_v<T, deserialization_tag>) {
        T v(deserialization_tag{});
        detail::deserialize_impl(v, buffer);
        return v;
    } else {
        static_assert(always_false<T>::value,
                      "The type must be either default-constructible or constructible using "
                      "a deserialization tag.");
    }
}

/// Deserializes `v` from the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// A debug-mode assertion checks that the buffer is large enough.
///
/// \ingroup serialization
template<typename T>
inline T deserialize(const byte* buffer, const size_t buffer_size) {
    PREQUEL_ASSERT(buffer_size >= serialized_size<T>(), "The provided buffer is too small.");
    unused(buffer_size);
    return deserialize<T>(buffer);
}

/// A stack allocated buffer large enough to hold the serialized representation
/// of values of type T.
///
/// \ingroup serialization
template<typename T>
using serialized_buffer = std::array<byte, serialized_size<T>()>;

/// Serializes `instance` into a stack-allocated buffer.
/// \ingroup serialization
template<typename T>
serialized_buffer<T> serialize_to_buffer(const T& instance) {
    serialized_buffer<T> buffer;
    serialize(instance, buffer.data(), buffer.size());
    return buffer;
}

/// Deserializes an instance of type `T` from the stack-allocated buffer.
/// \ingroup serialization
template<typename T>
T deserialize_from_buffer(const serialized_buffer<T>& buffer) {
    return deserialize<T>(buffer.data());
}

namespace detail {

struct field_offset_t {
    size_t offset = 0;
    bool found = false;
};

template<typename T, typename V>
constexpr field_offset_t serialized_offset_impl(V T::*field) {
    constexpr auto format = get_binary_format<T>();

    field_offset_t result;
    detail::tuple_for_each(format.fields(), [&](auto member_ptr) {
        using struct_field_type = member_type_t<decltype(member_ptr)>;

        if (result.found)
            return;

        if constexpr (std::is_same_v<struct_field_type, V>) {
            if (member_ptr == field) {
                result.found = true;
                return;
            }
        }

        result.offset += serialized_size<struct_field_type>();
    });
    return result;
}

// The member type of the pointer must be the same as the object type of the next pointer.
template<typename ObjectType, auto MemberPointer, auto... MemberPointers>
constexpr size_t serialized_offset_recursive() {
    using object_type = object_type_t<decltype(MemberPointer)>;
    using member_type = member_type_t<decltype(MemberPointer)>;
    static_assert(std::is_same_v<ObjectType, object_type>,
                  "Member pointers must point to members of their parent object.");

    constexpr field_offset_t result = serialized_offset_impl(MemberPointer);
    static_assert(result.found, "The member was not found in the object's binary format.");

    if constexpr (sizeof...(MemberPointers) > 0) {
        return result.offset + serialized_offset_recursive<member_type, MemberPointers...>();
    } else {
        return result.offset;
    }
}

template<typename... T>
constexpr auto select_last_member_ptr(T... ptrs) {
    std::tuple<T...> args(ptrs...);
    return std::get<std::tuple_size_v<decltype(args)> - 1>(args);
}

template<auto... MemberPtrs>
using member_value_type = member_type_t<decltype(select_last_member_ptr(MemberPtrs...))>;

} // namespace detail

/// Returns the byte-offset of the serialized representation of `field`
/// within the serialized representation of an instance of type `T`, i.e.
/// the object that contains it.
/// The byte offset is the sum of the serialized members that preceed
/// the field in `T`. There are no padding bytes on disk.
/// In order for this to work, the type T must implement get `get_binary_format()`
/// function.
///
/// Consider a struct defined like this:
///
/// \code{.cpp}
///     struct container {
///         u8  v1;
///         u32 v2;
///         u8  v3;
///
///         static constexpr auto get_binary_format() {
///             return binary_format(&container::v1, &container::v2, &container::v3);
///         }
///     }
/// \endcode
///
/// Then the following will always be true:
///     - `serialized_offset<&container::v1>() == 0`,
///     - `serialized_offset<&container::v2>() == 1`,
///     - `serialized_offset<&container::v3>() == 5`.
///
/// \todo we need a similar function for arrays
///
/// \ingroup serialization
template<auto MemberPtr, auto... MemberPtrs>
constexpr size_t serialized_offset() {
    static_assert(std::is_member_object_pointer<decltype(MemberPtr)>::value
                      && (std::is_member_object_pointer<decltype(MemberPtrs)>::value && ...),
                  "Must pass members pointers.");
    using object_type = object_type_t<decltype(MemberPtr)>;
    return detail::serialized_offset_recursive<object_type, MemberPtr, MemberPtrs...>();
}

/*
 * Deserializes only the specified member of the outermost struct.
 * The buffer must be large enough to hold that structure.
 */
template<auto MemberPtr, auto... MemberPtrs>
auto deserialize_member(const byte* buffer) {
    static constexpr size_t offset = serialized_offset<MemberPtr, MemberPtrs...>();
    using member_type = detail::member_value_type<MemberPtr, MemberPtrs...>;

    static constexpr size_t object_size = serialized_size<object_type_t<decltype(MemberPtr)>>();
    unused(object_size);
    PREQUEL_ASSERT(offset <= object_size - serialized_size<member_type>(),
                   "Member offset out of bounds.");

    return deserialize<member_type>(buffer + offset);
}

/*
 * Deserializes only the specified member of the outermost struct.
 * The buffer must be large enough to hold that structure.
 */
template<auto MemberPtr, auto... MemberPtrs>
auto deserialize_member(const byte* buffer, size_t buffer_size) {
    PREQUEL_ASSERT(buffer_size >= serialized_size<object_type_t<decltype(MemberPtr)>>(),
                   "Buffer is too small for that type of object.");
    unused(buffer_size);
    return deserialize_member<MemberPtr, MemberPtrs...>(buffer);
}

/*
 * Serializes only the specified member of the outermost struct.
 * The buffer must be large enough to hold that structure.
 */
template<auto MemberPtr, auto... MemberPtrs>
void serialize_member(const detail::member_value_type<MemberPtr, MemberPtrs...>& v, byte* buffer) {
    static constexpr size_t offset = serialized_offset<MemberPtr, MemberPtrs...>();
    static constexpr size_t object_size = serialized_size<object_type_t<decltype(MemberPtr)>>();
    unused(object_size);
    PREQUEL_ASSERT(offset <= object_size - serialized_size<decltype(v)>(),
                   "Member offset out of bounds.");

    serialize(v, buffer + offset);
}

/*
 * Serializes only the specified member of the outermost struct.
 * The buffer must be large enough to hold that structure.
 */
template<auto MemberPtr, auto... MemberPtrs>
auto serialize_member(const detail::member_value_type<MemberPtr, MemberPtrs...>& v, byte* buffer,
                      size_t buffer_size) {
    PREQUEL_ASSERT(buffer_size >= serialized_size<object_type_t<decltype(MemberPtr)>>(),
                   "Buffer is too small for that type of object.");
    unused(buffer_size);
    return serialize_member<MemberPtr, MemberPtrs...>(v, buffer);
}

} // namespace prequel

#endif // PREQUEL_SERIALIZATION_HPP
