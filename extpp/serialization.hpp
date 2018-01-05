#ifndef EXTPP_SERIALIZATION_HPP
#define EXTPP_SERIALIZATION_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/type_traits.hpp>
#include <extpp/detail/iter_tools.hpp>

#include <boost/endian/conversion.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <tuple>
#include <utility>

namespace extpp {

template<typename T>
constexpr size_t serialized_size();

template<typename T>
constexpr size_t serialized_size(T&&);

template<typename T>
byte* serialize(const T& v, byte* buffer);

template<typename T>
const byte* deserialize(T& v, const byte* buffer);

template<typename T, typename V>
class binary_format_field {
public:
    using instance_type = T;
    using field_type = V;

public:
    constexpr binary_format_field(V T::* field)
        : m_field(field)
    {}

    template<typename Instance>
    decltype(auto) get(Instance&& i) const {
        return std::forward<Instance>(i).*m_field;
    }

    constexpr V T::* ptr() const { return m_field; }

private:
    V T::* m_field;
};

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
            v(binary_format_field(ptr));
        });
    }

    /// Returns the number of fields.
    constexpr size_t fields() const {
        return sizeof...(V);
    }

private:
    template<typename Visitor, size_t... I>
    constexpr void visit_fields_impl(Visitor&& v, std::index_sequence<I...>) const {
        (v(binary_format_field(std::get<I>(m_fields))), ...);
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
///         // This function allows the library to 'understand'
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

namespace detail {

template<typename T, typename = void>
struct has_binary_format : std::false_type {};

template<typename T>
struct has_binary_format<T, void_t<decltype(T::get_binary_format())>> : std::true_type {};

template<typename T, typename = void>
struct has_explicit_serializer : std::false_type {};

template<typename T>
struct has_explicit_serializer<T, void_t<typename T::binary_serializer>> : std::true_type {};

template<typename T>
constexpr auto get_binary_format() {
    static_assert(has_binary_format<T>::value,
                  "The type does not implement the get_binary_format() function.");
    return T::get_binary_format();
}


// The default serializer implements binary serialization for fixed size primitive values
// and some common standard types. Arrays, tuples, etc. of serialized types are themselves
// serializable.
template<typename T>
struct default_serializer {
    static_assert(always_false<T>::value,
                  "The specified type cannot be serialized.");
};

template<typename T, size_t size>
struct big_endian_serializer {
    static_assert(sizeof(T) == size,
                  "Unexpected datatype size.");

    static constexpr size_t serialized_size = sizeof(T);

    static void serialize(T v, byte* b) {
        boost::endian::native_to_big_inplace(v);
        std::memcpy(b, &v, sizeof(T));
    }

    static void deserialize(T& v, const byte* b) {
        std::memcpy(&v, b, sizeof(T));
        boost::endian::big_to_native_inplace(v);
    }
};

template<>
struct default_serializer<u8> : big_endian_serializer<u8, 1> {};
template<>
struct default_serializer<u16> : big_endian_serializer<u16, 2> {};
template<>
struct default_serializer<u32> : big_endian_serializer<u32, 4> {};
template<>
struct default_serializer<u64> : big_endian_serializer<u64, 8> {};

// std::intN_t types are specified to use 2s complement.
template<>
struct default_serializer<i8> : big_endian_serializer<i8, 1> {};
template<>
struct default_serializer<i16> : big_endian_serializer<i16, 2> {};
template<>
struct default_serializer<i32> : big_endian_serializer<i32, 4> {};
template<>
struct default_serializer<i64> : big_endian_serializer<i64, 8> {};

template<>
struct default_serializer<bool> {
    static constexpr size_t serialized_size = 1;

    static void serialize(bool v, byte* b) {
        b[0] = v;
    }

    static void deserialize(bool& v, const byte* b) {
        v = b[0];
    }
};

// One-dimensional arrays of static size are serialized by serializing each element.
template<typename T, size_t N>
struct default_serializer<T[N]> {
    static constexpr size_t serialized_size = N * extpp::serialized_size<T>();

    static void serialize(const T (&v)[N], byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = extpp::serialize(v[i], b);
        }
    }

    static void deserialize(T (&v)[N], const byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = extpp::deserialize(v[i], b);
        }
    }
};

template<typename T, size_t N>
struct default_serializer<std::array<T, N>> {
    static constexpr size_t serialized_size = N * extpp::serialized_size<T>();

    static void serialize(const std::array<T, N>& v, byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = extpp::serialize(v[i], b);
        }
    }

    static void deserialize(const std::array<T, N>& v, const byte* b) {
        for (size_t i = 0; i < N; ++i) {
            b = extpp::deserialize(v[i], b);
        }
    }
};

template<typename T1, typename T2>
struct default_serializer<std::pair<T1, T2>> {
    static constexpr size_t serialize_size = extpp::serialized_size<T1>() + extpp::serialized_size<T2>();

    static void serialize(const std::pair<T1, T2>& v, byte* b) {
        b = extpp::serialize(v.first, b);
        extpp::serialize(v.second, b);
    }

    static void deserialize(std::pair<T1, T2>& v, const byte* b) {
        b = extpp::deserialize(v.first, b);
        extpp::deserialize(v.second, b);
    }
};

template<typename... T>
struct default_serializer<std::tuple<T...>> {
    static constexpr size_t serialized_size = (extpp::serialized_size<T>() + ...);

    static void serialize(const std::tuple<T...>& v, byte* b) {
        tuple_for_each(v, [&](const auto& elem) {
            b = extpp::serialize(elem, b);
        });
    }

    static void deserialize(std::tuple<T...>&v, const byte* b) {
        tuple_for_each(v, [&](auto& elem) {
            b = extpp::deserialize(elem, b);
        });
    }
};

// TODO: Specialization for std::variant.

// Serializer for types that provide the get_binary_format function.
// The type is serialized by serializing every member (in the order
// in which they are defined by the function).
template<typename T>
struct binary_format_serializer {
    static constexpr auto format = get_binary_format<T>();

    static constexpr size_t compute_serialized_size() {
        size_t size = 0;
        format.visit_fields([&](auto field) {
            using field_t = decltype(field);
            size += extpp::serialized_size<typename field_t::field_type>();
        });
        return size;
    }

    static constexpr size_t serialized_size = compute_serialized_size();

    static void serialize(const T& v, byte* b) {
        format.visit_fields([&](auto field) {
            const auto& ref = field.get(v);
            b = extpp::serialize(ref, b);
        });
    }

    static void deserialize(T& v, const byte* b) {
        format.visit_fields([&](auto field) {
            auto& ref = field.get(v);
            b = extpp::deserialize(ref, b);
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

    static void serialize(const T& v, byte* b) {
        return impl::serialize(v, b);
    }

    static void deserialize(T& v, const byte* b) {
        return impl::deserialize(v, b);
    }
};

enum class serializer_kind {
    default_serialization,
    binary_format_serialization,
    explicit_serialization,
};

template<typename T>
constexpr auto which_serializer() {
    if constexpr (std::is_class_v<T> || std::is_union_v<T>) {
        if constexpr (has_explicit_serializer<T>::value) {
            return serializer_kind::explicit_serialization;
        } else if (has_binary_format<T>::value) {
            return serializer_kind::binary_format_serialization;
        }
    }
    return serializer_kind::default_serialization;
}

template<typename T, serializer_kind Kind = which_serializer<T>()>
struct select_serializer;

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

} // namespace detail

/// \defgroup serialization Binary Serialization
///
/// TODO Text
///
/// blablabla

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
byte* serialize(const T& v, byte* buffer) {
    detail::serializer_t<T>::serialize(v, buffer);
    return buffer + serialized_size(v);
}

/// Serializes `v` into the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// A debug-mode assertion checks that the buffer is large enough.
///
/// \ingroup serialization
template<typename T>
byte* serialize(const T& v, byte* buffer, size_t buffer_size) {
    EXTPP_ASSERT(buffer_size >= serialized_size(v),
                 "The provided buffer is too small.");
    unused(buffer_size);
    return serialize(v, buffer);
}

/// Deserializes `v` from the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// \ingroup serialization
template<typename T>
const byte* deserialize(T& v, const byte* buffer) {
    detail::serializer_t<T>::deserialize(v, buffer);
    return buffer + serialized_size(v);
}

/// Deserializes `v` from the provided `buffer`, which
/// must be at least `serialized_size(v)` bytes long.
///
/// A debug-mode assertion checks that the buffer is large enough.
///
/// \ingroup serialization
template<typename T>
const byte* deserialize(T& v, const byte* buffer, const size_t buffer_size) {
    EXTPP_ASSERT(buffer_size >= serialized_size(v),
                 "The provided buffer is too small.");
    unused(buffer_size);
    return deserialize(v, buffer);
}

namespace detail {

struct field_offset_t {
    size_t offset = 0;
    bool found = false;
};

template<typename T, typename V>
constexpr field_offset_t serialized_offset_impl(V T::*field) {
    constexpr auto format = detail::get_binary_format<T>();

    field_offset_t result;
    format.visit_fields([&](auto struct_field) {
        using struct_field_type = typename decltype(struct_field)::field_type;

        if (result.found)
            return;

        if constexpr (std::is_same_v<struct_field_type, V>) {
            if (struct_field.ptr() == field) {
                result.found = true;
                return;
            }
        }

        result.offset += serialized_size<struct_field_type>();
    });
    return result;
}

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
///             return make_binary_format(&container::v1, &container::v2, &container::v3);
///         }
///     }
/// \endcode
///
/// Then the following will always be true:
///     - `serialized_offset<&container::v1>() == 0`,
///     - `serialized_offset<&container::v2>() == 1`,
///     - `serialized_offset<&container::v3>() == 5`.
///
/// \ingroup serialization
template<auto MemberPtr>
constexpr size_t serialized_offset() {
    static_assert(std::is_member_object_pointer<decltype(MemberPtr)>::value,
                  "Must pass a member pointer.");
    constexpr auto result = detail::serialized_offset_impl(MemberPtr);
    static_assert(result.found, "The member was not found in the binary format.");
    return result.offset;
}

} // namespace extpp

#endif // EXTPP_SERIALIZATION_HPP
