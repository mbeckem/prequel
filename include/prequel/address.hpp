#ifndef PREQUEL_ADDRESS_HPP
#define PREQUEL_ADDRESS_HPP

#include <prequel/assert.hpp>
#include <prequel/block_index.hpp>
#include <prequel/defs.hpp>
#include <prequel/math.hpp>
#include <prequel/serialization.hpp>
#include <prequel/type_traits.hpp>
#include <prequel/detail/operators.hpp>

#include <iosfwd>
#include <limits>
#include <type_traits>

namespace prequel {

class engine;

class raw_address;

template<typename T>
class address;

template<typename To>
address<To> raw_address_cast(const raw_address& addr);

template<typename To, typename From>
address<To> address_cast(const address<From>& addr);

/// Addresses an arbitrary byte offset in external memory.
class raw_address
        : detail::make_comparable<raw_address>
        , detail::make_addable<raw_address, u64>
        , detail::make_subtractable<raw_address, u64>
{
public:
    static constexpr u64 invalid_value = u64(-1);

public:
    raw_address(): m_value(invalid_value) {}

    explicit raw_address(u64 value)
        : m_value(value) {}

    u64 value() const { return m_value; }

    bool valid() const { return m_value != invalid_value; }

    explicit operator u64() const { return value(); }

    explicit operator bool() const { return valid(); }

    raw_address& operator+=(u64 offset) {
        PREQUEL_ASSERT(valid(), "Invalid address.");
        m_value += offset;
        return *this;
    }

    raw_address& operator-=(u64 offset) {
        PREQUEL_ASSERT(valid(), "Invalid address.");
        m_value -= offset;
        return *this;
    }

    friend bool operator==(const raw_address& lhs, const raw_address& rhs) {
        return lhs.value() == rhs.value();
    }

    friend bool operator<(const raw_address& lhs, const raw_address& rhs) {
        // "+1": The invalid pointer (-1) is the smallest value.
        return (lhs.value() + 1) < (rhs.value() + 1);
    }

    friend std::ostream& operator<<(std::ostream& o, const raw_address& addr);

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_address::m_value);
    }

private:
    u64 m_value;
};

static_assert(sizeof(raw_address) == sizeof(u64), "Requires EBO.");

template<typename T>
class address;

template<typename T>
address<T> raw_address_cast(const raw_address& addr);

/// Addresses a value of type `T` in external memory.
/// The address points to the serialized representation of an instance of type `T`.
template<typename T>
class address
        : detail::make_comparable<address<T>>
        , detail::make_addable<address<T>, u64>
        , detail::make_subtractable<address<T>, u64>
{
public:
    using element_type = T;

public:
    address() = default;

public:
    explicit address(const raw_address& addr)
        : m_raw(addr)
    {}

public:
    bool valid() const { return m_raw.valid(); }

    explicit operator bool() const { return valid(); }

    /// Returns the address of a member of this object.
    ///
    /// Given an address to some object of type `T`,
    /// the function call `addr.member<&T::m>()` obtains
    /// the on-disk address of the member `m`.
    ///
    /// \pre `valid()`.
    template<auto MemberPtr>
    auto member() const {
        using ptr_type = decltype(MemberPtr);

        static_assert(std::is_member_object_pointer_v<ptr_type>,
                      "Must pass a member object pointer.");
        static_assert(std::is_same_v<object_type_t<ptr_type>, T>,
                      "Must pass a member of this type.");

        PREQUEL_ASSERT(valid(), "Invalid pointer.");
        static constexpr u64 offset = serialized_offset<MemberPtr>();
        return address<member_type_t<ptr_type>>(raw() + offset);
    }

    /// Returns the address of this object's parent, i.e. the object that contains
    /// this object.
    ///
    /// Given an address to some object of type `T`,
    /// the function call `addr.instance<&U::t>()` obtains
    /// the on-disk address of the object of type U that contains it.
    ///
    /// The member pointer *must* point to this instance and
    /// there *must* be an object of type `M` that contains this instance.
    /// This cannot be checked at runtime.
    ///
    /// This operation is the reverse of the `member()` function:
    ///
    /// \code{.cpp}
    ///     address<U> a1 = ...;
    ///     address<T> a2 = a1.member<&U::some_field>();    // obtain address to some_field of type U.
    ///     address<U> a3 = a1.instance<&U::some_field>();  // go back to the outer object.
    ///     assert(a1 == a3);
    /// \endcode
    ///
    /// \pre `valid()`.
    template<auto MemberPtr>
    auto parent() const {
        using ptr_type = decltype(MemberPtr);

        static_assert(std::is_member_object_pointer_v<ptr_type>,
                      "Must pass a member object pointer.");
        static_assert(std::is_same_v<member_type_t<ptr_type>, T>,
                      "The member pointer must point to an object of this type.");

        PREQUEL_ASSERT(valid(), "Invalid pointer.");
        static constexpr u64 offset = serialized_offset<MemberPtr>();
        return address<object_type_t<ptr_type>>(raw() - offset);
    }

    const raw_address& raw() const { return m_raw; }

    address& operator+=(u64 offset) {
        m_raw += offset * serialized_size<T>();
        return *this;
    }

    address& operator-=(u64 offset) {
        m_raw -= offset * serialized_size<T>();
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& o, const address& addr) {
        return o << addr.m_raw;
    }

    operator const raw_address&() const { return raw(); }

    friend bool operator==(const address& lhs, const address& rhs) {
        return lhs.m_raw == rhs.m_raw;
    }

    friend bool operator<(const address& lhs, const address& rhs) {
        return lhs.m_raw < rhs.m_raw;
    }

    static constexpr auto get_binary_format() {
        return make_binary_format(&address::m_raw);
    }

private:
    raw_address m_raw;
};

/**
 * Returns the difference (i.e. the signed distance) between `from` and `to`.
 * This is the address equivalent of `to - from`, or the number of bytes one
 * has to advance in order to reach `to` from `from`.
 */
inline i64 difference(const raw_address& from, const raw_address& to) {
    PREQUEL_ASSERT(from, "From address is invalid.");
    PREQUEL_ASSERT(to, "To address is invalid.");
    return signed_difference(to.value(), from.value());
}

/**
 * Returns the difference (i.e. the signed distance) between `from` and `to`.
 * This is the address equivalent of `to - from`, or the number of elements one
 * has to advance in order to reach `to` from `from`.
 */
template<typename T>
i64 difference(const address<T>& from, const address<T>& to) {
    return difference(from.raw(), to.raw()) / serialized_size<T>();
}

/**
 * Returns the absolute distance (in bytes) of `a` and `b`, i.e. `abs(a - b)`
 * in mathematical terms.
 */
inline u64 distance(const raw_address& a, const raw_address& b) {
    PREQUEL_ASSERT(a, "From address is invalid.");
    PREQUEL_ASSERT(b, "To address is invalid.");
    return a <= b ? b.value() - a.value() : a.value() - b.value();
}


/**
 * Returns the absolute distance (in elements) of `a` and `b`, i.e. `abs(a - b)`
 * in mathematical terms.
 */
template<typename T>
u64 distance(const address<T>& from, const address<T>& to) {
    return distance(from.raw(), to.raw()) / serialized_size<T>();
}

/// Performs the equivalent of `reinterpret_cast` to `To*`.
template<typename To>
address<To> raw_address_cast(const raw_address& addr) {
    return address<To>(addr);
}

// Just to aid overload resolution.
template<typename To, typename From>
address<To> raw_address_cast(const address<From>& addr) {
    return raw_address_cast<To>(addr.raw());
}

/// \defgroup linear_io Linear I/O-functions
/// @{

/// Perform a linear write, starting from the given disk address.
/// Will write exactly `size` bytes from `data` to disk to the
/// address range [address, address + size).
void write(engine& e, raw_address address, const void* data, size_t size);

/// Perform a linear read, starting from the given disk address.
/// Will read exactly `size` bytes from the address range [address, address + size)
/// on disk into `data`.
void read(engine& e, raw_address address, void* data, size_t size);

/// Zeroes `size` bytes, starting from the given address.
void zero(engine& e, raw_address address, u64 size);

/// Copies `size` bytes from `src` to `dest`. The two ranges can overlap.
/// \pre `src` and `dest` are valid addresses.
void copy(engine& e, raw_address src, raw_address dest, u64 size);

/// Performs a linear write of the given value at the given disk address.
/// The value must be serializable.
template<typename T>
void write(engine& e, address<T> address, const T& value) {
    serialized_buffer<T> buffer;
    serialize(value, buffer.data(), buffer.size());
    write(e, address.raw(), buffer.data(), buffer.size());
}

/// Performs a linear read of the given value at the given disk address.
/// The value must be serializable.
template<typename T>
void read(engine& e, address<T> address, T& value) {
    serialized_buffer<T> buffer;
    read(e, address.raw(), buffer.data(), buffer.size());
    deserialize(value, buffer.data(), buffer.size());
}

/// @}

} // namespace prequel

#endif // PREQUEL_ADDRESS_HPP
