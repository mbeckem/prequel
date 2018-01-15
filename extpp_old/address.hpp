#ifndef EXTPP_ADDRESS_HPP
#define EXTPP_ADDRESS_HPP

#include <extpp/assert.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/type_traits.hpp>
#include <extpp/detail/memory.hpp>
#include <extpp/detail/operators.hpp>

#include <ostream>
#include <limits>
#include <type_traits>

namespace extpp {

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
        , detail::make_addable<raw_address, i64>
        , detail::make_subtractable<raw_address, i64>
{
    struct byte_addr_t {};

public:
    static constexpr u64 invalid_value = u64(-1);

    static raw_address block_address(block_index block, u32 block_size) {
        return block ? raw_address(checked_mul<u64>(block.value(), block_size))
                     : raw_address();
    }

    static raw_address byte_address(u64 address) {
        return raw_address(address);
    }

public:
    raw_address(): m_value(invalid_value) {}

    explicit raw_address(u64 value)
        : m_value(value) {}

    u64 value() const { return m_value; }

    bool valid() const { return m_value != invalid_value; }

    explicit operator u64() const { return value(); }

    explicit operator bool() const { return valid(); }

    block_index get_block_index(u32 block_size) const {
        return valid() ? block_index(value() / block_size) : block_index();
    }

    u32 get_offset_in_block(u32 block_size) const {
        return valid() ? mod_pow2<u64>(value(), block_size) : 0;
    }

    raw_address& operator+=(i64 offset) {
        EXTPP_ASSERT(valid(), "Invalid address.");
        m_value += offset;
        return *this;
    }

    raw_address& operator-=(i64 offset) {
        EXTPP_ASSERT(valid(), "Invalid address.");
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

    friend std::ostream& operator<<(std::ostream& o, const raw_address& addr) {
        if (!addr)
            return o << "INVALID";
        return o << addr.value();
    }

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_address::m_value);
    }

private:
    u64 m_value;
};

template<typename T>
class address;

template<typename T>
address<T> raw_address_cast(const raw_address& addr);

template<typename To, typename From>
address<To> address_cast(const address<From>& addr);

/// Addresses a value of type `T` in external memory.
template<typename T>
class address
        : detail::make_comparable<address<T>>
        , detail::make_addable<address<T>, i64>
        , detail::make_subtractable<address<T>, i64>
{
public:
    using element_type = T;

public:
    address() = default;

public:
    explicit address(const raw_address& addr)
        : m_raw(addr)
    {
        check_aligned();
    }

public:
    bool valid() const { return m_raw.valid(); }

    explicit operator bool() const { return valid(); }

    const raw_address& raw() const { return m_raw; }

    address& operator+=(i64 offset) {
        m_raw += offset * sizeof(T);
        check_aligned();
        return *this;
    }

    address& operator-=(i64 offset) {
        m_raw -= offset * sizeof(T);
        check_aligned();
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& o, const address& addr) {
        // TODO: Include the type name of T.
        return o << addr.m_raw;
    }

    /// Addresses are convertible to base classes by default.
    template<typename Base, std::enable_if_t<std::is_base_of<Base, T>::value>* = nullptr>
    operator address<Base>() const {
        return address_cast<Base>(*this);
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
    void check_aligned() {
        EXTPP_ASSERT(!raw().valid() || is_aligned(raw().value(), alignof(T)),
                     "The address must be either invalid or properly aligned.");
    }

private:
    raw_address m_raw;
};

inline i64 difference(const raw_address& from, const raw_address& to) {
    EXTPP_ASSERT(from, "From address is invalid.");
    EXTPP_ASSERT(to, "To address is invalid.");
    return signed_difference(to.value(), from.value());
}

template<typename T>
i64 difference(const address<T>& from, const address<T>& to) {
    return distance(from.raw(), to.raw()) / sizeof(T);
}

inline u64 distance(const raw_address& from, const raw_address& to) {
    EXTPP_ASSERT(from, "From address is invalid.");
    EXTPP_ASSERT(to, "To address is invalid.");
    return from <= to ? to.value() - from.value() : from.value() - to.value();
}

/// \pre `from <= to`.
template<typename T>
u64 distance(const address<T>& from, const address<T>& to) {
    return distance(from.raw(), to.raw()) / sizeof(T);
}

/// Performs the equivalent of `reinterpret_cast` to `To*`.
template<typename To>
address<To> raw_address_cast(const raw_address& addr) {
    static_assert(is_trivial<To>::value, "Only trivial types are supported in external memory.");
    return address<To>(addr);
}

// Just to aid overload resolution.
template<typename To, typename From>
address<To> raw_address_cast(const address<From>& addr) {
    return raw_address_cast<To>(addr.raw());
}

/// Performs the equivalent of a `static_cast` from `From*` to `To*`.
/// Such a cast is only possible if From and To form an inheritance
/// relationship, i.e. From is a base of To or the other way around.
/// The raw pointer value will be adjusted automatically to point to the correct address.
template<typename To, typename From>
address<To> address_cast(const address<From>& addr) {
    static_assert(std::is_base_of<To, From>::value || std::is_base_of<From, To>::value,
                  "Addresses to objects of type From cannot be statically converted to To.");
    if (!addr)
        return {};
    if constexpr (std::is_base_of<To, From>::value) {
        return raw_address_cast<To>(addr.raw() + detail::offset_of_base<From, To>());
    } else {
        return raw_address_cast<To>(addr.raw() - detail::offset_of_base<To, From>());
    }
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
void copy(engine& e, raw_address dest, raw_address src, u64 size);

/// @}

} // namespace extpp

#endif // EXTPP_ADDRESS_HPP
