#ifndef EXTPP_ADDRESS_HPP
#define EXTPP_ADDRESS_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/type_traits.hpp>

#include <extpp/detail/operators.hpp>

#include <ostream>
#include <limits>
#include <type_traits>

namespace extpp {

namespace detail {

// We need a dummy object (not initialized), should be optimized out.
template<typename T> struct ptr_adjust_holder { static T value; };

// This code uses old-style compile time programming by design to get
// around constexpr restrictions (i.e. forbidden casts at compile time).
// Base must be a (non-virtual) base class of Derived.
//
// Inspired by the blog post at
// https://thecppzoo.blogspot.de/2016/10/constexpr-offsetof-practical-way-to.html
template<typename Derived, typename Base>
struct ptr_adjust_helper {
    using holder = ptr_adjust_holder<Derived>;

    enum {
        // The pointer offset for Derived -> Base casts.
        // Note: this does not work with MSVC.
        value = (char *)((Base *) &holder::value) - (char *) &holder::value
    };

// This technique doesn't seem to be required.
//    char for_sizeof[
//        (char *)((Base *) &holder::value) -
//        (char *)&holder::value
//    ];
// later:
//    sizeof(...::for_sizeof).
};

template<typename Derived, typename Base>
constexpr ptrdiff_t ptr_adjust() {
    return ptr_adjust_helper<Derived, Base>::value;
}

} // namespace detail

template<u32 BlockSize>
class raw_address;

template<typename T, u32 BlockSize>
class address;

template<typename T, u32 BlockSize>
address<T, BlockSize> address_cast(const raw_address<BlockSize>& addr);

/// Addresses an arbitrary byte offset in external memory.
template<u32 BlockSize>
class raw_address
        : detail::make_comparable<raw_address<BlockSize>>
        , detail::make_addable<raw_address<BlockSize>, i64>
        , detail::make_subtractable<raw_address<BlockSize>, i64>
{
    static_assert(is_pow2(BlockSize), "BlockSize must be a power of two.");

    struct byte_addr_t {};

public:
    static constexpr u32 block_size = BlockSize;
    static constexpr u32 block_size_log2 = log2(block_size);
    static constexpr u64 invalid_value = u64(-1);

    static raw_address from_block(u64 block_index) {
        return raw_address(block_index, 0);
    }

    static raw_address byte_address(u64 index) {
        return raw_address(byte_addr_t(), index);
    }

public:
    raw_address(): m_value(invalid_value) {}

    raw_address(u64 block, u32 offset)
        : m_value(block * BlockSize + offset)
    {
        EXTPP_CHECK(offset < BlockSize, "Invalid offset within a block.");
    }

    u64 value() const { return m_value; }

    bool valid() const { return m_value != invalid_value; }

    explicit operator u64() const { return value(); }

    explicit operator bool() const { return valid(); }

    u64 block_index() const {
        EXTPP_ASSERT(valid(), "Invalid address.");
        return value() >> block_size_log2;
    }

    u32 block_offset() const {
        EXTPP_ASSERT(valid(), "Invalid address.");
        return mod_pow2(value(), u64(BlockSize));
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
        return lhs.value() + 1 < rhs.value() + 1;
    }

    friend std::ostream& operator<<(std::ostream& o, const raw_address& addr) {
        if (!addr)
            return o << "INVALID";
        return o << "(" << addr.block_index() << ", " << addr.block_offset() << ")";
    }

private:
    explicit raw_address(byte_addr_t, u64 value): m_value(value) {}

private:
    u64 m_value;
};

template<typename T, u32 BlockSize>
class address;

template<typename T, u32 BlockSize>
address<T, BlockSize> address_cast(const raw_address<BlockSize>& addr);

template<typename To, typename From, u32 BlockSize>
address<To, BlockSize> address_cast(const address<From, BlockSize>& addr);

/// Addresses a value of type `T` in external memory.
template<typename T, u32 BlockSize>
class address
        : detail::make_comparable<address<T, BlockSize>>
        , detail::make_addable<address<T, BlockSize>, i64>
        , detail::make_subtractable<address<T, BlockSize>, i64>
{
public:
    using element_type = T;

public:
    address() = default;

public:
    explicit address(const raw_address<BlockSize>& addr)
        : m_raw(addr)
    {
        check_aligned();
    }

public:
    bool valid() const { return m_raw.valid(); }

    explicit operator bool() const { return valid(); }

    const raw_address<BlockSize>& raw() const { return m_raw; }

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
    operator address<Base, BlockSize>() const {
        return address_cast<Base>(*this);
    }

    operator raw_address<BlockSize>() const { return raw(); }

    friend bool operator==(const address& lhs, const address& rhs) {
        return lhs.m_raw == rhs.m_raw;
    }

    friend bool operator<(const address& lhs, const address& rhs) {
        return lhs.m_raw < rhs.m_raw;
    }

private:
    void check_aligned() {
        EXTPP_ASSERT(!raw().valid() || is_aligned(raw().value(), alignof(T)),
                     "The address must be either invalid or properly aligned.");
    }

private:
    raw_address<BlockSize> m_raw;
};

template<u32 BlockSize>
i64 distance(const raw_address<BlockSize>& from, const raw_address<BlockSize>& to) {
    EXTPP_ASSERT(from, "From address is invalid.");
    EXTPP_ASSERT(to, "To address is invalid.");
    return signed_difference(to.value(), from.value());
}

template<typename T, u32 BlockSize>
i64 distance(const address<T, BlockSize>& from, const address<T, BlockSize>& to) {
    return distance(from.raw(), to.raw()) / sizeof(T);
}

/// Performs the equivalent of `reinterpret_cast` to `To*`.
template<typename To, u32 BlockSize>
address<To, BlockSize> address_cast(const raw_address<BlockSize>& addr) {
    static_assert(is_trivial<To>::value, "Only trivial types are supported in external memory.");
    return address<To, BlockSize>(addr);
}

/// Performs the equivalent of a `static_cast` from `From*` to `To*`.
/// Such a cast is only possible if From and To form an inheritance
/// relationship, i.e. From is a base of To or the other way around.
/// The raw pointer value will be adjusted automatically to point to the correct address.
template<typename To, typename From, u32 BlockSize>
address<To, BlockSize> address_cast(const address<From, BlockSize>& addr) {
    static_assert(std::is_base_of<To, From>::value || std::is_base_of<From, To>::value,
                  "Addresses to objects of type From cannot be statically converted to To.");
    if (!addr)
        return {};
    if constexpr (std::is_base_of<To, From>::value) {
        return address_cast<To>(addr.raw() + detail::ptr_adjust<From, To>());
    } else {
        return address_cast<To>(addr.raw() - detail::ptr_adjust<To, From>());
    }
}

} // namespace extpp

#endif // EXTPP_ADDRESS_HPP
