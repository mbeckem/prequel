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

template<size_t Size, size_t Align>
struct ptr_adjust_helper {
    alignas(Align) static const char buffer[Size];
};

// The pointer adjustment necessary to go from Derived* to Base*.
// Only useful (and highly unsafe) for trivially copyable types.
// This should really be a constexpr function but reinterpreting memory
// is forbidden in constexpr contexts.
template<typename Derived, typename Base>
static ptrdiff_t ptr_adjust_impl() {
    auto from = reinterpret_cast<const Derived*>(&ptr_adjust_helper<sizeof(Derived), alignof(Derived)>::buffer);
    auto to = static_cast<const Base*>(from);
    auto diff = reinterpret_cast<const char*>(to) - reinterpret_cast<const char*>(from);
    EXTPP_ASSERT(diff >= 0, "Negative offset from derived to base!");
    return diff;
}

template<typename Derived, typename Base>
static ptrdiff_t ptr_adjust() {
    static const ptrdiff_t adjust = ptr_adjust_impl<std::remove_const_t<Derived>, std::remove_const_t<Base>>();
    return adjust;
}

} // namespace detail

/// Addresses an arbitrary byte offset in external memory.
template<u32 BlockSize>
class raw_address
        : detail::make_comparable<raw_address<BlockSize>>
        , detail::make_addable<raw_address<BlockSize>, i64>
        , detail::make_subtractable<raw_address<BlockSize>, i64>
{
    static_assert(is_pow2(BlockSize), "BlockSize must be a power of two.");

public:
    static constexpr u32 block_size = BlockSize;
    static constexpr u32 block_size_log2 = log2(block_size);
    static constexpr u64 invalid_value = u64(-1);

    static raw_address from_block(u64 block_index) {
        return raw_address(block_index * BlockSize);
    }

public:
    raw_address(): m_value(invalid_value) {}

    explicit raw_address(u64 value): m_value(value) {}

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
        m_value += offset;
        return *this;
    }

    raw_address& operator-=(i64 offset) {
        m_value -= offset;
        return *this;
    }

    friend bool operator==(const raw_address& lhs, const raw_address& rhs) {
        return lhs.value() == rhs.value();
    }

    friend bool operator<(const raw_address& lhs, const raw_address& rhs) {
        return lhs.value() < rhs.value();
    }

    friend std::ostream& operator<<(std::ostream& o, const raw_address& addr) {
        if (!addr)
            return o << "INVALID";
        return o << "(" << addr.block_index() << ", " << addr.block_offset() << ")";
    }

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

    address(std::nullptr_t) {}

private:
    template<typename U, u32 Bs>
    friend address<U, Bs> address_cast(const raw_address<Bs>&);

    template<typename U, u32 Bs>
    friend class address;

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

/// Performs the equivalent of `reinterpret_cast` to `To*`.
template<typename To, u32 BlockSize>
address<To, BlockSize> address_cast(const raw_address<BlockSize>& addr) {
    static_assert(is_trivial<To>::value, "Only trivial types are supported in external memory.");
    return address<To, BlockSize>(addr);
}

namespace detail {

template<typename To, typename From, u32 BlockSize>
address<To, BlockSize> static_address_cast(const address<From, BlockSize>& addr, std::true_type /* To is base */) {
    // To is base of From.
    return address_cast<To>(addr.raw() + detail::ptr_adjust<From, To>());
}

template<typename To, typename From, u32 BlockSize>
address<To, BlockSize> static_address_cast(const address<From, BlockSize>& addr, std::false_type /* To is base */) {
    // From is base of To.
    return address_cast<To>(addr.raw() - detail::ptr_adjust<To, From>());
}

} // namespace detail

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
    return detail::static_address_cast<To>(addr, std::is_base_of<To, From>());
}

} // namespace extpp

#endif // EXTPP_ADDRESS_HPP
