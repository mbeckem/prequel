#ifndef PREQUEL_MATH_HPP
#define PREQUEL_MATH_HPP

#include <prequel/assert.hpp>
#include <prequel/defs.hpp>

#include <stdexcept>
#include <type_traits>

namespace prequel {

/// \defgroup math Math functions
/// @{

template<typename T>
using IsUnsigned = std::enable_if_t<std::is_unsigned<T>::value, T>;

template<typename T>
using IsInteger = std::enable_if_t<std::is_integral<T>::value, T>;

/// Rounds `v` towards the next power of two. Returns `v` if it is already a power of two.
/// Note: returns 0 if `v == 0`.
///
/// Adapted from http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
template<typename T, IsUnsigned<T>* = nullptr>
constexpr T round_towards_pow2(T v) noexcept {
    --v;
    for (u64 i = 1; i < sizeof(T) * CHAR_BIT; i *= 2) {
        v |= v >> i;
    }
    return ++v;
}

/// Computes the base-2 logarithm of `v`.
/// \pre `v > 0`.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr T log2(T v) noexcept {
    PREQUEL_CONSTEXPR_ASSERT(v != 0, "v must be greater than zero.");
    T log = 0;
    while (v >>= 1)
        ++log;
    return log;
}

/// Returns true if the given integer is a power of two.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr bool is_pow2(T v) noexcept {
    return v && !(v & (v - 1));
}

/// Returns a % b where b is a power of two.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr T mod_pow2(T a, T b) noexcept {
    PREQUEL_CONSTEXPR_ASSERT(is_pow2(b), "b must be a power of two");
    return a & (b - 1);
}

/// \brief Returns true if a is aligned, i.e. if it is divisible by b.
/// b must be a power of two.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr bool is_aligned(T a, T b) noexcept {
    return mod_pow2(a, b) == 0;
}

/// Returns ceil(A / B) for two positive (non-zero) integers.
template<typename T, IsInteger<T>* = nullptr>
constexpr T ceil_div(T a, T b) {
    PREQUEL_CONSTEXPR_ASSERT(a >= 0, "Dividend must be greater than or equal to zero.");
    PREQUEL_CONSTEXPR_ASSERT(b > 0, "Divisor must be greater than zero.");
    return (a + b - 1) / b;
}

/// Returns the signed difference `a - b` for two unsigned numbers.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr std::make_signed_t<T> signed_difference(T a, T b) {
    using U = std::make_signed_t<T>;
    if (a >= b) {
        return static_cast<U>(a - b);
    } else {
        return -static_cast<U>(b - a);
    }
}

/// Returns true if the addition `a+b` would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr bool add_overflows(T a, T b) {
    T dummy = 0;
    return __builtin_add_overflow(a, b, &dummy);
}

/// Returns true if the subtraction `a-b` would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr bool sub_overflows(T a, T b) {
    T dummy = 0;
    return __builtin_sub_overflow(a, b, &dummy);
}

/// Returns true if the multiplication `a*b` would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr bool mul_overflows(T a, T b) {
    T dummy = 0;
    return __builtin_mul_overflow(a, b, &dummy);
}

/// Performs checked addition of the passed arguments.
/// Throws std::overflow_error if the operation would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr T checked_add(T a, T b) {
    T result = 0;
    if (__builtin_add_overflow(a, b, &result))
        throw std::overflow_error("Addition overflows.");
    return result;
}

/// Performs checked subtraction of the passed arguments.
/// Throws std::overflow_error if the operation would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr T checked_sub(T a, T b) {
    T result = 0;
    if (__builtin_sub_overflow(a, b, &result))
        throw std::overflow_error("Subtraction overflows.");
    return result;
}

/// Performs checked multiplication of the passed arguments.
/// Throws std::overflow_error if the operation would overflow.
template<typename T, IsInteger<T>* = nullptr>
constexpr T checked_mul(T a, T b) {
    T result = 0;
    if (__builtin_mul_overflow(a, b, &result))
        throw std::overflow_error("Multiplication overflows.");
    return result;
}

/// @}

} // namespace prequel

#endif // PREQUEL_MATH_HPP
