#ifndef EXTPP_MATH_HPP
#define EXTPP_MATH_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>

#include <type_traits>

namespace extpp {

template<typename T>
using IsUnsigned = std::enable_if_t<std::is_unsigned<T>::value, T>;

/// Rounds `v` towards the next power of two.
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
template<typename T, IsUnsigned<T>* = nullptr>
constexpr T log2(T v) noexcept {
    EXTPP_CONSTEXPR_ASSERT(v != 0, "v must be greater than zero.");
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
    EXTPP_CONSTEXPR_ASSERT(is_pow2(b), "b must be a power of two");
    return a & (b - 1);
}

/// Returns true if a is aligned, i.e. if it is divisible by b.
/// b must be a power of two.
template<typename T, IsUnsigned<T>* = nullptr>
constexpr bool is_aligned(T a, T b) noexcept {
    return mod_pow2(a, b) == 0;
}

} // namespace extpp

#endif // EXTPP_MATH_HPP