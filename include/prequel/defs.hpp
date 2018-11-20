#ifndef PREQUEL_DEFS_HPP
#define PREQUEL_DEFS_HPP

#include <climits>
#include <cstdint>
#include <type_traits>

namespace prequel {

/// \defgroup defs Definitions
/// @{

using u8 = std::uint8_t;
using u16 = std::uint16_t;
using u32 = std::uint32_t;
using u64 = std::uint64_t;

using i8 = std::int8_t;
using i16 = std::int16_t;
using i32 = std::int32_t;
using i64 = std::int64_t;

using byte = unsigned char;

using std::ptrdiff_t;
using std::uintptr_t;
using std::size_t;

/*
 * Guards against weird platforms.
 */
static_assert(CHAR_BIT == 8,
              "Bytes with a size other than 8 bits are not supported.");
static_assert(std::is_same_v<char, u8> || std::is_same_v<unsigned char, u8>,
              "uint8_t must be either char or unsigned char.");
static_assert(std::is_same_v<char, i8> || std::is_same_v<signed char, i8>,
              "int8_t must be either char or signed char.");

// Marks the passed arguments as "used" to shut up warnings.
// The function will do nothing with its argument.
template<typename... Args>
void unused(Args&&...) {}

/// @}

} // namespace prequel

#endif // PREQUEL_DEFS_HPP
