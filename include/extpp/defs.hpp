#ifndef EXTPP_DEFS_HPP
#define EXTPP_DEFS_HPP

#include <climits>
#include <cstdint>

namespace extpp {

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

static_assert(CHAR_BIT == 8,
              "Bytes with a size other than 8 bits are not supported.");

// Marks the passed arguments as "used" to shut up warnings.
// The function will do nothing with its argument.
template<typename... Args>
void unused(Args&&...) {}

/// @}

} // namespace extpp

#endif // EXTPP_DEFS_HPP
