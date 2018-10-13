#ifndef PREQUEL_FORMATTING_HPP
#define PREQUEL_FORMATTING_HPP

#include <prequel/defs.hpp>

#include <string>

namespace prequel {

// Format a byte array as a hex string.
// A linebreak will be inserted between two hex numbers if the amount of numbers
// in the current line would exceed `numbers_per_line`.
std::string format_hex(const byte* data, size_t size, size_t numbers_per_line = -1);

} // namespace prequel

#endif // PREQUEL_FORMATTING_HPP
