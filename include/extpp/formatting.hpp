#ifndef EXTPP_FORMATTING_HPP
#define EXTPP_FORMATTING_HPP

#include <extpp/defs.hpp>

#include <string>

namespace extpp {

// Format a byte array as a hex string.
// A linebreak will be inserted between two hex numbers if the amount of numbers
// in the current line would exceed `numbers_per_line`.
std::string format_hex(const byte* data, size_t size, size_t numbers_per_line = -1);

} // namespace extpp

#endif // EXTPP_FORMATTING_HPP
