#ifndef EXTPP_DETAIL_HEX_HPP
#define EXTPP_DETAIL_HEX_HPP

#include <extpp/defs.hpp>

#include <string>

namespace extpp::detail {

// Format a byte array as a hex string.
inline std::string hex_str(const byte* data, size_t size) {
    static constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    std::string s;
    s.reserve(3 * size);
    for (size_t i = 0;;) {
        s += hexmap[(data[i] & 0xF0) >> 4];
        s += hexmap[data[i] & 0x0F];

        if (++i < size) {
            s += ' ';
        } else {
            break;
        }
    }
    return s;
}

}

#endif // EXTPP_DETAIL_HEX_HPP
