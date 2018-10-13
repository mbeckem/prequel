#include <prequel/formatting.hpp>

namespace prequel {

std::string format_hex(const byte* data, size_t size, size_t numbers_per_line) {
    static constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};


    std::string s;

    if (size == 0 || data == nullptr)
        return s;

    s.reserve(3 * size);
    size_t line_numbers = 0;
    for (size_t i = 0;;) {
        s += hexmap[(data[i] & 0xF0) >> 4];
        s += hexmap[data[i] & 0x0F];

        ++i;
        ++line_numbers;
        if (i < size) {
            if (line_numbers >= numbers_per_line) {
                s += "\n";
                line_numbers = 0;
            } else {
                s += " ";
            }
        } else {
            break;
        }
    }
    return s;
}

} // namespace prequel
