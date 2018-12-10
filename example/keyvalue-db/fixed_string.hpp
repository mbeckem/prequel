#ifndef FIXED_STRING_HPP
#define FIXED_STRING_HPP

#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>

namespace keyvaluedb {

/*
 * A fixed string of maximum size N.
 */
template<uint32_t N>
class fixed_string {
public:
    static constexpr uint32_t max_size = N;

public:
    fixed_string() { std::memset(m_data, 0, max_size); }

    explicit fixed_string(std::string_view str) {
        if (str.size() > max_size)
            throw std::runtime_error("String is too long.");

        std::memcpy(m_data, str.data(), str.size());
        std::memset(m_data + str.size(), 0, max_size - str.size());
    }

    const char* begin() const { return m_data; }
    const char* end() const { return m_data + size(); }
    const char* data() const { return m_data; }

    uint32_t size() const {
        // Index of first 0 byte (or max_size).
        return std::find(std::begin(m_data), std::end(m_data), 0) - std::begin(m_data);
    }

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&fixed_string::m_data);
    }

    friend bool operator<(const fixed_string& lhs, const fixed_string& rhs) {
        return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator==(const fixed_string& lhs, const fixed_string& rhs) {
        return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator!=(const fixed_string& lhs, const fixed_string& rhs) {
        return !(lhs == rhs);
    }

private:
    // Unset bytes at the end are zero.
    // String is not null terminated (i.e. all max_size bytes can be used).
    char m_data[max_size];
};

} // namespace keyvaluedb

#endif // FIXED_STRING_HPP
