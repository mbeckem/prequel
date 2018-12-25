#ifndef PREQUEL_FIXED_STRING_HPP
#define PREQUEL_FIXED_STRING_HPP

#include <prequel/defs.hpp>
#include <prequel/binary_format.hpp>

#include <algorithm>
#include <cstring>

namespace prequel {

// TODO: Fixed string template without zero terminator (i.e. real before the string).

/*
 * A fixed string of maximum size N. Strings are either terminated by NULs
 * or take up the entire array. In other words, when the string size is less
 * than `N`, then the remaining characters in the internal array are all 0.
 */
template<u32 N>
class fixed_cstring {
public:
    static constexpr u32 max_size = N;

public:
    fixed_cstring() { std::memset(m_data, 0, max_size); }

    /*
     * Constructs a fixed cstring from the given string pointer, which
     * must be zero terminated.
     *
     * Throws if the string is too long to fit into the available capacity.
     */
    fixed_cstring(const char* str): fixed_cstring(std::string_view(str)) {}

    /*
     * Constructs a fixed cstring from the given string view. Note that
     * the string view should not contain embedded NUL bytes (those would
     * be interpreted as the premature end of the string).
     *
     * Throws if the string is too long to fit into the available capacity.
     */
    fixed_cstring(std::string_view str) {
        if (str.size() > max_size)
            throw std::runtime_error("String is too long.");

        std::memcpy(m_data, str.data(), str.size());
        std::memset(m_data + str.size(), 0, max_size - str.size());
    }

    const char* begin() const { return m_data; }
    const char* end() const { return m_data + size(); }
    const char* data() const { return m_data; }

    std::string_view view() const { return std::string_view(data(), size()); }

    u32 size() const {
        // Index of first 0 byte (or max_size).
        return std::find(std::begin(m_data), std::end(m_data), 0) - std::begin(m_data);
    }

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&fixed_cstring::m_data);
    }

    friend bool operator<(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator>(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return rhs < lhs;
    }

    friend bool operator<=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return !(lhs > rhs);
    }

    friend bool operator>=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return !(lhs < rhs);
    }

    friend bool operator==(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator!=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return !(lhs == rhs);
    }

private:
    // Unset bytes at the end are zero. If the entire capacity is used,
    // the string is not zero terminated.
    char m_data[max_size];
};

} // namespace prequel

#endif // PREQUEL_FIXED_STRING_HPP
