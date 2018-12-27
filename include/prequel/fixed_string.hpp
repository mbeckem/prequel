#ifndef PREQUEL_FIXED_STRING_HPP
#define PREQUEL_FIXED_STRING_HPP

#include <prequel/assert.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>
#include <prequel/exception.hpp>

#include <algorithm>
#include <cstring>
#include <type_traits>

namespace prequel {

/*
 * A fixed string that stores strings up to a given maximum size.
 *
 * Strings can contain arbitrary binary data because this type stores the length
 * of the string separately. Unused bytes at the end of the string are guaranteed to be zero.
 *
 * This class supports binary serialization and is serialiazed as a size (1 or 2 bytes; 1 byte
 * if `Capacity <= 255`, 2 bytes otherwise) and a byte array of size `Capacity`.
 */
template<u32 Capacity>
class fixed_string {
public:
    static constexpr u32 max_size = Capacity;

public:
    fixed_string()
        : m_size(0) {
        std::memset(m_data, 0, max_size);
    }

    fixed_string(const char* str)
        : fixed_string(std::string_view(str)) {}

    fixed_string(const char* str, size_t size)
        : fixed_string(std::string_view(str, size)) {}

    fixed_string(std::string_view str) {
        if (str.size() > max_size)
            PREQUEL_THROW(bad_argument("String is too long."));

        m_size = str.size();
        std::memcpy(m_data, str.data(), str.size());
        std::memset(m_data + str.size(), 0, max_size - str.size());
    }

    const char* begin() const { return m_data; }
    const char* end() const { return m_data + size(); }
    const char* data() const { return m_data; }

    u32 size() const { return m_size; }

    std::string_view view() const { return std::string_view(data(), size()); }

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&fixed_string::m_size, &fixed_string::m_data);
    }

    friend bool operator<(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() < rhs.view();
    }

    friend bool operator>(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() > rhs.view();
    }

    friend bool operator<=(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() <= rhs.view();
    }

    friend bool operator>=(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() >= rhs.view();
    }

    friend bool operator==(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() == rhs.view();
    }

    friend bool operator!=(const fixed_string& lhs, const fixed_string& rhs) {
        return lhs.view() != rhs.view();
    }

private:
    static_assert(Capacity < u32(1) << 16, "Capacity is too large (maximum is 65535).");

    using internal_size_type = std::conditional_t<Capacity <= 255, u8, u16>;

private:
    internal_size_type m_size;
    char m_data[max_size];
};

/*
 * A fixed string type that stores strings up to a given maximum size.
 *
 * Strings are either terminated by NULs or take up the entire array.
 * In other words, when the string size is less than `N`, then the remaining characters
 * in the internal array are all 0.
 *
 * This class supports binary serialization and is serialized as a byte array
 * of size `Capacity`.
 */
template<u32 Capacity>
class fixed_cstring {
public:
    static constexpr u32 max_size = Capacity;

public:
    fixed_cstring() { std::memset(m_data, 0, max_size); }

    /*
     * Constructs a fixed cstring from the given string pointer, which
     * must be zero terminated.
     *
     * Throws if the string is too long to fit into the available capacity.
     */
    fixed_cstring(const char* str)
        : fixed_cstring(std::string_view(str)) {}

    /*
     * Constructs a fixed cstring from the given string view. Note that
     * the string view should not contain embedded NUL bytes (those would
     * be interpreted as the premature end of the string).
     *
     * Throws if the string is too long to fit into the available capacity.
     */
    fixed_cstring(std::string_view str) {
        if (str.size() > max_size)
            PREQUEL_THROW(bad_argument("String is too long."));

        PREQUEL_ASSERT(std::find(str.begin(), str.end(), 0) == str.end(),
                       "Strings must not contain embedded NULs.");
        std::memcpy(m_data, str.data(), str.size());
        std::memset(m_data + str.size(), 0, max_size - str.size());
    }

    const char* begin() const { return m_data; }
    const char* end() const { return m_data + size(); }
    const char* data() const { return m_data; }

    u32 size() const {
        // Index of first 0 byte (or max_size).
        return std::find(std::begin(m_data), std::end(m_data), 0) - std::begin(m_data);
    }

    std::string_view view() const { return std::string_view(data(), size()); }

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&fixed_cstring::m_data);
    }

    friend bool operator<(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() < rhs.view();
    }

    friend bool operator>(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() > rhs.view();
    }

    friend bool operator<=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() <= rhs.view();
    }

    friend bool operator>=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() >= rhs.view();
    }

    friend bool operator==(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() == rhs.view();
    }

    friend bool operator!=(const fixed_cstring& lhs, const fixed_cstring& rhs) {
        return lhs.view() != rhs.view();
    }

private:
    // Unset bytes at the end are zero. If the entire capacity is used,
    // the string is not zero terminated.
    char m_data[max_size];
};

} // namespace prequel

#endif // PREQUEL_FIXED_STRING_HPP
