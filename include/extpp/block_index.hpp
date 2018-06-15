#ifndef EXTPP_BLOCK_INDEX_HPP
#define EXTPP_BLOCK_INDEX_HPP

#include <extpp/binary_format.hpp>
#include <extpp/defs.hpp>
#include <extpp/assert.hpp>
#include <extpp/detail/operators.hpp>

#include <ostream>

namespace extpp {

class block_index :
        public detail::make_addable<block_index, u64>,
        public detail::make_subtractable<block_index, u64>,
        public detail::make_comparable<block_index>
{
public:
    static constexpr u64 invalid_value = u64(-1);

public:
    block_index() = default;

    explicit block_index(u64 index)
        : m_value(index)
    {}

    bool valid() const { return m_value != invalid_value; }
    u64 value() const { return m_value; }

    explicit operator u64() const { return value(); }
    explicit operator bool() const { return valid(); }

    block_index& operator+=(u64 offset) {
        EXTPP_ASSERT(*this, "Invalid block index.");
        m_value += offset;
        return *this;
    }

    block_index& operator-=(u64 offset) {
        EXTPP_ASSERT(*this, "Invalid block index.");
        m_value -= offset;
        return *this;
    }

    bool operator<(const block_index& rhs) const {
        return (value() + 1) < (rhs.value() + 1);
    }

    bool operator==(const block_index& rhs) const {
        return value() == rhs.value();
    }

    friend std::ostream& operator<<(std::ostream& o, const block_index& index) {
        if (!index)
            o << "INVALID";
        else
            o << index.value();
        return o;
    }

    static constexpr auto get_binary_format() {
        return make_binary_format(&block_index::m_value);
    }

private:
    u64 m_value = invalid_value;
};

} // namespace extpp

#endif // EXTPP_BLOCK_INDEX_HPP
