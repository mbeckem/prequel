#ifndef PREQUEL_BLOCK_INDEX_HPP
#define PREQUEL_BLOCK_INDEX_HPP

#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>
#include <prequel/assert.hpp>
#include <prequel/detail/operators.hpp>

#include <ostream>

namespace prequel {

class engine;

/// References a block in secondary storage by index.
class block_index :
        public detail::make_addable<block_index, u64>,
        public detail::make_subtractable<block_index, u64>,
        public detail::make_comparable<block_index>
{
public:
    static constexpr u64 invalid_value = u64(-1);

public:
    /// Constructs an invalid block index.
    block_index() = default;

    /// Constructs a block index with the given value.
    /// Passing \ref invalid_value creates an invalid block index.
    explicit block_index(u64 index)
        : m_value(index)
    {}

    /// Returns true if the block index refers to a block in secondary storage.
    /// \{
    bool valid() const { return m_value != invalid_value; }
    explicit operator bool() const { return valid(); }
    /// \}

    /// Returns the raw value of the block index (which might be \ref invalid_value).
    /// \{
    u64 value() const { return m_value; }
    explicit operator u64() const { return value(); }
    /// \}

    block_index& operator+=(u64 offset) {
        PREQUEL_ASSERT(*this, "Invalid block index.");
        m_value += offset;
        return *this;
    }

    block_index& operator-=(u64 offset) {
        PREQUEL_ASSERT(*this, "Invalid block index.");
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

/// Zeroes `size` blocks, starting from `index`.
/// \relates block_index
void zero_blocks(engine& e, block_index index, u64 size);

/// Copies `size` blocks from `src` to `dest`. The block ranges can overlap.
/// \relates block_index
void copy_blocks(engine& e, block_index src, block_index dest, u64 size);

} // namespace prequel

#endif // PREQUEL_BLOCK_INDEX_HPP
