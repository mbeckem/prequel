#ifndef EXTPP_DETAIL_BITSET_HPP
#define EXTPP_DETAIL_BITSET_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>

#include <climits>
#include <vector>

namespace extpp {
namespace detail {

class bitset {
private:
    using block_t = long long int;

    static constexpr size_t bits_per_block = sizeof(block_t) * CHAR_BIT;

public:
    static constexpr size_t npos = size_t(-1);

public:
    explicit bitset(size_t bits = 0)
        : m_bits(bits)
        , m_blocks(ceil_div(bits, bits_per_block))
    {}

    size_t size() const { return m_bits; }

    void clear() {
        m_blocks.clear();
        m_bits = 0;
    }

    void resize(size_t bits) {
        m_blocks.resize(ceil_div(bits, bits_per_block));
        m_bits = bits;
    }

    void reset() {
        m_blocks.assign(m_blocks.size(), 0);
    }

    bool test(size_t bit) const {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        return m_blocks[block_index(bit)] & (block_t(1) << bit_index(bit));
    }

    void set(size_t bit) {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        m_blocks[block_index(bit)] |= (block_t(1) << bit_index(bit));
    }

    void unset(size_t bit) {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        m_blocks[block_index(bit)] &= ~(block_t(1) << bit_index(bit));
    }

    /// Finds the position of the first unset bit, starting from `n` (inclusive).
    /// Returns the index of that bit or `npos` if none was found.
    size_t find_set(size_t n = 0) const {
        if (n >= m_bits)
            return npos;

        // Check current block if not on block boundary.
        size_t b = block_index(n);
        int i = bit_index(n);
        if (i != 0) {
            int s = block_ffs(m_blocks[b] >> i);
            if (s != 0)
                return n + s - 1;
            return do_ffs(b + 1);
        }
        return do_ffs(b);
    }

    /// Finds the position of the first set bit, starting from `n` (inclusive).
    /// Returns the index of that bit or `npos` if none was found.
    size_t find_unset(size_t n = 0) const {
        if (n >= m_bits)
            return npos;

        size_t r = [&]{
            // Check current block if not on block boundary.
            size_t b = block_index(n);
            int i = bit_index(n);
            if (i != 0) {
                int s = block_ffs((~m_blocks[b]) >> i);
                if (s != 0)
                    return n + s - 1;
                return do_ffz(b + 1);
            }
            return do_ffz(b);
        }();
        // The last few bits beyond the size are always unset.
        return r < m_bits ? r : npos;
    }

private:
    size_t do_ffs(size_t b) const {
        size_t sz = m_blocks.size();
        while (b < sz) {
            int s = block_ffs(m_blocks[b]);
            if (s != 0) {
                return b * bits_per_block + s - 1;
            }

            b += 1;
        }
        return npos;
    }

    size_t do_ffz(size_t b) const {
        size_t sz = m_blocks.size();
        while (b < sz) {
            int s = block_ffz(m_blocks[b]);
            if (s != 0) {
                return b * bits_per_block + s - 1;
            }

            b += 1;
        }
        return npos;
    }

    static int block_ffs(block_t b) {
        // TODO: GCC specific.
        return __builtin_ffsll(b);
    }

    static int block_ffz(block_t b) {
        return block_ffs(~b);
    }

    static size_t block_index(size_t bit) {
        return bit / bits_per_block;
    }

    static int bit_index(size_t bit) {
        return bit & (bits_per_block - 1);
    }


private:
    size_t m_bits = 0;
    std::vector<block_t> m_blocks;
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_BITSET_HPP
