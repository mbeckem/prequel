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
    using block_t = unsigned long long int;

    static constexpr size_t bits_per_block = sizeof(block_t) * CHAR_BIT;

public:
    static constexpr size_t npos = size_t(-1);

public:
    /// Constructs a bitset with the given number of bits.
    explicit bitset(size_t bits = 0)
        : m_bits(bits)
        , m_blocks(ceil_div(bits, bits_per_block))
    {}

    /// Returns the number of bits in this bitset.
    size_t size() const { return m_bits; }

    /// Sets the size of the bitset to zero (frees all space).
    void clear() {
        m_blocks.clear();
        m_bits = 0;
    }

    /// Change the size of the bitset. Newly added bits are set to zero.
    void resize(size_t bits) {
        m_blocks.resize(ceil_div(bits, bits_per_block));
        m_bits = bits;
    }

    /// Set all bits to zero.
    void reset() {
        m_blocks.assign(m_blocks.size(), 0);
    }

    /// Returns true iff the bit at the position
    bool test(size_t bit) const {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        return m_blocks[block_index(bit)] & (block_t(1) << bit_index(bit));
    }

    /// Set the bit at the given position to one.
    void set(size_t bit) {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        m_blocks[block_index(bit)] |= (block_t(1) << bit_index(bit));
    }

    /// Set the bit at the given position to zero.
    void unset(size_t bit) {
        EXTPP_ASSERT(bit < m_bits, "Index out of bounds.");
        m_blocks[block_index(bit)] &= ~(block_t(1) << bit_index(bit));
    }

    /// Counts the total number of set bits.
    size_t count() const {
        return count(0, m_bits);
    }

    /// Counts the total number of set bits, starting from the given index.
    size_t count(size_t begin) {
        return count(begin, size() - begin);
    }

    /// Counts the total number of 1-bits in the range [begin, begin + n).
    size_t count(size_t begin, size_t n) const {
        if (begin >= m_bits || n == 0)
            return 0;

        // Sets all bits before `index` to zero.
        auto mask_front = [](block_t block, int index) {
            return (block >> index) << index;
        };

        // Sets all bits at `index` and after it to zero.
        auto mask_back = [](block_t block, int index) {
            int offset = (bits_per_block - index);
            return (block << offset) >> offset;
        };

        size_t end = std::min(begin + n, m_bits);
        size_t current_block = block_index(begin);
        size_t last_block = block_index(end);
        size_t result = 0;

        // Handle the first block.
        if (int i = bit_index(begin); i != 0) {
            block_t blk = m_blocks[current_block];
            blk = mask_front(blk, i);
            if (last_block == current_block) {
                int j = bit_index(end);
                blk = mask_back(blk, j);
                return block_popcount(blk);
            }

            result += block_popcount(blk);
            current_block++;
        }

        // Blockwise popcount for all blocks until the last one is reached.
        for (; current_block < last_block; ++current_block) {
            result += block_popcount(m_blocks[current_block]);
        }

        // Handle remainder in the last block.
        if (int i = bit_index(end); i != 0) {
            result += block_popcount(mask_back(m_blocks[current_block], i));
        }
        return result;
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

    static int block_popcount(block_t b) {
        // TODO: GCC specific
        return __builtin_popcountll(b);
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
