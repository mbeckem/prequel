#ifndef EXTPP_ENGINE_HPP
#define EXTPP_ENGINE_HPP

#include <extpp/assert.hpp>
#include <extpp/block_handle.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>

namespace extpp {

class engine {
public:
    engine(u32 block_size)
        : m_block_size(block_size)
    {
        EXTPP_CHECK(is_pow2(block_size), "Block size must be a power of two.");
    }

    virtual ~engine() = default;

    u32 block_size() const { return m_block_size; }

    /// Returns the size of the underlying storage, in blocks.
    /// All block indices between in `[0, size())` are valid for
    /// I/O operations.
    u64 size() const { return do_size(); }

    /// Grows the underyling storage by `n` blocks.
    virtual void grow(u64 n) { return do_grow(n); }

    /// Reads the block at the given index and returns a handle to it.
    /// Throws if an I/O error occurs.
    virtual block_handle read(block_index index) {
        EXTPP_CHECK(index, "Invalid index.");
        return do_read(index);
    }

    /// Similar to `read()`, but the block is zeroed instead.
    /// This can save a read operation if the block is not already in memory.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// Throws if an I/O error occurs.
    virtual block_handle zeroed(block_index index) {
        EXTPP_CHECK(index, "Invalid index.");
        return do_zeroed(index);
    }

    /// Like `zeroed()`, but instead sets the content of the block to `data`.
    ///
    /// Throws if an I/O error occurs.
    ///
    /// \warning `data` must be a pointer to (at least) `block_size()` bytes.
    virtual block_handle overwritten(block_index index, const byte* data, size_t data_size) {
        EXTPP_CHECK(index, "Invalid index.");
        EXTPP_CHECK(data_size >= block_size(), "Not enough data.");
        return do_overwritten(index, data);
    }

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    virtual void flush() {
        do_flush();
    }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

private:
    virtual u64 do_size() const = 0;
    virtual void do_grow(u64 n) = 0;
    virtual block_handle do_read(block_index index) = 0;
    virtual block_handle do_zeroed(block_index index) = 0;
    virtual block_handle do_overwritten(block_index index, const byte* data) = 0;
    virtual void do_flush() = 0;

private:
    u32 m_block_size;
};

} // namespace extpp

#endif // EXTPP_ENGINE_HPP
