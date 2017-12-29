#ifndef EXTPP_ENGINE_HPP
#define EXTPP_ENGINE_HPP

#include <extpp/defs.hpp>
#include <extpp/block_handle.hpp>

namespace extpp {

// TODO: Detemplatize
template<u32 BlockSize>
class engine {
public:
    engine() = default;
    virtual ~engine() = default;

    /// Returns the size of the underlying storage, in blocks.
    /// All block indices between in `[0, size())` are valid for
    /// I/O operations.
    virtual u64 size() const = 0;

    /// Grows the underyling storage by `n` blocks.
    virtual void grow(u64 n) = 0;

    /// Reads the block at the given index and returns a handle to it.
    /// Throws if an I/O error occurs.
    virtual block_handle read(block_index index) = 0;

    /// Similar to `read()`, but the block is zeroed instead.
    /// This can save a read operation if the block is not already in memory.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// Throws if an I/O error occurs.
    virtual block_handle zeroed(block_index index) = 0;

    /// Like `zeroed()`, but instead sets the content of the block to `data`.
    ///
    /// Throws if an I/O error occurs.
    ///
    /// \warning `data` must be a pointer to (at least) `BlockSize` bytes.
    virtual block_handle overwritten(block_index index, const byte* data) = 0;

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    virtual void flush() = 0;

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;
};

} // namespace extpp

#endif // EXTPP_ENGINE_HPP
