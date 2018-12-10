#ifndef PREQUEL_EXTENT_HPP
#define PREQUEL_EXTENT_HPP

#include <prequel/allocator.hpp>
#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/block_index.hpp>
#include <prequel/defs.hpp>
#include <prequel/handle.hpp>

#include <memory>

namespace prequel {

class extent;

namespace detail {

class extent_impl;

struct extent_anchor {
    /// Index of the first block (or invalid).
    block_index start;

    /// Number of blocks.
    u64 size = 0;

    static constexpr auto get_binary_format() {
        return binary_format(&extent_anchor::start, &extent_anchor::size);
    }
};

} // namespace detail

using extent_anchor = detail::extent_anchor;

/// An extent is a range of contiguous blocks in external storage.
/// Extents can be resized dynamically.
/// The content of blocks managed by an extent instance is *not* initialized
/// at first. However, when an extent is resized (and possibly moved on disk),
/// then the existing data will be copied over (as much as fits the new location).
class extent {
public:
    using anchor = extent_anchor;

public:
    explicit extent(anchor_handle<anchor> _anchor, allocator& _alloc);
    ~extent();

    extent(const extent&) = delete;
    extent& operator=(const extent&) = delete;

    extent(extent&&) noexcept;
    extent& operator=(extent&&) noexcept;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    u32 block_size() const;

    /// Returns true if this extent is empty (0 blocks).
    bool empty() const;

    /// Returns the number of blocks in this extent.
    u64 size() const;

    /// Returns the number of bytes occupied by this extent.
    u64 byte_size() const;

    /// Returns the the block index of this extent's block range.
    /// Returns the invalid block index if this extent is empty.
    block_index data() const;

    /// Returns the block index of the given block in this extent.
    /// Throws if index is out of bounds.
    block_index get(u64 index) const;

    /// Returns a handle to the block with the given index, if it can be accessed
    /// without performing actual I/O (see \ref engine::access).
    /// Throws if index is out of bounds.
    block_handle access(u64 index) const;

    /// Returns a handle to the block with the given index.
    /// Throws if index is out of bounds.
    block_handle read(u64 index) const;

    /// Returns a handle to the block with the given index via \ref engine::overwrite_zero,
    /// i.e. the data is not read from disk and overwritten with zeroes instead.
    /// Throws if index is out of bounds.
    block_handle overwrite_zero(u64 index) const;

    /// Returns a handle to the block with the given index via \ref engine::overwrite,
    /// i.e. the data is not read from disk and overwritten with the given data array instead.
    /// Throws if index is out of bounds.
    block_handle overwrite(u64 index, const byte* data, size_t data_size) const;

    /// Removes all blocks from this extent.
    /// \post `empty()`.
    void clear();

    /// Removes all blocks from this extent.
    /// The extent will occupy zero blocks on disk.
    /// \post `empty()`.
    void reset();

    /// Resizes this extent to the given number of blocks.
    /// Uses the allocator's reallocation function to potentially grow the
    /// extent in place. If the extent moves on disk, existing data is copied over
    /// to the new location as far as new_size permits.
    /// If the extent grows, new blocks are not initialized.
    ///
    /// \post `size() == new_size`
    ///
    /// \warning The extent may move on disk and therefore all existing block indices
    /// and addresses pointing into it may become invalid.
    void resize(u64 new_size);

private:
    detail::extent_impl& impl() const;

private:
    std::unique_ptr<detail::extent_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_EXTENT_HPP
