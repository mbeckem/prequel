#ifndef EXTPP_EXTENT_HPP
#define EXTPP_EXTENT_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/handle.hpp>

#include <memory>

namespace extpp {

class extent;
class extent_impl;

class extent_anchor {
    /// Index of the first block (or invalid).
    block_index start;

    /// Number of blocks.
    u64 size = 0;

    static constexpr auto get_binary_format() {
        return make_binary_format(&extent_anchor::start, &extent_anchor::size);
    }

    friend class binary_format_access;
    friend class extent_impl;
};

/// An extent is a range of contiguous blocks in external storage.
/// Extents can be resized dynamically.
/// The content of blocks managed by an extent instance is *not* initialized
/// at first. However, when an extent is resized (and possibly moved on disk),
/// then the existing data will be copied over (as much as fits the new location).
class extent {
public:
    using anchor = extent_anchor;

public:
    explicit extent(handle<anchor> _anchor, allocator& alloc);
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

    /// Returns the the block index of this extent's block range.
    /// Returns the invalid block index if this extent is empty.
    block_index data() const;

    /// Returns the block index of the given block in this extent.
    /// Throws if index is out of bounds.
    block_index get(u64 index) const;

    /// Returns a handle to the block with the given index.
    /// Throws if index is out of bounds.
    block_handle read(u64 index) const;

    /// Returns a handle to the block with the given index via \ref engine::zeroed,
    /// i.e. the data is not read from disk and overwritten with zeroes instead.
    /// Throws if index is out of bounds.
    block_handle zeroed(u64 index) const;

    /// Returns a handle to the block with the given index via \ref engine::overwritten,
    /// i.e. the data is not read from disk and overwritten with the given data array instead.
    /// Throws if index is out of bounds.
    block_handle overwritten(u64 index, const byte* data, size_t data_size) const;

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
    extent_impl& impl() const;

private:
    std::unique_ptr<extent_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_EXTENT_HPP
