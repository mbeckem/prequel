#ifndef EXTPP_EXTENT_HPP
#define EXTPP_EXTENT_HPP

#include <extpp/address.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>

namespace extpp {

template<u32 BlockSize>
class extent : public uses_allocator<BlockSize> {
public:
    static constexpr u32 block_size = BlockSize;

    class anchor {
        /// Address of the first block.
        raw_address<BlockSize> start;

        /// Number of contiguous blocks.
        u64 size = 0;

        friend class extent;
    };

public:
    /// Destroys any data used by the anchor. Must not use the anchor after calling `destroy()`.
    static void destroy(const anchor& a, allocator<BlockSize>& alloc) {
        if (a.start) {
            alloc.free(a.start);
        }
    }

public:
    extent(anchor_ptr<anchor> anc, allocator<BlockSize>& alloc)
        : extent::uses_allocator(alloc)
        , m_anchor(std::move(anc))
    {}

    extent(const extent&) = delete;
    extent(extent&&) noexcept = default;

    extent& operator=(const extent&) = delete;
    extent& operator=(extent&&) noexcept = default;

    /// Returns true if this extent does not contain any blocks.
    bool empty() const { return size() == 0; }

    /// Returns the number of blocks in this extent.
    u64 size() const { return m_anchor->size; }

    /// Returns the address of the first block in this extent.
    /// The number of available blocks is exactly `size()`.
    raw_address<BlockSize> data() const { return m_anchor->start; }

    /// Returns the address of the block with the given index.
    raw_address<BlockSize> get(u64 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return m_anchor->start + index * BlockSize;
    }

    /// Returns a handle to the block with the given index.
    /// \pre `index < size()`.
    block_handle<BlockSize> access(u64 index) const {
        return this->get_engine().read(get(index).get_block_index());
    }

    /// Returns a handle to the block with the given index.
    /// The block's content will be zeroed and it will be marked as dirty.
    /// This saves the initial read when the intent is to overwrite
    /// the content immediately.
    ///
    /// \pre `index < size()`.
    block_handle<BlockSize> zeroed(u64 index) const {
        return this->get_engine().zeroed(get(index).get_block_index());
    }

    /// Returns a handle to the block with the given index.
    /// The block's content will be overwritten with the given data
    /// and it will be marked as dirty.
    /// This saves the initial read when the intent is to overwrite
    /// the content immediately.
    ///
    /// \pre `data` has BlockSize readable bytes.
    /// \pre `index < size()`.
    block_handle<BlockSize> overwritten(u64 index, const byte* data) const {
        return this->get_engine().overwritten(get(index).get_block_index(), data);
    }

    /// Removes all blocks from this extent.
    /// \post `size() == 0`.
    void clear() {
        if (empty())
            return;

        this->get_allocator().free(m_anchor->start);
        m_anchor->start = {};
        m_anchor->size = 0;
        m_anchor.dirty();
        return;
    }

    /// Resizes this extents to the given number of blocks.
    /// Existing block contents within the range [0, min(size(), new_size)) are copied.
    /// New blocks are not initialized.
    ///
    /// \post `size() == new_size`.
    ///
    /// \warning This invalidates all block addresses and handles
    /// because the blocks might be moved to a new location.
    void resize(u64 new_size) {
        if (new_size == size())
            return;

        m_anchor->start = this->get_allocator().reallocate(m_anchor->start, new_size);
        m_anchor->size = new_size;
        m_anchor.dirty();
    }

private:
    anchor_ptr<anchor> m_anchor;
};

} // namespace extpp

#endif // EXTPP_EXTENT_HPP
