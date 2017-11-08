#ifndef EXTPP_EXTENT_HPP
#define EXTPP_EXTENT_HPP

#include <extpp/address.hpp>
#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>

namespace extpp {

template<u32 BlockSize>
class extent {
public:
    class anchor {
        /// Address of the first block.
        raw_address<BlockSize> start;

        /// Number of contiguous blocks.
        u64 size = 0;

        friend class extent;
    };

public:
    extent(handle<anchor, BlockSize> anc, extpp::engine<BlockSize>& eng, extpp::allocator<BlockSize>& alloc)
        : m_anchor(std::move(anc))
        , m_engine(&eng)
        , m_alloc(&alloc)
    {}

    extpp::engine<BlockSize>& engine() const { return *m_engine; }
    extpp::allocator<BlockSize>& allocator() const { return *m_alloc; }

    /// Returns true if this extent does not contain any blocks.
    bool empty() const { return size() == 0; }

    /// Returns the number of blocks in this extent.
    u64 size() const { return m_anchor->size; }

    /// Returns the address of the first block in this extent.
    /// The number of available blocks is exactly `size()`.
    raw_address<BlockSize> data() const { return m_anchor->start; }

    /// Returns a handle to the block with the given index.
    /// \pre `index < size()`.
    block_handle<BlockSize> access(u64 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return m_engine->read(m_anchor->start.block_index() + index);
    }

    /// Returns a handle to the block with the given index.
    /// The block's content will be zeroed and it will be marked as dirty.
    /// This saves the initial read when the intent is to overwrite
    /// the content immediately.
    ///
    /// \pre `index < size()`.
    block_handle<BlockSize> overwrite(u64 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds");
        return m_engine->overwrite(m_anchor->start.block_index() + index);
    }

    /// Returns a handle to the block with the given index.
    /// The block's content will be overwritten with the given data
    /// and it will be marked as dirty.
    /// This saves the initial read when the intent is to overwrite
    /// the content immediately.
    ///
    /// \pre `data` has BlockSize readable bytes.
    /// \pre `index < size()`.
    block_handle<BlockSize> overwrite(u64 index, const byte* data) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds");
        return m_engine->overwrite(m_anchor->start.block_index() + index, data);
    }

    /// Removes all blocks from this extent.
    /// \post `size() == 0`.
    void clear() {
        if (empty())
            return;

        m_alloc->free(m_anchor->start);
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
    /// \warning This invalidates all handles handed out using `access` or `overwrite`,
    /// because the blocks might me moved in external storage.
    void resize(u64 new_size) {
        // TODO reallocate.
        if (new_size == size()) {
            return;
        }
        if (new_size == 0) {
            clear();
            return;
        }

        const auto addr = m_alloc->allocate(new_size);
        const u64 copy = std::min(size(), new_size);
        for (u64 i = 0; i < copy; ++i) {
            auto lhs = m_engine->read(m_anchor->start.block_index() + i);
            m_engine->overwrite(addr.block_index() + i, lhs.data());
        }

        if (m_anchor->start)
            m_alloc->free(m_anchor->start);
        m_anchor->start = addr;
        m_anchor->size = new_size;
        m_anchor.dirty();
    }

private:
    handle<anchor, BlockSize> m_anchor;
    extpp::engine<BlockSize>* m_engine;
    extpp::allocator<BlockSize>* m_alloc;
};

} // namespace extpp

#endif // EXTPP_EXTENT_HPP
