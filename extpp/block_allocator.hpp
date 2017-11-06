#ifndef EXTPP_BLOCK_ALLOCATOR_HPP
#define EXTPP_BLOCK_ALLOCATOR_HPP

#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/io.hpp>

#include <extpp/detail/free_list.hpp>

namespace extpp {

/// FIXME: template everywhere.
template<u32 BlockSize>
class block_allocator {
    using free_list_t = detail::free_list<BlockSize>;

public:
    class anchor {
        /// The free list used by this allocator.
        /// Freed block are reused for future allocations.
        typename free_list_t::anchor list;

        /// The first free block at the end of the file (or invalid).
        u64 chunk_begin = -1;

        /// The number of free blocks at the end of the file.
        /// When the free list is empty, new blocks are allocated
        /// by resizing the file.
        u64 chunk_size = 0;

        friend class block_allocator;
    };

public:
    /// Creates a new block allocator instance from the given anchor.
    ///
    /// \param a
    ///     The data structures anchor on disk.
    /// \param chunk_size
    ///     The number of blocks allocated at the same time at the end of the file
    ///     when new storage is required. Must be at least 1.
    explicit block_allocator(handle<anchor, BlockSize> a, engine<BlockSize>& e, u32 chunk_size = 16)
        : m_anchor(std::move(a))
        , m_engine(&e)
        , m_file(&m_engine->fd())
        , m_list(m_anchor.neighbor(&m_anchor->list), e)
        , m_chunk_size(std::max(chunk_size, u32(1)))
    {}

    /// Allocates a new block.
    /// The block must be released using `free()` when it is no longer in use.
    raw_address<BlockSize> allocate() {
        if (!m_list.empty())
            return m_list.pop();

        if (m_anchor->chunk_size == 0) {
            const u64 file_size = m_file->file_size();
            const u32 block_size = BlockSize;
            const u64 first_free = file_size / block_size;
            EXTPP_CHECK(file_size % block_size == 0,
                         "File size is not a multiple of the block size.");

            const u64 alloc_size = block_size * m_chunk_size;
            if (file_size > std::numeric_limits<u64>::max() - alloc_size) {
                // TODO own exception type
                throw std::runtime_error("File size overflow");
            }

            m_file->truncate(file_size + alloc_size);
            m_anchor->chunk_begin = first_free;
            m_anchor->chunk_size = m_chunk_size;
            m_anchor.dirty();
        }

        EXTPP_ASSERT(m_anchor->chunk_size > 0, "There must be free blocks available");
        EXTPP_ASSERT(m_anchor->chunk_begin != u64(-1), "Free block must be valid.");

        const u64 next = m_anchor->chunk_begin;
        m_anchor->chunk_size -= 1;
        m_anchor->chunk_begin = m_anchor->chunk_size ? next + 1 : u64(-1);
        m_anchor.dirty();
        return raw_address<BlockSize>::from_block(next);
    }

    /// Frees a block previously allocated using `allocate()`.
    /// The block must not be modified by the application after this function
    /// has been called.
    void free(raw_address<BlockSize> addr) {
        m_list.push(addr);
    }

    block_allocator(block_allocator&&) noexcept = default;
    block_allocator& operator=(block_allocator&&) noexcept = default;

private:
    handle<anchor, BlockSize> m_anchor;
    engine<BlockSize>* m_engine;
    file* m_file;

    free_list_t m_list;
    u32 m_chunk_size;
};

} // namespace extpp

#endif // EXTPP_BLOCK_ALLOCATOR_HPP
