#ifndef EXTPP_DEFAULT_ALLOCATOR_HPP
#define EXTPP_DEFAULT_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/assert.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/io.hpp>
#include <extpp/handle.hpp>
#include <extpp/identity_key.hpp>

#include <extpp/detail/free_list.hpp>

#include <fmt/ostream.h>

namespace extpp {

template<u32 BlockSize>
class default_allocator : public allocator<BlockSize> {
private:
    /// An entry in the tree that contains all allocations (free or not).
    /// An extent is sequence of contiguous blocks and is represented
    /// using a (block, size) pair where `block` is the first block of the extent.
    /// Extents are indexed by their starting address (the `block` field).
    struct extent_t {
        u64 block;      ///< Index of the first block of this allocation.
        u64 size: 63;   ///< Size (in blocks) of this allocation.
        u64 free: 1;    ///< Whether this allocation is currently in use or not.

        extent_t(u64 block, u64 size, bool free)
            : block(block), size(size), free(free)
        {}
    };
    static_assert(sizeof(extent_t) == 2 * sizeof(u64), "compact extent.");

    struct extent_t_key {
        u64 operator()(const extent_t& a) const { return a.block; }
    };

    /// An entry in the free list (which is a tree). Entries
    /// are indexed by their size in order to find a fitting
    /// allocation in log time. Ties are broken using the starting address.
    /// These fields mirror the values of the extent_t instance.
    struct free_extent_t {
        u64 size;       ///< Size (in blocks) of this allocation.
        u64 block;      ///< First block of this allocation.

        bool operator<(const free_extent_t& other) const {
            if (size != other.size)
                return size < other.size;
            return block < other.block;
        }
    };

    struct metadata_allocator : allocator<BlockSize> {
        metadata_allocator(default_allocator* parent): parent(parent) {}

        metadata_allocator(metadata_allocator&&) = delete;
        metadata_allocator& operator=(const metadata_allocator&) = delete;

        raw_address<BlockSize> do_allocate(u64 n) override {
            EXTPP_CHECK(n == 1, "Cannot allocate sizes other than 1.");
            return parent->allocate_metadata_block();
        }

        raw_address<BlockSize> do_reallocate(raw_address<BlockSize>, u64 ) override {
            throw std::logic_error("Cannot reallocate meta data blocks.");
        }

        void do_free(raw_address<BlockSize> addr) override {
            parent->free_metadata_block(addr);
        }

    private:
        default_allocator* parent;
    };

    using free_list_t = detail::free_list<BlockSize>;

    using extents_t = btree<extent_t, extent_t_key, std::less<>, BlockSize>;
    using extents_iterator = typename extents_t::iterator;
    using extents_cursor = typename extents_t::cursor;

    using free_extents_t = btree<free_extent_t, identity_key, std::less<>, BlockSize>;
    using free_extents_iterator = typename free_extents_t::iterator;

public:
    class anchor {
        /// Number of blocks allocated for meta data.
        u64 meta_allocated = 0;

        /// Number of free metadata blocks.
        u64 meta_free = 0;

        /// Contains free metadata blocks. These are used for the internal datastructures (the btrees).
        typename free_list_t::anchor meta_freelist;

        /// Number of blocks allocated for data.
        u64 data_allocated = 0;

        /// Total number of data blocks that are currently unused.
        u64 data_free = 0;

        /// Maps every known extent address to its state.
        typename extents_t::anchor extents;

        /// Contains free extents.
        typename free_extents_t::anchor free_extents;

        friend class default_allocator;
    };

public:
    default_allocator(anchor_ptr<anchor> anch, engine<BlockSize>& e)
        : m_anchor(std::move(anch))
        , m_engine(&e)
        , m_file(&e.fd())
        , m_meta_freelist(m_anchor.neighbor(&m_anchor->meta_freelist), e)
        , m_meta_alloc(this)
        , m_extents(m_anchor.neighbor(&m_anchor->extents), *m_engine, m_meta_alloc)
        , m_free_extents(m_anchor.neighbor(&m_anchor->free_extents), *m_engine, m_meta_alloc)
    {}

    // The class contains children that point to itself.
    default_allocator(default_allocator&&) = delete;
    default_allocator& operator=(default_allocator&&) = delete;

    u64 data_free() const { return m_anchor->data_free; }
    u64 data_used() const { return data_total() - data_free(); }
    u64 data_total() const { return m_anchor->data_allocated; }

    u64 metadata_free() const { return m_anchor->meta_free; }
    u64 metadata_used() const { return metadata_total() - metadata_free(); }
    u64 metadata_total() const { return m_anchor->meta_allocated; }

    u32 min_chunk() const { return m_min_chunk; }
    void min_chunk(u32 chunk) { m_min_chunk = chunk; }

    u32 min_meta_chunk() const { return m_min_meta_chunk; }
    void min_meta_chunk(u32 chunk) { m_min_meta_chunk = chunk; }

    void debug_print(std::ostream& o) {
        fmt::print(o,
                   "Default allocator state: \n"
                   "  Data allocated:      {} blocks\n"
                   "  Data free:           {} blocks\n"
                   "  Metadata allocated:  {} blocks\n"
                   "  Metadata free:       {} blocks\n",
                   m_anchor->data_allocated,
                   m_anchor->data_free,
                   m_anchor->meta_allocated,
                   m_anchor->meta_free);
        fmt::print(o, "\n");

        fmt::print(o, "Allocated extents ({} total):\n", m_extents.size());
        for (const extent_t& e : m_extents) {
            fmt::print(o, "  Start: {}, Length: {}, Free: {}\n", e.block, e.size, e.free);
        }
        fmt::print(o, "\n");

        fmt::print(o, "Freelist entries ({} total):\n", m_free_extents.size());
        for (const free_extent_t& e : m_free_extents) {
            fmt::print(o , "  Start: {}, Length: {}\n", e.block, e.size);
        }
        o << std::flush;
    }

protected:
    raw_address<BlockSize> do_allocate(u64 request) override {
        // Find a free extent with at least `request` blocks.
        if (auto addr = allocate_best_fit(request))
            return addr;
        return allocate_new_space(request);
    }

    void do_free(raw_address<BlockSize> addr) override {
        extents_iterator pos = m_extents.find(addr.block_index());
        EXTPP_CHECK(pos != m_extents.end(),
                    "The pointer passed to free() does not point "
                    "to a previous allocation.");
        EXTPP_CHECK(!pos->free, "Double free detected.");
        // TODO: Can improve error reporting by detecting if a was
        // freed and the free range was merged with its predecessor.

        extent_t extent = *pos;
        extent.free = true;
        m_extents.erase(pos);

        add_free_extent(extent);
        m_anchor->data_free += extent.size;
        m_anchor.dirty();
    }

    raw_address<BlockSize> do_reallocate(raw_address<BlockSize> addr, u64 request) override {
        extents_cursor pos = m_extents.find(addr.block_index());
        EXTPP_CHECK(pos != m_extents.end(),
                    "The pointer passed to reallocate() does not point "
                    "to a previous allocation.");
        EXTPP_CHECK(!pos->free, "Calling reallocate() on a previously freed address.");

        // Size unchanged.
        if (request == pos->size)
            return addr;

        // Shrink.
        if (request < pos->size) {
            const u64 released = pos->size - request;
            m_extents.modify(pos, [&](extent_t& e) {
                e.size = request;
            });

            m_anchor->data_free += released;
            m_anchor.dirty();
            add_free_extent(extent_t(pos->block + pos->size, released, true));
            return addr;
        }

        // Try to grow the region in place.
        if (grow_in_place(pos, request - pos->size))
            return addr;

        // Otherwise, allocate a new chunk and copy the current data over.
        auto new_addr = do_allocate(request);
        copy_blocks(addr, new_addr, pos->size);
        do_free(addr);
        return new_addr;
    }

private:
    /// Try to serve a request by reusing an existing free extent.
    /// This implements the best fit strategy, with ties being broken
    /// using first fit strategy, i.e. the smallest fitting extent
    /// with the lowest address is chosen.
    raw_address<BlockSize> allocate_best_fit(u64 request) {
        auto free_pos = best_fit(request);
        if (free_pos == m_free_extents.end()) {
            return {};
        }
        auto pos = m_extents.find(free_pos->block);
        EXTPP_ASSERT(pos != m_extents.end(), "Extent was not found.");
        EXTPP_ASSERT(pos->free, "Block must be free since it was on the free list.");
        EXTPP_ASSERT(pos->block == free_pos->block, "Same block index.");
        EXTPP_ASSERT(pos->size == free_pos->size, "Same size.");

        const extent_t extent = *pos;
        m_extents.erase(pos);
        m_free_extents.erase(free_pos);
        return allocate_new_extent(extent, request);
    }

    /// Satisfies an allocation request by growing the underlying file.
    /// We either grow the extent with the highest address (if it is free and borders
    /// the end of the file) or we create a new extent.
    raw_address<BlockSize> allocate_new_space(u64 request) {
        if (m_anchor->meta_allocated == 0)
            allocate_metadata_chunk();

        extent_t extent = [&]{
            if (!m_extents.empty()) {
                auto pos = std::prev(m_extents.end());
                if (pos->free && extent_touches_file_end(*pos)) {
                    EXTPP_ASSERT(pos->size < request,
                                 "Extent should have been used by the best-fit allocation.");
                    remove_free(*pos);
                    extent_t result = *pos;
                    m_extents.erase(pos);
                    return result;
                }
            }
            return extent_t(file_size(), 0, true);
        }();

        const u64 allocated = allocate_data(extent, request - extent.size);
        extent.size += allocated;

        m_anchor->data_free += allocated;
        m_anchor.dirty();

        return allocate_new_extent(extent, request);
    }

    /// Allocates exactly `request` bytes from the extent pointed to by `pos`
    /// and then registers a new extent.
    raw_address<BlockSize> allocate_new_extent(const extent_t& extent, u64 request) {
        const u64 block = allocate_from_extent(extent, request);
        add_extent(extent_t(block, request, false));
        return raw_address<BlockSize>::from_block(block);
    }

    /// Allocates the first `request` blocks in the extent pointed to by `pos`.
    /// If the original extent is larger than `request`, then the remainder will
    /// be registered as another free extent.
    ///
    /// \note The new allocation will not yet be registered in the extent tree.
    u64 allocate_from_extent(const extent_t& extent, u64 request) {
        EXTPP_ASSERT(extent.free, "The extent must be free.");
        EXTPP_ASSERT(extent.size >= request, "Extent too small.");

        m_anchor->data_free -= request;
        m_anchor.dirty();

        if (extent.size > request) {
            const extent_t rem(extent.block + request, extent.size - request, true);
            add_extent(rem);
            add_free(rem);
        }
        return extent.block;
    }

    /// Tries to allocate `additional` blocks for the existing extent, without relocating it.
    /// Returns true if successful.
    bool grow_in_place(const extents_cursor& pos, u64 additional) {
        EXTPP_ASSERT(pos && pos != m_extents.end(), "Invalid cursor.");

        // If this is the rightmost extent, simply grow the file.
        if (extent_touches_file_end(*pos)) {
            const u64 allocated = allocate_data(*pos, additional);
            m_extents.modify(pos, [&](extent_t& e) {
                e.size += additional;
            });

            if (allocated > additional) {
                const extent_t rem(pos->block + pos->size, allocated - additional, true);
                add_extent(rem); // No merge neccessary since this is now the last extent.
                add_free(rem);

                m_anchor->data_free += rem.size;
                m_anchor.dirty();
            }
            return true;
        }

        // Growing might be possible if we have a free neighbor to our right.
        extents_cursor next_pos = std::next(pos);
        if (next_pos == m_extents.end() || !next_pos->free || !extents_touch(*pos, *next_pos))
            return false;

        // If the next extent is too small but touches the end of the file, then we can grow it.
        if (next_pos->size < additional && extent_touches_file_end(*next_pos)) {
            remove_free(*next_pos);
            const u64 allocated = allocate_data(*next_pos, additional - next_pos->size);
            m_extents.modify(next_pos, [&](extent_t& e) {
                e.size += allocated;
            });
            m_anchor->data_free += allocated;
            m_anchor.dirty();
        }
        // If the size is sufficient, then we can use it.
        else if (next_pos->size >= additional) {
            remove_free(*next_pos);
        }
        // Otherwise, there has to be a reallocation.
        else {
            return false;
        }

        const extent_t extent = *next_pos;
        m_extents.erase(next_pos);
        const u64 block = allocate_from_extent(extent, additional);
        EXTPP_ASSERT(block == pos->block + pos->size, "Block must be contiguous.");
        unused(block);

        m_extents.modify(pos, [&](extent_t& e) {
            e.size += additional;
        });
        return true;
    }

    /// Insert a new, free extent into the extent tree.
    /// The extent will be merged with its neighbors, if possible.
    void add_free_extent(extent_t extent) {
        EXTPP_ASSERT(extent.free, "Extent must be free.");
        EXTPP_ASSERT(m_extents.find(extent.block) == m_extents.end(), "Extent must not be registered.");

        if (extent.free && !m_extents.empty()) {
            extents_cursor next = m_extents.upper_bound(extent.block);
            extents_cursor prev = std::prev(next.iterator());

            if (next != m_extents.end() && next->free && extents_touch(extent, *next)) {
                extent.size += next->size;
                remove_free(*next);
                m_extents.erase(next);
            }
            if (prev != m_extents.end() && prev->free && extents_touch(*prev, extent)) {
                extent.block = prev->block;
                extent.size += prev->size;
                remove_free(*prev);
                m_extents.erase(prev);
            }
        }

        add_extent(extent);
        add_free(extent);
    }

    /// Allocates new space for data blocks at the end of the file.
    /// Returns the number of actually allocated blocks (>= additional).
    u64 allocate_data(const extent_t& e, u64 additional)  {
        EXTPP_ASSERT(extent_touches_file_end(e), "Extent must be at the end of the file.");
        unused(e);

        const u64 chunk = chunk_size(additional, m_min_chunk);
        const u64 block = allocate_chunk(chunk);
        EXTPP_ASSERT(block == e.block + e.size, "Unexpected block index (not contiguous).");
        unused(block);

        m_anchor->data_allocated += chunk;
        m_anchor.dirty();
        return chunk;
    }

    /// Allocate a new chunk of memory for metadata storage.
    void allocate_metadata_chunk() {
        const u64 chunk = chunk_size(2, m_min_meta_chunk);
        const u64 block = allocate_chunk(chunk);
        for (u64 b = block + chunk; b --> block;) {
            m_meta_freelist.push(raw_address<BlockSize>::from_block(b));
        }

        m_anchor->meta_allocated += chunk;
        m_anchor->meta_free += chunk;
        m_anchor.dirty();
    }

    /// Allocate a block for metadata storage.
    raw_address<BlockSize> allocate_metadata_block() {
        if (m_meta_freelist.empty())
            allocate_metadata_chunk();
        
        auto addr = m_meta_freelist.pop();
        m_anchor->meta_free -= 1;
        m_anchor.dirty();
        return addr;
    }

    /// Free a block used by the metadata structures.
    void free_metadata_block(raw_address<BlockSize> addr) {
        m_meta_freelist.push(addr);
        m_anchor->meta_free += 1;
        m_anchor.dirty();
    }

private:
    /// Copies the given number of blocks from `source` to `dest`.
    void copy_blocks(raw_address<BlockSize> source, raw_address<BlockSize> dest, u64 count) {
        u64 i = source.block_index();
        u64 j = dest.block_index();
        while (count-- > 0) {
            block_handle<BlockSize> in = m_engine->read(i++);
            m_engine->overwrite(j++, in.data());
        }
    }

    /// Returns the appropriate allocation size for the requested
    /// number of blocks.
    u64 chunk_size(u64 blocks, u32 minimum) const {
        EXTPP_ASSERT(blocks > 0, "Zero sized allocation.");

        if (blocks < (u64(1) << 63))
            blocks = round_towards_pow2(blocks);
        return std::max(u64(minimum), blocks);
    }

    /// Allocates a new chunk of exactly the size `blocks`
    /// and returns the index of the first block in that chunk.
    u64 allocate_chunk(u64 blocks) {
        const u64 chunk_start = file_size();
        set_file_size(chunk_start + blocks);
        return chunk_start;
    }

    /// Add a new extent to the extents tree. Addresses (`e.block`) are unique.
    auto add_extent(const extent_t& e) {
        EXTPP_ASSERT(e.size > 0, "Cannot register zero-sized extents.");

        extents_iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_extents.insert(e);
        EXTPP_ASSERT(inserted, "extent entry was not inserted.");
        return pos;
    }

    /// Add a new extent to the free list. It is an error if an entry for `e`
    /// already exists.
    auto add_free(const extent_t& e) {
        EXTPP_ASSERT(e.free, "Extent must be free.");
        free_extent_t f;
        f.block = e.block;
        f.size = e.size;

        free_extents_iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_free_extents.insert(f);
        EXTPP_ASSERT(inserted, "free extent entry was not inserted.");
        return pos;
    }

    /// Removes an extent from the free list. It is an error if the
    /// entry does not exist.
    void remove_free(const extent_t& e) {
        free_extent_t key;
        key.size = e.size;
        key.block = e.block;
        bool erased = m_free_extents.erase(key);
        (void) erased;
        EXTPP_ASSERT(erased, "free extent was not found.");
    }

    /// Returns the iterator to an entry on the free list that is able
    /// to satisfy a request of `n` blocks. Returns `end()` if no such entry exists.
    auto best_fit(u64 n) const {
        if (n == 1) {
            // 1 is the lowest possible free extent size, thus the first
            // entry (if any) is the best fit.
            return m_free_extents.begin();
        }

        free_extent_t key;
        key.size = n;
        key.block = 0;
        return m_free_extents.lower_bound(key);
    }

    /// Returns the file size in blocks.
    u64 file_size() {
        u64 size = m_file->file_size();
        if (EXTPP_UNLIKELY(size % BlockSize != 0))
            throw std::logic_error("Current file size is not a multiple of the block size.");
        return size / BlockSize;
    }

    /// Sets the file size in blocks.
    void set_file_size(u64 blocks) {
        // TODO Overflow check.
        m_file->truncate(blocks * BlockSize);
    }

    /// Returns true iff `left...right` forms a contiguous region.
    bool extents_touch(const extent_t& left, const extent_t& right) {
        return left.block + left.size == right.block;
    }

    /// Returns true iff `e` borders the end of the file.
    bool extent_touches_file_end(const extent_t& e) {
        return e.block + e.size == file_size();
    }

private:
    anchor_ptr<anchor> m_anchor;
    engine<BlockSize>* m_engine;
    file* m_file;

    /// Minimum allocation size for data blocks on file truncation.
    u32 m_min_chunk = 128;

    /// Minimum allocation size for meta data blocks on file truncation.
    u32 m_min_meta_chunk = 16;

    /// Free list for metadata blocks.
    free_list_t m_meta_freelist;

    /// Allocator for internal datastructures.
    metadata_allocator m_meta_alloc;

    /// Contains one entry for every allocated extent (free or not).
    extents_t m_extents;

    /// Contains one entry for every free extent (for best fit allocation).
    free_extents_t m_free_extents;
};

} // namespace extpp

#endif // EXTPP_DEFAULT_ALLOCATOR_HPP
