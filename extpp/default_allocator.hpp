#ifndef EXTPP_DEFAULT_ALLOCATOR_HPP
#define EXTPP_DEFAULT_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/handle.hpp>
#include <extpp/identity_key.hpp>

#include <extpp/detail/free_list.hpp>

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
    };

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

        raw_address<BlockSize> allocate(u64 n) override {
            if (n != 1)
                throw std::logic_error("Cannot allocate sizes other than 1.");

            return parent->allocate_metadata_block();
        }

        void free(raw_address<BlockSize> addr) override {
            parent->free_metadata_block(addr);
        }

    private:
        default_allocator* parent;
    };

    using free_list_t = detail::free_list<BlockSize>;

    using extents_t = btree<extent_t, extent_t_key, std::less<>, BlockSize>;

    using free_extents_t = btree<free_extent_t, identity_key, std::less<>, BlockSize>;

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
    default_allocator(handle<anchor, BlockSize> anch, engine<BlockSize>& e, u32 min_chunk = 128)
        : m_anchor(std::move(anch))
        , m_engine(&e)
        , m_file(&e.fd())
        , m_min_chunk(std::max(min_chunk, u32(1)))
        , m_meta_freelist(m_anchor.neighbor(&m_anchor->meta_freelist), e)
        , m_meta_alloc(this)
        , m_extents(m_anchor.neighbor(&m_anchor->extents), m_engine, m_meta_alloc)
        , m_free_extents(m_anchor.neighbor(&m_anchor->free_extents), m_engine, m_meta_alloc)
    {}

    // The class contains children that point to itself.
    default_allocator(default_allocator&&) = delete;
    default_allocator& operator=(default_allocator&&) = delete;

    raw_address<BlockSize> allocate(u64 request) override {
        // Find a free extent with at least `request` blocks.
        if (auto addr = allocate_from_freelist(request))
            return addr;
        // Otherwise, resize the last free chunk (if possible).
        if (auto addr = allocate_from_wilderness(request))
            return addr;
        // Otherwise, request new space from the file.
        return allocate_from_file(request);
    }

    void free(raw_address<BlockSize> a) override {
        EXTPP_CHECK(a, "The address passed to free() is invalid.");
        EXTPP_CHECK(a.block_offset() == 0, "The address passed to free() does not point to a block.");

        u64 block = a.block_index();
        auto pos = m_extents.find(block);
        EXTPP_CHECK(pos != m_extents.end(),
                    "The pointer passed to free() does not point "
                    "to a previous allocation.");
        EXTPP_CHECK(!pos->free, "Double free detected.");

        // Try to merge the newly freed extent with its immediate neighbors.
        // if (merge_free_extents(pos))
        //   return;
        // TODO

        // If that was not possible, flip its free bit and put it into the freelist.
        m_extents.modify(pos, [](extent_t& e) {
            e.free = true;
        });
        add_free(pos->block, pos->size);
    }

private:
    // Try to serve a request by reusing an existing free extent.
    // This implements the best fit strategy, with ties being broken
    // using first fit strategy, i.e. the smallest fitting extent
    // with the lowest address is chosen.
    raw_address<BlockSize> allocate_from_freelist(u64 request) {
        auto free_pos = find_free(request);
        if (free_pos == m_free_extents.end())
            return {};

        auto extent_pos = m_extents.find(free_pos->block);
        EXTPP_ASSERT(extent_pos != m_extents.end(), "Free extent was not found in extents tree.");
        EXTPP_ASSERT(extent_pos->free, "The extent must be free.");

        const u64 block = extent_pos->block;
        const u64 size = extent_pos->size;

        // Modify the existing extent's size and free state.
        m_extents.modify(extent_pos, [&](auto& e) {
            e.free = false;
            e.size = request;
        });
        m_free_extents.erase(free_pos);

        // If the extent's original size was larger, register the remainder as another free extent.
        if (request < size) {
            u64 rem_block = block + request;
            u64 rem_size = size - request;
            add_extent(rem_block, rem_size, true);
            add_free(rem_block, rem_size);
        }

        m_anchor->data_free -= request;
        m_anchor.dirty();
        return raw_address<BlockSize>::from_block(block);
    }

    // Try to serve a request by resizing free chunk at the end
    // of the allocated address space.
    raw_address<BlockSize> allocate_from_wilderness(u64 request) {
        return {};
    }

    // Serve a request by allocating new space from the underlying file.
    raw_address<BlockSize> allocate_from_file(u64 request) {
        // We allocate space in chunks from the underlying file.
        // Individual allocations might be greater than the requested size;
        // the remainder gets added to the freelist.
        const u64 chunk = chunk_size(request);
        const u64 block = allocate_chunk(chunk);
        m_anchor->data_allocated += chunk;
        m_anchor.dirty();

        if (chunk > request) {
            u64 rem_block = block + request;
            u64 rem_size = chunk - request;
            add_extent(rem_block, rem_size, true);
            add_free(rem_block, rem_size);

            m_anchor->data_free += rem_size;
            m_anchor.dirty();
        }

        add_extent(block, request, false);
        return raw_address<BlockSize>::from_block(block);
    }

    // Allocate a block for metadata storage.
    raw_address<BlockSize> allocate_metadata_block() {
        if (!m_meta_freelist.empty())
            return m_meta_freelist.pop();

        u64 chunk = chunk_size(1);
        u64 block = allocate_chunk(chunk);
        for (u64 b = block + chunk - 1; b > block; --b)
            m_meta_freelist.push(raw_address<BlockSize>::from_block(b));

        m_anchor->meta_allocated += chunk;
        m_anchor->meta_free += chunk - 1;
        m_anchor.dirty();
        return raw_address<BlockSize>::from_block(block);
    }

    // Free a block used by the metadata structures.
    void free_metadata_block(raw_address<BlockSize> addr) {
        m_meta_freelist.push(addr);
        m_anchor->meta_free += 1;
        m_anchor.dirty();
    }

private:
    u64 chunk_size(u64 blocks) const {
        if (blocks < (u64(1) << 63))
            blocks = round_towards_pow2(blocks);
        return std::max(u64(m_min_chunk), blocks);
    }

    /// Allocates a new chunk of exactly the size `blocks`
    /// and returns the index of the first block in that chunk.
    u64 allocate_chunk(u64 blocks) {
        // TODO Exception types.

        u64 oldsize = m_file->file_size();
        if (oldsize % BlockSize != 0)
            throw std::logic_error("Current file size is not a multiple of the block size.");

        // TODO Overflow check.
        u64 newsize = oldsize + blocks * BlockSize;
        m_file->truncate(newsize);

        m_anchor->total_allocated += blocks;
        m_anchor.dirty();

        return oldsize / BlockSize;
    }

    void add_extent(u64 block, u64 size, bool free) {
        extent_t a;
        a.block = block;
        a.size = size;
        a.free = free;

        bool inserted;
        std::tie(std::ignore, inserted) = m_extents.insert(a);
        EXTPP_ASSERT(inserted, "extent entry was not inserted.");
    }

    void add_free(u64 block, u64 size) {
        free_extent_t f;
        f.block = block;
        f.size = size;

        bool inserted;
        std::tie(std::ignore, inserted) = m_free_extents.insert(f);
        EXTPP_ASSERT(inserted, "free extent entry was not inserted.");
    }

    auto find_free(u64 n) const {
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

private:
    handle<anchor, BlockSize> m_anchor;
    engine<BlockSize>* m_engine;
    file* m_file;

    /// Minumum number of blocks to allocate on each truncation of the native file.
    u32 m_min_chunk;

    /// Free list for metadata blocks.
    /// These blocks are used to form the btrees that
    /// manage all extents.
    free_list_t m_meta_freelist;

    /// Allocator for internal datastructures.
    metadata_allocator m_meta_alloc;

    /// Manages all known extents. Free neighboring extents are merged together
    /// in order to fight fragmentation. The btree is indexed by address.
    extents_t m_extents;

    /// Manages all free extents. The btree is indexed by size (in order to find
    /// fitting extents quickly) with ties broken by address (favouring lower addresses
    /// can reduce fragmentation).
    free_extents_t m_free_extents;
};

} // namespace extpp

#endif // EXTPP_DEFAULT_ALLOCATOR_HPP
