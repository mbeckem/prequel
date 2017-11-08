#ifndef EXTPP_DEFAULT_ALLOCATOR_HPP
#define EXTPP_DEFAULT_ALLOCATOR_HPP

#include <extpp/allocator.hpp>
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
        , m_extents(m_anchor.neighbor(&m_anchor->extents), *m_engine, m_meta_alloc)
        , m_free_extents(m_anchor.neighbor(&m_anchor->free_extents), *m_engine, m_meta_alloc)
    {}

    // The class contains children that point to itself.
    default_allocator(default_allocator&&) = delete;
    default_allocator& operator=(default_allocator&&) = delete;

protected:
    raw_address<BlockSize> do_allocate(u64 request) override {
        // Find a free extent with at least `request` blocks.
        if (auto addr = allocate_from_freelist(request))
            return addr;
        return allocate_from_wilderness(request);
    }

    raw_address<BlockSize> do_reallocate(raw_address<BlockSize> addr, u64 request) override {
//        auto pos = m_extents.find(addr.block_index());
//        EXTPP_CHECK(pos != m_extents.end(),
//                    "The pointer passed to reallocate() does not point "
//                    "to a previous allocation.");
//        EXTPP_CHECK(!pos->free, "Calling reallocate() on a previously freed address.");

//        // Size unchanged.
//        if (request == pos->size)
//            return addr;

//        const u64 block = pos->block;
//        const u64 size = pos->size;

//        // Shrink.
//        if (request < size) {
//            m_extents.modify(pos, [&](extent_t& e) {
//                e.size = request;
//            });
//            add_free_block_after(std::move(pos), block + request, size - request);
//            return addr;
//        }

//        // Additional memory is required.
//        const u64 additional = request - size;

//        // Try to take memory from the immediate neighbor to the right.
//        auto next_pos = std::next(pos);
//        if (next_pos != m_extents.end() && next_pos->free && block + size == next_pos->block) {
//            m_extents.modify(pos, [&](extent_t& e) {
//                e.size = request;
//            });
//        }

//        // If this is the rightmost allocation, simply grow the file.
//        if (next_pos == m_extents.end() && block + size == file_size()) {
//            const u64 new_chunk = chunk_size(additional);
//            const u64 new_block = allocate_chunk(new_chunk);

//            m_extents.modify(pos, [&](extent_t& e) {
//                e.size = request;
//            });
//            if (new_chunk > additional)
//                add_free_block_after(std::move(next_pos), new_block + additional, new_chunk - additional);
//            return addr;
//        }

        // TODO
        unused(addr, request);
        EXTPP_UNREACHABLE("Not implemented");
    }

    void do_free(raw_address<BlockSize> addr) override {
        auto pos = m_extents.find(addr.block_index());
        EXTPP_CHECK(pos != m_extents.end(),
                    "The pointer passed to free() does not point "
                    "to a previous allocation.");
        EXTPP_CHECK(!pos->free, "Double free detected.");
        // TODO: Can improve error reporting by detecting if a was
        // freed and the free range was merged with its predecessor.

        m_extents.modify(pos, [](auto& e) {
            e.free = true;
        });
        m_anchor->data_free += pos->size;
        m_anchor.dirty();

        // Merge with predecessor.
        {
            auto prev_pos = std::prev(pos);
            if (prev_pos != m_extents.end() && prev_pos->free && prev_pos->block + prev_pos->size == pos->block) {
                remove_free(prev_pos->block, prev_pos->size);
                m_extents.modify(prev_pos, [&](extent_t& prev) {
                    prev.size += pos->size;
                });
                pos = m_extents.erase(pos);
                --pos;
            }
        }

        // Merge with successor.
        {
            auto next_pos = std::next(pos);
            if (next_pos != m_extents.end() && next_pos->free && pos->block + pos->size == next_pos->block) {
                remove_free(next_pos->block, next_pos->size);
                m_extents.modify(pos, [&](extent_t& e) {
                    e.size += next_pos->size;
                });
                pos = m_extents.erase(next_pos);
                --pos;
            }
        }

        add_free(pos->block, pos->size);
    }

public:
    void debug_stats(std::ostream& o) {
        fmt::print(o,
                   "Default allocator state: \n"
                   "  data allocated:      {} blocks\n"
                   "  data free:           {} blocks\n"
                   "  metadata allocated:  {} blocks\n"
                   "  metadata free:       {} blocks\n",
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

        fmt::print(o, "Freelist entries ({} total)\n", m_free_extents.size());
        for (const free_extent_t& e : m_free_extents) {
            fmt::print(o , "  Length: {}, Start: {}\n", e.size, e.block);
        }
        o << std::flush;
    }

private:
    /// Try to serve a request by reusing an existing free extent.
    /// This implements the best fit strategy, with ties being broken
    /// using first fit strategy, i.e. the smallest fitting extent
    /// with the lowest address is chosen.
    raw_address<BlockSize> allocate_from_freelist(u64 request) {
        auto free_pos = best_fit(request);
        if (free_pos == m_free_extents.end())
            return {};

        auto extent_pos = m_extents.find(free_pos->block);
        return allocate_from_freelist(std::move(extent_pos), std::move(free_pos), request);
    }

    /// Uses the given extent (and its position on the free list)
    /// to satisfy an allocation request.
    raw_address<BlockSize> allocate_from_freelist(
            typename extents_t::iterator extent_pos,
            typename free_extents_t::iterator free_pos,
            u64 request)
    {
        EXTPP_ASSERT(extent_pos != m_extents.end(), "Invalid extent_pos.");
        EXTPP_ASSERT(free_pos != m_free_extents.end(), "Invalid free_pos.");
        EXTPP_ASSERT(extent_pos->free, "The extent must be free.");
        EXTPP_ASSERT(extent_pos->block == free_pos->block, "Extent starts do not match.");
        EXTPP_ASSERT(extent_pos->size == free_pos->size, "Extent sizes to not match.");
        EXTPP_ASSERT(extent_pos->size >= request, "Extent too small.");

        const u64 block = extent_pos->block;
        const u64 size = extent_pos->size;

        // Modify the existing extent's size and free state.
        m_extents.modify(extent_pos, [&](auto& e) {
            e.free = false;
            e.size = request;
        });
        m_free_extents.erase(free_pos);

        // If the extent's original size was larger, register the remainder as another free extent.
        // If the immediate successor is also free, merge the two extents.
        if (size > request) {
            u64 rem_block = block + request;
            u64 rem_size = size - request;

            ++extent_pos;
            if (extent_pos != m_extents.end() && extent_pos->block == (rem_block + rem_size) && extent_pos->free) {
                rem_size += extent_pos->size;
                remove_free(extent_pos->block, extent_pos->size);
                m_extents.erase(extent_pos);
            }
            add_extent(rem_block, rem_size, true);
            add_free(rem_block, rem_size);
        }

        m_anchor->data_free -= request;
        m_anchor.dirty();
        return raw_address<BlockSize>::from_block(block);
    }

    /// Satisfies an allocation request by growing the underlying file.
    raw_address<BlockSize> allocate_from_wilderness(u64 request) {
        auto tup = ensure_wilderness(request);
        return allocate_from_freelist(std::move(std::get<0>(tup)),
                                      std::move(std::get<1>(tup)),
                                      request);
    }

    /// Ensure that there is an extent at the end of the addressable space
    /// that can satisfiy the allocation request.
    /// Returns a tuple (epos, fpos) where epos is the position of the extents's metadata
    /// in m_extents and fpos is the position of the extent in m_free_extents.
    auto ensure_wilderness(u64 request) {
        // Try to enlarge the extent with the highest address.
        // This only works if the extent is free and it touches the current end of the file.
        if (!m_extents.empty()) {
            auto wild_pos = std::prev(m_extents.end());
            if (wild_pos->free && wild_pos->block + wild_pos->size == file_size()) {
                EXTPP_ASSERT(wild_pos->size < request,
                             "Extent should have been used by the freelist allocation.");
                remove_free(wild_pos->block, wild_pos->size);

                request -= wild_pos->size;
                const u64 chunk = chunk_size(request - wild_pos->size);
                const u64 block = allocate_chunk(chunk);
                m_anchor->data_allocated += chunk;
                m_anchor->data_free += chunk;
                m_anchor.dirty();

                EXTPP_ASSERT(block == wild_pos->block + wild_pos->size,
                             "Extents are contiguous.");
                m_extents.modify(wild_pos, [&](extent_t& e) {
                    e.size += chunk;
                });

                auto free_pos = add_free(wild_pos->block, wild_pos->size);
                return std::make_tuple(std::move(wild_pos), std::move(free_pos));
            }
        }

        // Allocate a new extent at the end of the file.
        const u64 chunk = chunk_size(request);
        const u64 block = allocate_chunk(chunk);
        m_anchor->data_allocated += chunk;
        m_anchor->data_free += chunk;
        m_anchor.dirty();

        auto wild_pos = add_extent(block, chunk, true);
        auto free_pos = add_free(block, chunk);
        return std::make_tuple(std::move(wild_pos), std::move(free_pos));
    }

    /// Allocate a block for metadata storage.
    raw_address<BlockSize> allocate_metadata_block() {
        if (!m_meta_freelist.empty()) {
            auto addr = m_meta_freelist.pop();
            m_anchor->meta_free -= 1;
            m_anchor.dirty();
            return addr;
        }

        u64 chunk = chunk_size(1);
        u64 block = allocate_chunk(chunk);
        for (u64 b = block + chunk - 1; b > block; --b)
            m_meta_freelist.push(raw_address<BlockSize>::from_block(b));

        m_anchor->meta_allocated += chunk;
        m_anchor->meta_free += chunk - 1;
        m_anchor.dirty();
        return raw_address<BlockSize>::from_block(block);
    }

    /// Free a block used by the metadata structures.
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
        const u64 chunk_start = file_size();
        set_file_size(chunk_start + blocks);
        return chunk_start;
    }

    /// Add a new extent to the extents tree. Addresses (`block`) are unique.
    auto add_extent(u64 block, u64 size, bool free) {
        extent_t a;
        a.block = block;
        a.size = size;
        a.free = free;

        typename extents_t::iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_extents.insert(a);
        EXTPP_ASSERT(inserted, "extent entry was not inserted.");
        return pos;
    }

    /// Add a new extent to the free list.
    auto add_free(u64 block, u64 size) {
        free_extent_t f;
        f.block = block;
        f.size = size;

        typename free_extents_t::iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_free_extents.insert(f);
        EXTPP_ASSERT(inserted, "free extent entry was not inserted.");
        return pos;
    }

    /// Removes an extent from the free list. It is an error if the
    /// entry does not exist.
    void remove_free(u64 block, u64 size) {
        free_extent_t key;
        key.size = size;
        key.block = block;
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
