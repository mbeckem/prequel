#ifndef EXTPP_HEAP_STORAGE_HPP
#define EXTPP_HEAP_STORAGE_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/heap/base.hpp>

#include <optional>

namespace extpp::heap_detail {

// Note: it would likely be an improvement to partition the heap further into
// chunks for objects that *might* contain references to other objects
// and plain data-chunks that *never* contain references.
// Data-chunks would never have to be visited during marking, only their
// marking bitmaps would be accessed.
template<u32 BlockSize>
class storage {
private:
    static_assert(BlockSize >= cell_size, "BlockSize must not be smaller than the cell size.");
    static_assert(BlockSize % cell_size == 0, "BlockSize must be multiple of the cell size");

public:
    static constexpr u64 cells_per_block = BlockSize / cell_size;

public:
    struct chunk_entry {
        raw_address<BlockSize> addr;    // First block in this chunk.
        u64 blocks: 62;                 // Total number of blocks.
        u64 large_object: 1;            // True if this is a large object chunk.
        u64 unused: 1;

        chunk_entry() = default;

        chunk_entry(raw_address<BlockSize> addr, u64 blocks, bool large)
            : addr(addr), blocks(blocks), large_object(large), unused(0)
        {}

        raw_address<BlockSize> begin_addr() const { return addr; }
        raw_address<BlockSize> end_addr() const { return addr + (blocks * BlockSize); }

        /// Returns the index of the given cell within this chunk.
        u64 cell_index(address<cell, BlockSize> addr) const {
            EXTPP_ASSERT(addr.raw() >= begin_addr() && addr.raw() < end_addr(),
                         "Address out of bounds.");
            return distance(raw_address_cast<cell>(begin_addr()), addr);
        }

        u64 cell_count() const { return blocks * cells_per_block; }

        struct key_extract {
            raw_address<BlockSize> operator()(const chunk_entry& c) const { return c.addr; }
        };
    };

    using chunk_tree = extpp::btree<chunk_entry, typename chunk_entry::key_extract, std::less<>, BlockSize>;
    using chunk_iterator = typename chunk_tree::iterator;

    using anchor = typename chunk_tree::anchor;

public:
    storage(anchor_ptr<anchor> anc, allocator<BlockSize>& alloc)
        : m_tree(std::move(anc), alloc)
    {}

    engine<BlockSize>& get_engine() const { return m_tree.get_engine(); }
    allocator<BlockSize>& get_allocator() const { return m_tree.get_allocator(); }

    chunk_iterator begin() const { return m_tree.begin(); }
    chunk_iterator end() const { return m_tree.end(); }

    /// Number of allocated chunks.
    u64 chunk_count() const { return m_tree.size(); }

    /// Finds the entry for the chunk that starts at the given address.
    std::optional<chunk_entry> find_chunk_exact(raw_address<BlockSize> addr) const {
        auto pos = m_tree.find(addr);
        return pos == m_tree.end() ? std::nullopt : std::make_optional(*pos);
    }

    /// Finds the entry for the chunk that contains the object with the given address.
    /// In other words, returns the chunk `c` with `c.addr >= addr && addr < c.addr + c.blocks * BlockSize`.
    // TODO: Actually not required at the moment.
//    std::optional<chunk_entry> find_chunk(raw_address<BlockSize> addr) const {

//    }

    /// Allocates a new chunk, inserts it into the tree and returns the new entry.
    /// Invalidates iterators.
    chunk_entry allocate(u64 blocks, bool large_object) {
        auto addr = get_allocator().allocate(blocks);

        chunk_entry entry(addr, blocks, large_object);
        auto [pos, inserted] = m_tree.insert(entry);
        EXTPP_ASSERT(inserted, "Chunk address not unique.");
        unused(pos, inserted);
        return entry;
    }

    /// Free's the chunk and invalidates iterators.
    void free(const chunk_entry& entry) {
        bool removed = m_tree.erase(entry.addr);
        EXTPP_CHECK(removed, "Chunk with that address was not allocated by this instance.");
        get_allocator().free(entry.addr);
    }

private:
    chunk_tree m_tree;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_STORAGE_HPP
