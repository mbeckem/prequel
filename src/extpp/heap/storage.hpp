#ifndef EXTPP_HEAP_STORAGE_HPP
#define EXTPP_HEAP_STORAGE_HPP

#include <extpp/heap/base.hpp>

#include <extpp/address.hpp>
#include <extpp/block_index.hpp>
#include <extpp/btree.hpp>
#include <extpp/engine.hpp>
#include <extpp/detail/deferred.hpp>

namespace extpp::heap_detail {

class storage {
public:
    struct chunk_entry {
        block_index block;      // First block of this chunk.
        u64 large_object: 1;    // Large object chunks contain only one object.
        u64 unused: 1;
        u64 size: 62;           // Number of blocks in this chunk.

        chunk_entry(): block(), large_object(0), unused(0), size(0) {}

        chunk_entry(block_index block, u64 size, bool large_object)
            : block(block), size(size), large_object(large_object), unused(0)
        {
            EXTPP_ASSERT(block, "Invalid block.");
            EXTPP_ASSERT(size, "Zero sized chunk.");
        }

        struct derive_key {
            block_index operator()(const chunk_entry& entry) const {
                return entry.block;
            }
        };

        struct binary_serializer {
            static constexpr size_t serialized_size() {
                return 16;
            }

            static void serialize(const chunk_entry& e, byte* b) {
                b = extpp::serialize(e.block, b);

                u64 u = (e.large_object ? u64(1) << 63 : 0) | e.size;
                extpp::serialize(u, b);
            }

            static void deserialize(chunk_entry& e, const byte* b) {
                b = extpp::deserialize(e.block, b);

                u64 u;
                extpp::deserialize(u, b);
                e.large_object = u & (u64(1) << 63) ? true : false;
                e.unused = 0;
                e.size = u & ((u64(1) << 62) - 1);
            }
        };
    };

    u64 cell_index(const chunk_entry& entry, raw_address cell) const {
        EXTPP_ASSERT(cell.valid(), "Cell address must be valid.");
        EXTPP_ASSERT(cell.value() % cell_size == 0, "Cell address must be a multiple of the cell size.");
        EXTPP_ASSERT(cell >= get_engine().to_address(entry.block)
                     && cell < get_engine().to_address(entry.block + entry.size),
                     "Cell address not in chunk.");
        return distance(cell, get_engine().to_address(entry.block)) >> cell_size_log;
    }

    u64 cell_count(const chunk_entry& entry) const {
        return entry.size * m_cells_per_block;
    }


public:
    using chunk_tree = btree<chunk_entry, chunk_entry::derive_key>;

    using anchor = chunk_tree::anchor;

public:
    storage(anchor_handle<anchor> _anchor, extpp::allocator& _alloc)
        : m_engine(&_alloc.get_engine())
        , m_chunk_tree(std::move(_anchor), _alloc)
    {
        u32 block_size = m_engine->block_size();
        if (block_size % cell_size != 0)
            EXTPP_THROW(bad_argument("The block size must be a multiple of the cell size."));
        m_cells_per_block = block_size >> cell_size;
    }

    /**
     * Allocate a new chunk of `size` blocks and insert it into the tree.
     */
    chunk_entry allocate(u64 size, bool large_object) {
        block_index block = get_allocator().allocate(size);
        detail::deferred cleanup = [&]{
            get_allocator().free(block);
        };

        chunk_entry entry(block, size, large_object);
        auto result = m_chunk_tree.insert(entry);
        EXTPP_ASSERT(result.inserted, "Chunk address must be unique.");
        unused(result);

        cleanup.disable();
        return entry;
    }

    /**
     * Frees the given chunk. Can only be called when nothing inside the chunk is referenced
     * anymore by the application.
     */
    void free(const chunk_entry& entry) {
        auto cursor = m_chunk_tree.find(entry.block);
        EXTPP_ASSERT(cursor, "Chunk must exist in the tree.");
        cursor.erase();
        get_allocator().free(entry.block);
    }

    engine& get_engine() const { return *m_engine; }
    allocator& get_allocator() const { return m_chunk_tree.get_allocator(); }

private:
    extpp::engine* m_engine;
    u32 m_cells_per_block;
    chunk_tree m_chunk_tree;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_STORAGE_HPP
