#ifndef EXTPP_HEAP_GC_HPP
#define EXTPP_HEAP_GC_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/detail/bitset.hpp>
#include <extpp/detail/rollback.hpp>
#include <extpp/heap/storage.hpp>
#include <extpp/heap/object_access.hpp>
#include <extpp/heap/object_table.hpp>
#include <extpp/heap/type_set.hpp>

#include <functional>
#include <utility>
#include <vector>

namespace extpp::heap_detail {

template<u32 BlockSize>
class gc_data {
public:
    using storage_type = storage<BlockSize>;

    using storage_chunk_entry = typename storage_type::chunk_entry;

public:
    struct entry {
        /// The chunk represented by this entry.
        storage_chunk_entry chunk;

        /// True if at least one cell is this chunk is marked as "live".
        bool marked = false;

        /// One bit for every cell in the chunk. Live cells are set to 1.
        detail::bitset bitmap;

        /// Only used when compacting.
        std::vector<u64> relocations;

        entry() = default;

        entry(const entry&) = delete;
        entry& operator=(const entry&) = delete;

        entry(entry&&) noexcept = default;
        entry& operator=(entry&&) noexcept = default;
    };

public:
    explicit gc_data(storage_type& storage)
        : m_storage(storage)
    {
        m_entries.reserve(storage.chunk_count());

        // Note: storage is sorted by address since it uses a btree.
        for (const storage_chunk_entry& sc : storage) {
            entry& e = m_entries.emplace_back();
            e.chunk = sc;
            e.bitmap.resize(sc.blocks * storage_type::cells_per_block);
        }
    }

    auto begin() const { return m_entries.begin(); }
    auto end() const { return m_entries.end(); }

    auto begin() { return m_entries.begin(); }
    auto end() { return m_entries.end(); }


    /// Returns the chunk entry for that object, i.e. the chunk that contains that object.
    const entry& entry_for(raw_address<BlockSize> object) const {
        auto pos = std::upper_bound(m_entries.begin(), m_entries.end(), object,
                                    [&](const raw_address<BlockSize>& obj, const entry& entry) {
            return obj < entry.chunk.addr;
        });
        EXTPP_ASSERT(pos != m_entries.begin(), "No chunk for that object.");
        --pos;

        const entry& e = *pos;
        EXTPP_ASSERT(e.chunk.addr <= object && e.chunk.addr + e.chunk.blocks * BlockSize >= object,
                     "Object not in range for chunk.");
        return e;
    }

    entry& entry_for(raw_address<BlockSize> object) {
        return const_cast<entry&>(const_cast<const gc_data*>(this)->entry_for(object));
    }


private:
    storage_type& m_storage;

    /// One entry for every chunk in the storage.
    /// Ordered by address. Contains marking bitmaps and
    /// other runtime state required by the garbage collector.
    std::vector<entry> m_entries;
};

template<u32 BlockSize>
class mark_pass {
public:
    using storage_type = storage<BlockSize>;

    using gc_data_type = gc_data<BlockSize>;
    using chunk_entry = typename gc_data_type::entry;

    using object_access_type = object_access<BlockSize>;
    using object_header_type = typename object_access_type::object_header;

    using object_table_type = object_table<BlockSize>;
    using object_entry_type = object_entry<BlockSize>;

public:
    explicit mark_pass(gc_data_type& chunks, object_access_type& access, object_table_type& table, const type_set& types)
        : m_chunks(chunks)
        , m_access(access)
        , m_table(table)
        , m_types(types)
    {}

    void visit(reference ref) {
        EXTPP_ASSERT(m_stack.empty(), "The stack must be empty.");

        struct visitor_t : reference_visitor {
            mark_pass* mark;

            visitor_t(mark_pass* mark): mark(mark) {}

            void visit(reference ref) override {
                mark->visit_reference(ref);
            };
        } visitor(this);

        visit_reference(ref);
        while (!m_stack.empty()) {
            auto [ref, type] = m_stack.back();
            m_stack.pop_back();

            EXTPP_ASSERT(type->visit_references, "Type has no references.");
            type->visit_references(ref, visitor);
        }
    }

private:
    void visit_reference(reference ref) {
        if (!ref)
            return;

        EXTPP_CHECK(m_table.valid(ref),
                    "Encountered an invalid reference during garbage collection. This means that "
                    "you are holding on to a destroyed object because you did not visit "
                    "it during garbage collection or that your storage is corrupted.");

        object_entry_type obj = m_table[ref];
        raw_address<BlockSize> addr = obj.get_address();
        object_header_type header = m_access.read_header(addr, m_types);
        u64 cells = ceil_div(header.total_size(), cell_size);
        if (mark(addr, cells) && header.type->visit_references) {
            m_stack.emplace_back(ref, header.type);
        }
    }

    /// Marks the object at the given address (which consists of the given number of cells)
    /// as live. Returns true if the object was previously unmarked, i.e. if this is the first
    /// time the object was marked.
    bool mark(raw_address<BlockSize> object, u64 cells) {
        chunk_entry& ent = m_chunks.entry_for(object);
        EXTPP_ASSERT(object + cells * cell_size <= ent.chunk.end_addr(),
                     "Object must be fully contained in chunk.");

        // Large objects use the "marked" flag and nothing else.
        if (ent.chunk.large_object) {
            if (ent.marked)
                return false;

            ent.marked = true;
            return true;
        }

        if (!ent.marked)
            ent.marked = true;

        // Mark all cells that belong to the object.
        u64 index = ent.chunk.cell_index(raw_address_cast<cell>(object));
        if (ent.bitmap.test(index))
            return false; // We have been here before, no need to mark all cells again.

        // TODO: Bulk set?
        for (u64 i = 0; i < cells; ++i)
            ent.bitmap.set(index + i);
        return true;
    }

private:
    gc_data_type& m_chunks;
    object_access_type& m_access;
    object_table_type& m_table;
    const type_set& m_types;

    /// Stack for recursive marking of objects.
    std::vector<std::tuple<reference, const type_info*>> m_stack;
};

/// Iterate over the object table and free those slots that have not been marked.
// TODO: Most objects do not have finalizers, but this still loops over all of them.
template<u32 BlockSize>
void sweep_table(object_table<BlockSize>& table, gc_data<BlockSize>& chunks, object_access<BlockSize> access, const type_set& types) {
    // Note: The data access pattern in the following code is not optimal,
    // because we iterate in table-order (i.e. pretty much arbitrary).
    // If we could iterate in address-order instead, we would only have to load
    // every bitmap once, iterate over all objects that belong to a chunk,
    // and then load the next bitmap.
    // That would however involve somehow (a) traversing the heap blocks in linear order
    // (which is costly as those data blocks don't have to be accessed right now)
    // or (b) sorting the  object table by address, which would require additional space
    // since the table order cannot be changed without corrupting the references
    // that point into it. It would also increase the runtime to O(n * log n)...
    for (auto pos = table.begin(), e = table.end(); pos != e; ++pos) {
        if (pos->is_free())
            continue; // Already garbage.

        const raw_address addr = pos->get_address();
        const auto& ent = chunks.entry_for(addr);
        const auto& chunk = ent.chunk;
        if (ent.marked && (chunk.large_object || ent.bitmap.test(chunk.cell_index(raw_address_cast<cell>(addr)))))
            continue; // Not garbage.

        // Object is unmarked: invoke finalizer and free the table entry.
        auto header = access.read_header(addr, types);
        if (header.type->finalizer) {
            header.type->finalizer(table.to_reference(pos));
        }
        table.remove(pos);
    }
}

template<u32 BlockSize>
class sweep_pass {
public:
    using object_access_type = object_access<BlockSize>;
    using object_header_type = typename object_access_type::object_header;

    using object_table_type = object_table<BlockSize>;
    using free_list_type = segregated_free_list<BlockSize>;

    using gc_data_type = gc_data<BlockSize>;
    using chunk_entry_type = typename gc_data_type::entry;

    using storage_type = storage<BlockSize>;

public:
    sweep_pass(gc_data_type& chunks,
               object_access_type& access,
               object_table_type& table,
               free_list_type& free_list,
               storage_type& storage,
               type_set& types)
        : m_chunks(chunks)
        , m_access(access)
        , m_table(table)
        , m_free_list(free_list)
        , m_storage(storage)
        , m_types(types)
    {}

    void run() {
        sweep_table(m_table, m_chunks, m_access, m_types);
        build_free_list();
    }

private:
    /// Rebuild the free list from scratch by iterating over the bitmap
    /// associated with each chunk.
    void build_free_list() {
        m_free_list.clear();

        for (auto ent = m_chunks.begin(), e = m_chunks.end(); ent != e; ++ent) {
            // Large object chunks are simply returned to the allocator.
            if (ent->chunk.large_object) {
                if (!ent->marked) {
                    m_storage.free(ent->chunk);
                }
                continue;
            }

            // Identify the free space in the chunk and add them to the free list.
            // Chunks that are entirely unmarked are completely free.
            const address base_cell = raw_address_cast<cell>(ent->chunk.addr);
            if (!ent->marked) {
                m_storage.free(ent->chunk);
                continue;
            }

            // Identify contiguous free regions (sequences of 0s) in the bitmap.
            // FIXME: Think about size_t ... and 64bit cell indicies?
            const detail::bitset& bitmap = ent->bitmap;
            size_t i = bitmap.find_unset(0);
            while (i != detail::bitset::npos) {
                size_t j = bitmap.find_set(i + 1);
                if (j == detail::bitset::npos)
                    j = bitmap.size();

                m_free_list.free(base_cell + static_cast<u64>(i), static_cast<u64>(j - i));
                if (j == bitmap.size())
                    break;
                i = bitmap.find_unset(j + 1);
            }
        }
    }

private:
    gc_data_type& m_chunks;
    object_access_type& m_access;
    object_table_type& m_table;
    free_list_type& m_free_list;
    storage_type& m_storage;
    type_set& m_types;
};

template<u32 BlockSize>
class compact_pass {
public:
    using object_access_type = object_access<BlockSize>;
    using object_header_type = typename object_access_type::object_header;

    using object_table_type = object_table<BlockSize>;
    using object_entry_type = object_entry<BlockSize>;

    using free_list_type = segregated_free_list<BlockSize>;

    using gc_data_type = gc_data<BlockSize>;
    using chunk_entry_type = typename gc_data_type::entry;

    using storage_type = storage<BlockSize>;

public:
    compact_pass(gc_data_type& chunks,
                 object_access_type& access,
                 object_table_type& table,
                 free_list_type& free_list,
                 storage_type& storage,
                 type_set& types)
        : m_chunks(chunks)
        , m_access(access)
        , m_table(table)
        , m_free_list(free_list)
        , m_storage(storage)
        , m_types(types)
    {}

    void run() {
        sweep_table(m_table, m_chunks, m_access, m_types);
        compact_objects();
        compute_relocation_tables();
        update_references();
    }

private:
    // Number of cells per entry in the relocation table.
    static constexpr u32 cells_per_table_chunk = 256;

    void compact_objects() {
        m_free_list.clear();
        for (auto ent = m_chunks.begin(), e = m_chunks.end(); ent != e; ++ent) {
            // Large object chunks are simply returned to the allocator.
            if (ent->chunk.large_object) {
                if (!ent->marked) {
                    m_storage.free(ent->chunk);
                }
                continue;
            }

            const address base_cell = raw_address_cast<cell>(ent->chunk.addr);
            if (!ent->marked) {
                m_storage.free(ent->chunk);
                continue;
            }

            // Move all live objects to the beginning of the chunk.
            // Start at the first `0` in the marking bitmap and slide all objects
            // after that to the front.
            const detail::bitset& bitmap = ent->bitmap;

            size_t write_cell = bitmap.find_unset();
            if (write_cell == bitmap.npos)
                continue;

            size_t read_cell = bitmap.find_set(write_cell + 1);
            while (read_cell != bitmap.npos) {
                size_t end = bitmap.find_unset(read_cell + 1);
                if (end == bitmap.npos)
                    end = bitmap.size();

                size_t size = end - read_cell;
                copy(engine(), (base_cell + write_cell).raw(), (base_cell + read_cell).raw(), size * cell_size);
                write_cell += size;

                if (end == bitmap.size())
                    break;
                read_cell = bitmap.find_set(end + 1);
            }

            if (write_cell != ent->chunk.cell_count()) {
                m_free_list.free(base_cell + write_cell, ent->chunk.cell_count() - write_cell);
            }
        }
    }

    void compute_relocation_tables() {
        for (auto ent = m_chunks.begin(), e = m_chunks.end(); ent != e; ++ent) {
            std::vector<u64>& table = ent->relocations;
            u64 total_cells = ent->chunk.cell_count();
            size_t chunks = total_cells / cells_per_table_chunk;
            u64 sum = 0;

            table.resize(chunks);
            for (size_t i = 0; i < chunks; ++i) {
                table[i] = sum;
                sum += ent->bitmap.count(i * cells_per_table_chunk, cells_per_table_chunk);
            }
        }
    }

    /// Computes the forward address (the new object location) by looking at
    /// the precomputed table and manually computing a small remainder.
    raw_address<BlockSize> forward_address(const chunk_entry_type& ent, raw_address<BlockSize> old_address) const {
        EXTPP_ASSERT(!ent.chunk.large_object, "Large objects do not relocate.");

        const u64 old_index = ent.chunk.cell_index(raw_address_cast<cell>(old_address));
        const size_t chunk_index = old_index / cells_per_table_chunk;
        const size_t chunk_begin = chunk_index * cells_per_table_chunk;
        const size_t offset_in_chunk = old_index % cells_per_table_chunk;

        EXTPP_ASSERT(chunk_index < ent.relocations.size(), "Index out of bounds.");
        u64 new_index = ent.relocations[chunk_index];                   // Sum of ones up to chunk_begin.
        new_index += ent.bitmap.count(chunk_begin, offset_in_chunk);    // Sum of ones in [chunk_begin, old_index)
        return raw_address_cast<cell>(ent.chunk.addr) + new_index;
    }

    void update_references() {
        for (auto pos = m_table.begin(), e = m_table.end(); pos != e; ++pos) {
            if (pos->is_free())
                continue;

            raw_address addr = pos->get_address();
            const chunk_entry_type& ent = m_chunks.entry_for(addr);
            if (!ent.chunk.large_object) {
                raw_address forward = forward_address(ent, addr);
                if (forward != addr) {
                    m_table.modify(pos, [&](object_entry_type& object) {
                        object.set_address(forward);
                    });
                }
            }
        }
    }

    extpp::engine<BlockSize>& engine() const { return m_storage.engine(); }

private:
    gc_data_type& m_chunks;
    object_access_type& m_access;
    object_table_type& m_table;
    free_list_type& m_free_list;
    storage_type& m_storage;
    type_set& m_types;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_GC_HPP
