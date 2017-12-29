#ifndef EXTPP_HEAP_HPP
#define EXTPP_HEAP_HPP

#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/log.hpp>
#include <extpp/math.hpp>
#include <extpp/heap/base.hpp>
#include <extpp/heap/gc.hpp>
#include <extpp/heap/object_access.hpp>
#include <extpp/heap/object_table.hpp>
#include <extpp/heap/free_space.hpp>
#include <extpp/heap/storage.hpp>
#include <extpp/heap/type_set.hpp>

#include <fmt/ostream.h>

#include <unordered_map>

#if EXTPP_BIN_TRACE_ENABLED == 1
#   define EXTPP_BIN_TRACE(...) (::extpp::print_trace("EXTPP_BIN", __VA_ARGS__))
#else
#   define EXTPP_BIN_TRACE(...) do {} while (0)
#endif

namespace extpp {

template<u32 BlockSize>
class heap;

template <u32 BlockSize>
class collector;

template <u32 BlockSize>
class compactor;

namespace detail {

template<typename SweepPass, u32 BlockSize>
class collector_base;

} // namespace detail
} // namespace extpp

namespace extpp {

template<u32 BlockSize>
class heap : public uses_allocator<BlockSize> {
public:
    static constexpr u32 block_size = BlockSize;

    using reference = extpp::reference;

private:
    /*
     * Heap layout
     * ============
     *
     * The heap is divided into chunks of a certain number of contiguous *data blocks*.
     * There is no metadata at the beginning of a data block to allow for larger objects
     * that might span multiple disk blocks to be contiguous on disk.
     *
     * Data blocks are divided into small *cells*. The size of a cell is currently 16 bytes.
     * Objects are allocated as a sequence of cells; all sizes are rounded up to a multiple
     * of the cell size.
     *
     *
     * The object table
     * =================
     *
     * References to objects within the heap are implemented as indices into the object table.
     * Every object reference points to an entry within the object table, which in turn points to the current
     * physical address of the object on disk. This enables us to move objects without having to update
     * all references, which would be tricky (or in some cases even impossible) for references
     * from outside the garbage collected space. Once an object is no longer referenced,
     * its slot in the object table is freed and put into a free list. The slot will be reused
     * for new objects.
     *
     *
     * Object layout
     * ==============
     *
     * Every object is stored as a contiguous number of cells within a chunk. They can span the
     * boundaries of multiple blocks. There is an object header in front of every object,
     * see class object_access.
     *
     *
     * Large objects
     * ==============
     *
     * Large objects are handled differently than other objects. There is little benefit
     * from compacting large objects, since copying them takes a lot of time. Instead, large
     * objects get a new chunk of their own and will not be moved at all.
     * Once a large object becomes dead, the chunk is freed.
     *
     *
     * Garbage collection
     * ===================
     *
     * Garbage collection is done is done by using the well known "Mark and Sweep"
     * and "Mark and Compact" techniques. First, the entire (live) object graph
     * is traversed by starting from a set of root references. During this process,
     * every object seen is marked as "live". As a consequence, all other objects
     * are dead and their space can be reclaimed.
     *
     * Before garbage collection begins, we allocate a large bitmap for every chunk of
     * allocated blocks. These bitmaps currently reside in internal memory, because
     * their state does not have to be persistent (TODO: For very large heaps,
     * they should be paged in and out as required). Every bitmap has exactly one
     * bit for every *cell* in the chunk's region. Per-cell marking enables
     * us to quickly identify free space and coalesce free ranges, see below.
     *
     *
     * Finding the mark bit
     * =====================
     *
     * Given an object O stored in some data block with address D one has to first find
     * the beginning of the chunk that contains D. This is done by traversing a runtime
     * (internal) ordered tree structure that contains one entry for every chunk,
     * indexed by it's starting address on disk. The starting address of the chunk
     * can then be used to compute the cell offset of O within that chunk.
     * The tree entry also contains the marking bitset, in which we can now mark the
     * region occupied by O as "live".
     *
     *
     * Reclaming free space
     * =====================
     *
     * Once the entire live heap has been marked, we can sweep through it on a per-chunk basis.
     * For every chunk, we iterate through its bitmap in order to identify contiguous regions
     * of 0s. These regions are added to the free list for future allocations. This way,
     * free cells are coalesced into regions of maximal size, which helps against fragmentation.
     */

private:
    enum class gc_phase {
        none, mark, collect
    };

    using storage_type = heap_detail::storage<BlockSize>;
    using chunk_entry = typename storage_type::chunk_entry;
    using chunk_iterator = typename storage_type::chunk_iterator;

    using object_table_type = heap_detail::object_table<BlockSize>;
    using object_table_iterator = typename object_table_type::iterator;
    using object_entry_type = typename object_table_type::object_entry_type;

    using object_access_type = heap_detail::object_access<BlockSize>;
    using object_header_type = typename object_access_type::object_header;

    using free_space_type = heap_detail::free_space<BlockSize>;

    using cell = heap_detail::cell;

    static constexpr auto cell_size = heap_detail::cell_size;

    static constexpr auto cells_per_block = storage_type::cells_per_block;

public:
    class anchor {
        typename storage_type::anchor storage;
        typename object_table_type::anchor objects;
        typename free_space_type::anchor free_space;

        friend class heap;
    };

    using collector_type = collector<BlockSize>;
    using compactor_type = compactor<BlockSize>;

public:
    heap(anchor_ptr<anchor> h, allocator<BlockSize>& alloc)
        : heap::uses_allocator(alloc)
        , m_anchor(std::move(h))
        , m_access(this->get_engine())
        , m_storage(m_anchor.member(&anchor::storage), alloc)
        , m_free_space(m_anchor.member(&anchor::free_space), alloc)
        , m_table(m_anchor.member(&anchor::objects), alloc)
    {}

    heap(const heap&) = delete;
    heap(heap&&) noexcept = default;

    heap& operator=(const heap&) = delete;
    heap& operator=(heap&&) noexcept = default;

    u64 chunk_size() const { return m_chunk_size; }
    void chunk_size(u64 blocks) {
        m_chunk_size = std::max(4 * block_count(free_space_type::max_small_object_cells), blocks);
    }

    /// Inserts a new object into the heap.
    /// The type of the object must have been registered previously
    /// using \ref register_type().
    ///
    /// \param type
    ///     The type of the new object.
    /// \param object_data
    ///     The object's raw data.
    /// \param object_size
    ///     The object's size, in bytes.
    ///
    /// \return A reference to the new object.
    reference insert(type_index type, const void* object_data, u64 object_size) {
        EXTPP_CHECK(m_gc_phase == gc_phase::none,
                    "Cannot insert new objects during garbage collection.");
        EXTPP_CHECK(type, "Invalid type index.");
        EXTPP_CHECK(object_size > 0, "Cannot insert zero-sized objects.");

        // TODO Exception safe & overflow check.
        const object_header_type header = m_access.make_header(m_types.get(type), object_size);
        const u64 allocation_size = header.total_size();
        const u64 cells = cell_count(allocation_size);
        const raw_address addr = allocate(cells);

        m_access.write_header(addr, allocation_size, header);
        write(this->get_engine(), addr + header.header_size, object_data, object_size);

        return m_table.insert(object_entry_type::make_reference(addr));
    }

    /// Load an object from disk into the given output buffer.
    // TODO Raw api.
    template<typename Container>
    void load(reference ref, Container& output) const {
        EXTPP_CHECK(m_table.valid(ref), "load(): Invalid reference.");

        const object_entry_type entry = m_table[ref];
        const raw_address addr = entry.get_address();
        const object_header_type header = m_access.read_header(addr, m_types);
        const u64 body_size = header.body_size;
        EXTPP_CHECK(body_size <= std::numeric_limits<size_t>::max(),
                    "Cannot fit that object into main memory.");

        output.resize(body_size);
        read(this->get_engine(), addr + header.header_size, output.data(), body_size);
    }

    /// Returns the type of the object that `ref` points at.
    type_index type(reference ref) const {
        EXTPP_CHECK(m_table.valid(ref), "load(): Invalid reference.");
        const object_entry_type entry = m_table[ref];
        const raw_address addr = entry.get_address();
        const object_header_type header = m_access.read_header(addr, m_types);
        return header.type->index;
    }

    /// Register the given type info with this heap.
    /// Every type used by the application must be registered exactly once,
    /// every time the heap is loaded.
    void register_type(const type_info& type) {
        EXTPP_CHECK(m_gc_phase == gc_phase::none,
                    "Cannot register new types during garbage collection.");
        m_types.register_type(type);
    }

    gc_phase phase() const { return m_gc_phase; }

    void debug_print(std::ostream& out) {
        fmt::print(out, "Chunk tree:\n");
        for (chunk_entry chunk : m_storage) {
            fmt::print(out, "- Address: {}, Blocks: {}, Large: {}\n", chunk.addr, chunk.blocks, bool(chunk.large_object));
        }
        fmt::print(out, "\n");

        fmt::print(out, "Free list:\n");
        m_free_space.debug_stats(out);
        fmt::print(out, "\n");

        fmt::print(out, "Object table:\n");
        u64 index = 0;
        for (object_entry_type obj : m_table) {
            if (obj.is_free()) {
                fmt::print(out, "- Index {}: Free, Next {}\n", index, obj.get_next());
            } else {
                fmt::print(out, "- Index {}: Address {}\n", index, obj.get_address());
            }
            ++index;
        }
    }

    collector_type begin_collection() {
        EXTPP_CHECK(m_gc_phase == gc_phase::none,
                    "Garbage collection is already running.");
        return collector_type(*this);
    }

    compactor_type begin_compaction() {
        EXTPP_CHECK(m_gc_phase == gc_phase::none,
                    "Garbage collection is already running.");
        return compactor_type(*this);
    }

private:
    address<cell> allocate(u64 cells) {
        // Large objects are allocated directly in a chunk of their own.
        // Small objects are allocated from the free list. If the free-list
        // cannot satisfy the request, a new chunk is allocated and
        // the attempt is repeated.

        if (cells > free_space_type::max_small_object_cells) {
            const u64 blocks = block_count(cells);
            chunk_entry entry = m_storage.allocate(blocks, true);
            return raw_address_cast<cell>(entry.addr);
        }

        if (auto addr = m_free_space.allocate(cells))
            return addr;

        chunk_entry entry = m_storage.allocate(m_chunk_size, false);
        m_free_space.free(raw_address_cast<cell>(entry.addr), entry.cell_count());

        auto addr = m_free_space.allocate(cells);
        EXTPP_ASSERT(addr, "Allocation failed.");
        return addr;
    }

    /// Returns the number of cells occupied by an object with the given byte size.
    static constexpr u64 cell_count(u64 byte_size) {
        return ceil_div(byte_size, u64(cell_size));
    }

    /// Returns the number of blocks required for the given number of cells.
    static constexpr u64 block_count(u64 cells) {
        return ceil_div(cells, u64(cells_per_block));
    }

private:
    template<typename S, u32 B>
    friend class detail::collector_base;

    void set_gc_phase(gc_phase phase) {
        m_gc_phase = phase;
    }

private:
    anchor_ptr<anchor> m_anchor;

    /// The current state of the garbage collector.
    gc_phase m_gc_phase = gc_phase::none;

    /// Allocate this many blocks at once.
    u64 m_chunk_size = 128;

    /// Manages object layout (headers and so forth).
    object_access_type m_access;

    /// Manages chunks allocated for object storage.
    storage_type m_storage;

    /// Contains free cell ranges. Built during the sweeping phase.
    free_space_type m_free_space;

    /// Contains the mappings from reference to real disk address.
    object_table_type m_table;

    /// Runtime type information.
    heap_detail::type_set m_types;
};

namespace detail {

template<typename CollectPass, u32 BlockSize>
class collector_base {
public:
    using heap_type = extpp::heap<BlockSize>;

public:
    collector_base(const collector_base&) = delete;
    collector_base& operator=(const collector_base&) = delete;

    ~collector_base() {
        m_heap.set_gc_phase(heap_type::gc_phase::none);
    }

    /// Visit a root reference.
    void visit(reference ref) {
        EXTPP_CHECK(!m_in_visit,
                    "visit() cannot be called recursively.");
        EXTPP_CHECK(m_heap.phase() == heap_type::gc_phase::mark,
                    "visit() can only be called while in the marking phase.");

        m_in_visit = true;
        detail::rollback rb = [&]{
            m_in_visit = false;
        };

        m_mark.visit(ref);
    }

    void operator()() {
        m_heap.set_gc_phase(heap_type::gc_phase::collect);
        m_collect.run();
        m_heap.set_gc_phase(heap_type::gc_phase::none);
    }

protected:
    friend heap_type;

    collector_base(heap_type& b)
        : m_heap(b)
        , m_data(m_heap.m_storage)
        , m_mark(m_data, m_heap.m_access, m_heap.m_table, m_heap.m_types)
        , m_collect(m_data, m_heap.m_access, m_heap.m_table, m_heap.m_free_space, m_heap.m_storage, m_heap.m_types)
    {
        m_heap.set_gc_phase(heap_type::gc_phase::mark);
    }

private:
    heap_type& m_heap;
    bool m_in_visit = false;
    heap_detail::gc_data<BlockSize> m_data;
    heap_detail::mark_pass<BlockSize> m_mark;
    CollectPass m_collect;
};

} // namespace detail

template<u32 BlockSize>
class collector : public detail::collector_base<heap_detail::sweep_pass<BlockSize>, BlockSize> {
public:
    using collector::collector_base::collector_base;
};

template<u32 BlockSize>
class compactor : public detail::collector_base<heap_detail::compact_pass<BlockSize>, BlockSize> {
public:
    using compactor::collector_base::collector_base;
};

} // namespace extpp

#endif // EXTPP_HEAP_HPP
