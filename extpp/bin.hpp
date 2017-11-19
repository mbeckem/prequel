#ifndef EXTPP_BIN_HPP
#define EXTPP_BIN_HPP

#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>
#include <extpp/identity_key.hpp>
#include <extpp/log.hpp>
#include <extpp/math.hpp>
#include <extpp/stream.hpp>
#include <extpp/detail/bitset.hpp>
#include <extpp/detail/empty.hpp>

#include <fmt/ostream.h>

#include <functional>
#include <set>
#include <vector>

#if EXTPP_BIN_TRACE_ENABLED == 1
#   define EXTPP_BIN_TRACE(...) (::extpp::print_trace("EXTPP_BIN", __VA_ARGS__))
#else
#   define EXTPP_BIN_TRACE(...) do {} while (0)
#endif

namespace extpp {

template<u32 BlockSize>
class bin_marker;

template<u32 BlockSize>
class bin_collector;

template<u32 BlockSize>
class bin {
public:
    static constexpr u32 block_size = BlockSize;

    class reference {
    public:
        reference() = default;

        explicit operator bool() const { return index != bin::invalid_reference_index; }

    private:
        friend class bin;

        explicit reference(u64 index): index(index) {}

        u64 index = bin::invalid_reference_index;
    };

private:
    using raw_address_t = raw_address<BlockSize>;

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
     * Every object is stored as a contiguous number of cells. The first 8 byte in the first cell
     * of the object's storage are used for the object header. The object header currently only
     * contains a single value, which is the index of the object in the object table.
     * This index will be (?) useful when implementing the compaction algorithm.
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

    /// The minimum unit of allocation.
    struct cell {
        char data[16];
    };

    static constexpr u64 cell_size = 16;
    static constexpr u64 cell_size_log = log2(cell_size);

    using cell_address_t = address<cell, BlockSize>;

    /// In front of every live object.
    struct object_header {
        u64 index;
    };

    static_assert(sizeof(object_header) * 2 == cell_size, "Object header too large.");
    static_assert(BlockSize >= cell_size, "BlockSize must not be smaller than the cell size.");
    static_assert(BlockSize % cell_size == 0, "BlockSize must be multiple of the cell size");

    struct data_block {
        static constexpr u32 num_cells = BlockSize / cell_size;

        cell cells[num_cells];
    };
    static_assert(sizeof(data_block) == BlockSize, "Invalid block size.");

    /// Everything above this size is a large object.
    /// This value is arbitrary and needs experimentation. TODO
    static constexpr u64 max_small_object_blocks = 8;
    static constexpr u64 max_small_object_cells = max_small_object_blocks * BlockSize / cell_size;

private:
    class segregated_free_list {
        // The free list for index `i` contains cell ranges
        // of sizes `size_classes[i], ..., size_classes[i+1] - 1`.
        static constexpr std::array<u16, 16> size_classes = {
            1,  2,   3,   4,
            6,  8,   12,  16,
            24, 32,  48,  64,
            96, 128, 192, 256
        };

        struct list_node {
            address<list_node, BlockSize> next;
            u64 size;

            list_node(address<list_node, BlockSize> next, u64 size)
                : next(next), size(size) {}
        };

        struct list_header {
            address<list_node, BlockSize> head, tail;
        };

        using stream_type = stream<list_header, BlockSize>;

        struct cell_range {
            cell_address_t addr;
            u64 size = 0;

            cell_range() = default;
            cell_range(cell_address_t addr, u64 size)
                : addr(addr)
                , size(size)
            {}

            // Sort entries by size, break ties using the address.
            struct best_fit_order {
                bool operator()(const cell_range& lhs, const cell_range& rhs) const {
                    if (lhs.size != rhs.size)
                        return lhs.size < rhs.size;
                    return lhs.addr < rhs.addr;
                }
            };

            bool operator==(const cell_range& other) const {
                return addr == other.addr && size == other.size;
            }
        };

        using free_tree = btree<cell_range, identity_key, typename cell_range::best_fit_order, BlockSize>;
        using free_tree_iterator = typename free_tree::iterator;

    public:
        class anchor {
            typename stream_type::anchor lists;
            typename free_tree::anchor tree;

            friend class segregated_free_list;
        };

    public:
        segregated_free_list(anchor_ptr<anchor> anc, extpp::engine<BlockSize>& e, extpp::allocator<BlockSize>& a)
            : m_lists_headers(anc.neighbor(&anc->lists), e, a)
            , m_large_ranges(anc.neighbor(&anc->tree), e, a)
        {
            m_lists_headers.growth(linear_growth(1));
            if (m_lists_headers.empty()) {
                m_lists_headers.resize(size_classes.size() - 1);
            }
            for (auto i = m_lists_headers.begin(), e = m_lists_headers.end(); i != e; ++i)
                m_lists.push_back(m_lists_headers.pointer_to(i));
        }

        void clear() {
            for (auto& i : m_lists) {
                *i = list_header();
                i.dirty();
            }
            m_large_ranges.clear();
        }

        void free(cell_address_t cell, u64 size) {
            EXTPP_ASSERT(cell, "Cell pointer must be valid.");
            EXTPP_ASSERT(size > 0, "Invalid region size.");

            const size_t sc = size_class_index(size);
            EXTPP_ASSERT(sc < size_classes.size(), "Invalid size class.");
            EXTPP_ASSERT(size_classes[sc] <= size, "Size class invariant.");
            EXTPP_ASSERT(sc == size_classes.size() - 1 || size_classes[sc+1] > size, "Size class invariant.");

            if (sc == size_classes.size() - 1) {
                insert_large_run({cell, size});
            } else {
                insert_into_list(sc, {cell, size});
            }
        }

        cell_address_t allocate(u64 size) {
            cell_range range = get_free(size);
            if (!range.addr)
                return {};

            EXTPP_ASSERT(range.size >= size, "Range does not satisfy the size request.");
            if (range.size > size) {
                free(range.addr + size, range.size - size);
            }
            return range.addr;
        }

        void debug_stats(std::ostream& out) {
            for (size_t i = 0; i < size_classes.size() - 1; ++i) {
                auto& ls = m_lists[i];

                fmt::print(out, "Size [{}, {}):\n", size_classes[i], size_classes[i+1]);

                for (address<list_node, BlockSize> addr = ls->head; addr;) {
                    auto node = access(engine(), addr);
                    fmt::print(out, "  - Cell {}, Size {}\n", addr.raw(), node->size);
                    addr = node->next;
                }

                fmt::print(out, "\n");
            }

            fmt::print(out, "Size [{}, INF):\n", size_classes.back());
            for (const cell_range& r : m_large_ranges) {
                fmt::print(out, "  - Cell {}, Size {}\n", r.addr.raw(), r.size);
            }
        }

    private:
        void insert_into_list(size_t index, cell_range range) {
            EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");
            static_assert(cell_size >= sizeof(list_node), "Cannot store a list node in a cell.");

            auto new_tail = access(engine(), address_cast<list_node>(range.addr.raw()));
            new (new_tail.get()) list_node(nullptr, range.size);
            new_tail.dirty();

            auto& ls = m_lists[index];
            if (!ls->head) {
                EXTPP_ASSERT(!ls->tail, "Tail must be invalid too.");
                ls->head = ls->tail = new_tail.address();
                ls.dirty();
            } else {
                auto old_tail = access(engine(), ls->tail);
                old_tail->next = new_tail.address();
                old_tail.dirty();
                ls->tail = new_tail.address();
                ls.dirty();
            }
        }

        cell_range pop_list_head(size_t index) {
            EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");

            auto& ls = m_lists[index];
            if (!ls->head)
                return {};

            auto head_node = access(engine(), ls->head);
            auto cell_addr = address_cast<cell>(ls->head.raw());

            ls->head = head_node->next;
            if (!ls->head)
                ls->tail = nullptr;
            ls.dirty();

            return {cell_addr, head_node->size};
        }

        /// Scan through the entire list and try to find a large enough range.
        cell_range remove_first_fit(size_t index, u64 size) {
            EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");
            EXTPP_ASSERT(size_classes[index] <= size && size_classes[index+1] > size, "Wrong size class.");

            auto& ls = m_lists[index];

            address<list_node, BlockSize> prev_addr;
            address<list_node, BlockSize> curr_addr = ls->head;
            handle<list_node, BlockSize> prev_node;
            while (curr_addr) {
                handle<list_node, BlockSize> curr_node = access(engine(), curr_addr);
                if (curr_node->size >= size) {
                    // The node is large enough, unlink it.
                    if (prev_addr) {
                        prev_node->next = curr_node->next;
                        prev_node.dirty();
                    } else {
                        ls->head = curr_node->next;
                        ls.dirty();
                    }

                    if (curr_addr == ls->tail) {
                        ls->tail = prev_addr;
                        ls.dirty();
                    }
                    return {address_cast<cell>(curr_addr.raw()), curr_node->size};
                }

                prev_addr = curr_addr;
                curr_addr = curr_node->next;
                prev_node = std::move(curr_node);
            }
            return {};
        }

        /// Find the index of the list that contains blocks of the given size.
        size_t size_class_index(u64 size) {
            // First size class with sz > size.
            auto p = std::upper_bound(size_classes.begin(), size_classes.end(), size);
            EXTPP_ASSERT(p != size_classes.begin(), "Impossible."); // First size class is 1.
            p--; // sz <= size.
            return p - size_classes.begin();
        }

        /// Find a free range of the given size or more (if possible) and remove it
        /// from its datastructure.
        cell_range get_free(u64 size) {
            if (size >= size_classes.back()) {
                if (auto pos = find_large_run(size); pos != m_large_ranges.end()) {
                    cell_range result = *pos;
                    m_large_ranges.erase(pos);
                    return result;
                }
                return {};
            }

            // Test all segmented lists that are guaranteed
            // to satisfy the request (if they're not empty).
            const size_t si = size_class_index(size);
            const size_t sj = size_classes[si] == size ? si : si + 1;
            for (size_t i = sj; i < m_lists.size(); ++i) {
                if (cell_range range = pop_list_head(i); range.addr)
                    return range;
            }

            // Any large object would be big enough.
            if (auto pos = m_large_ranges.begin(); pos != m_large_ranges.end()) {
                cell_range range = *pos;
                m_large_ranges.erase(pos);
                return range;
            }

            // Otherwise, do a first fit search in the list that *might*
            // be able to satisfy the request. We may have already visited the list above.
            if (si != sj) {
                if (cell_range range = remove_first_fit(si, size); range.addr)
                    return range;
            }
            return {};
        }

        void insert_large_run(cell_range range) {
            EXTPP_ASSERT(range.size >= size_classes.back(),
                         "Range is not large enough.");

            free_tree_iterator pos;
            bool inserted;
            std::tie(pos, inserted) = m_large_ranges.insert(range);
            EXTPP_ASSERT(inserted, "Entry was not inserted.");
        }

        free_tree_iterator find_large_run(u64 size) {
            if (size <= size_classes.back())
                return m_large_ranges.begin();

            cell_range entry;
            entry.size = size;
            entry.addr = 0;
            return m_large_ranges.lower_bound(entry);
        }

        extpp::engine<BlockSize>& engine() const { return m_lists_headers.engine(); }

    private:
        /// Stores the linked list headers.
        stream_type m_lists_headers;

        /// Pins the headers in main memory.
        std::vector<handle<list_header, BlockSize>> m_lists;

        /// Ordered tree for very large regions.
        free_tree m_large_ranges;
    };

    struct chunk_entry {
        raw_address_t addr;     // First block in this chunk.
        u64 blocks: 62;         // Total number of blocks.
        u64 large_object: 1;    // True if this is a large object chunk.
        u64 unused: 1;

        chunk_entry() = default;

        chunk_entry(raw_address_t addr, u64 blocks, bool large)
            : addr(addr), blocks(blocks), large_object(large), unused(0)
        {}

        struct key_extract {
            raw_address_t operator()(const chunk_entry& c) const { return c.addr; }
        };
    };

    static constexpr u64 invalid_reference_index = -1;

    union object_table_entry {
    private:
        struct reference_t {
            u64 free: 1;        // Type discriminator.
            u64 unused: 1;
            u64 address: 62;    // On disk address divided by the cell size.
            u64 size;           // Size of the object on disk, in bytes.
        } ref;

        struct free_t {
            u64 free: 1;
            u64 unused: 63;
            u64 next;           // Index of the next free slot or -1.
        } free;

    public:
        static object_table_entry make_reference(raw_address_t addr, u64 size) {
            EXTPP_ASSERT(addr.value() % cell_size == 0, "Address must be aligned correctly.");

            object_table_entry e;
            e.ref.free = 0;
            e.ref.unused = 0;
            e.ref.address = addr.value() / cell_size;
            e.ref.size = size;
            return e;
        }

        static object_table_entry make_free(u64 next) {
            object_table_entry e;
            e.free.free = 1;
            e.free.unused = 0;
            e.free.next = next;
            return e;
        }

    public:
        bool is_free() const { return free.free == 1; }
        bool is_reference() const { return ref.free == 0; }

        raw_address_t get_address() const { return static_cast<raw_address_t>(as_reference().address * cell_size); }
        void set_address(raw_address_t addr) {
            EXTPP_ASSERT(addr.value() % cell_size == 0, "Address must be aligned correctly.");
            as_reference().address = addr.value() / cell_size;
        }

        u64 get_size() const { return as_reference().size; }

        u64 get_next() const { return as_free().next; }
        void set_next(u64 next) {
            as_free().next = next;
        }

    private:
        object_table_entry() {} // Not initialized.

        reference_t& as_reference() { return const_cast<reference_t&>(const_cast<const object_table_entry*>(this)->as_reference()); }
        free_t& as_free() { return const_cast<free_t&>(const_cast<const object_table_entry*>(this)->as_free()); }

        const reference_t& as_reference() const {
            EXTPP_ASSERT(is_reference(), "Must be a reference.");
            return ref;
        }

        const free_t& as_free() const {
            EXTPP_ASSERT(is_free(), "Must be a freelist entry.");
            return free;
        }
    };

    using chunk_tree = btree<chunk_entry, typename chunk_entry::key_extract, std::less<>, BlockSize>;
    using chunk_iterator = typename chunk_tree::iterator;

    using object_table = stream<object_table_entry, BlockSize>;
    using object_table_iterator = typename object_table::iterator;

public:
    class anchor {
        typename chunk_tree::anchor chunks;
        typename object_table::anchor objects;
        typename segregated_free_list::anchor free_list;

        u64 next_free_table_entry = invalid_reference_index;

        friend class bin;
    };

    using collector = bin_collector<BlockSize>;
    using marker = bin_marker<BlockSize>;

public:
    bin(anchor_ptr<anchor> h, extpp::engine<BlockSize>& e, extpp::allocator<BlockSize>& a)
        : m_anchor(std::move(h))
        , m_engine(&e)
        , m_alloc(&a)
        , m_chunks(m_anchor.neighbor(&m_anchor->chunks), e, a)
        , m_free_list(m_anchor.neighbor(&m_anchor->free_list), e, a)
        , m_objects(m_anchor.neighbor(&m_anchor->objects), e, a)
    {}

    bin(const bin&) = delete;
    bin(bin&&) noexcept = default;

    bin& operator=(const bin&) = delete;
    bin& operator=(bin&&) noexcept = default;

    u64 chunk_size() const { return m_chunk_size; }
    void chunk_size(u64 blocks) {
        m_chunk_size = std::max(4 * max_small_object_blocks, blocks);
    }

    reference insert(const byte* data, u64 data_size) {
        // TODO Exception safe & overflow check.
        const u64 object_size_bytes = data_size + sizeof(object_header);
        const u64 object_size_cells = cell_count(object_size_bytes);
        const raw_address_t addr = allocate(object_size_cells).raw();

        // Remember the original size (not rounded up to cell size).
        const u64 index = insert_reference(addr, object_size_bytes);

        const object_header header{index};
        write(*m_engine, addr, &header, sizeof(object_header));
        write(*m_engine, addr + sizeof(object_header), data, data_size);
        return reference{index};
    }

    void load(reference ref, std::vector<byte>& output) const {
        EXTPP_CHECK(valid_reference(ref), "load(): Invalid reference.");

        const object_table_entry entry = m_objects[ref.index];
        const raw_address_t addr = entry.get_address();
        const u64 object_size = entry.get_size();
        const u64 data_size = object_size - sizeof(object_header);

        EXTPP_CHECK(data_size <= output.max_size(), "Object too large.");
        output.resize(data_size);

        object_header header;
        read(*m_engine, addr, &header, sizeof(object_header));
        EXTPP_CHECK(header.index == ref.index, "Header integrity check failed.");
        read(*m_engine, addr + sizeof(object_header), data_size);
    }

    u64 size(reference ref) const {
        EXTPP_CHECK(valid_reference(ref), "load(): Invalid reference.");

        return m_objects[ref.index].get_size();
    }

    void debug_stats(std::ostream& out) {
        fmt::print(out, "Chunk tree:\n");
        for (chunk_entry chunk : m_chunks) {
            fmt::print(out, "- Address: {}, Blocks: {}, Large: {}\n", chunk.addr, chunk.blocks, bool(chunk.large_object));
        }
        fmt::print(out, "\n");

        fmt::print(out, "Free list:\n");
        m_free_list.debug_stats(out);
        fmt::print(out, "\n");

        fmt::print(out, "Object table:\n");
        u64 index = 0;
        for (object_table_entry obj : m_objects) {
            if (obj.is_free()) {
                fmt::print(out, "- Index {}: Free, Next {}\n", index, obj.get_next());
            } else {
                fmt::print(out, "- Index {}: Address {}, Size {}\n", index, obj.get_address(), obj.get_size());
            }
            ++index;
        }
    }

    collector collect() { return collector(*this); }

private:
    cell_address_t allocate(u64 cells) {
        if (cells >= max_small_object_cells)
            return allocate_large_object_chunk(cells);
        if (auto addr = m_free_list.allocate(cells))
            return addr;

        allocate_chunk();
        if (auto addr = m_free_list.allocate(cells))
            return addr;

        EXTPP_UNREACHABLE("Failed to allocate from a fresh chunk.");
    }

    cell_address_t allocate_large_object_chunk(u64 cells) {
        const u64 blocks = block_count(cells);
        const raw_address_t addr = m_alloc->allocate(blocks);
        insert_chunk(chunk_entry(addr, blocks, true));
        return address_cast<cell>(addr);
    }

    /// Allocate a new chunk and put it into the free list.
    void allocate_chunk() {
        const u64 blocks = m_chunk_size;
        const raw_address_t addr = m_alloc->allocate(blocks);
        const chunk_entry chunk(addr, blocks, false);
        insert_chunk(chunk);
        m_free_list.free(address_cast<cell>(addr), blocks * data_block::num_cells);
    }

    /// Allocate a slot in the object table that points to (addr, size).
    /// Returns the index of that slot.
    u64 insert_reference(raw_address_t addr, u64 byte_size) {
        u64 index = m_anchor->next_free_table_entry;
        if (index != invalid_reference_index) {
            auto pos = m_objects.begin() + index;

            m_anchor->next_free_table_entry = pos->get_next();
            m_anchor.dirty();

            m_objects.replace(pos, object_table_entry::make_reference(addr, byte_size));
            return index;
        }

        m_objects.push_back(object_table_entry::make_reference(addr, byte_size));
        return m_objects.size() - 1;
    }

    bool valid_reference(reference r) const {
        return r.index < m_objects.size() && m_objects[r.index].is_reference();
    }

    /// Inserts a new chunk into the chunk tree.
    auto insert_chunk(const chunk_entry& entry) {
        chunk_iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_chunks.insert(entry);
        EXTPP_ASSERT(inserted, "Entry was not inserted.");
        return pos;
    }

    void erase_chunk(raw_address_t addr) {
        bool erased = m_chunks.erase(addr);
        EXTPP_ASSERT(erased, "Chunk was not found in chunk tree.");
        unused(erased);
    }

    /// Returns the cell index span [begin, end) for the object `o` in the given chunk.
    u64 cell_index(const chunk_entry& e, const object_table_entry& o) {
        EXTPP_ASSERT(!e.large_object, "Meaningless for large objects.");
        EXTPP_ASSERT(o.get_address() >= e.addr
                     && o.get_address() + o.get_size() < e.addr + e.blocks * BlockSize,
                     "Object does not belong to this chunk.");
        return distance(address_cast<cell>(e.addr), address_cast<cell>(o.get_address()));
    }

    /// Returns the number of cells occupied by an object with the given byte size.
    static u64 cell_count(u64 byte_size) {
        return ceil_div(byte_size, cell_size);
    }

    /// Returns the number of blocks required for the given number of cells.
    static u64 block_count(u64 cells) {
        return ceil_div(cells, u64(data_block::num_cells));
    }

private:
    // private gc API
    friend marker;
    friend collector;

    void start_collecting() {
        EXTPP_CHECK(!m_collecting, "A collection is already running.");
        m_collecting = true;
    }

    void initialize_marker(marker& m) {
        for (const chunk_entry& chunk : m_chunks)
            m.add_chunk(chunk);
    }

    /// Marks the object as reachable.
    /// Returns true if this is the first time the object was seen in this marking phase.
    bool mark(marker& m, reference ref) {
        EXTPP_CHECK(valid_reference(ref), "Invalid reference.");

        // Get the chunk that contains this object, and its marking bitmap.
        object_table_entry object = m_objects[ref.index];
        auto chunk = m.find_chunk(object.get_address());

        // Large objects use the "marked" flag and nothing else.
        if (chunk->large_object) {
            if (chunk->marked)
                return false;

            chunk->marked = true;
            EXTPP_BIN_TRACE("Object {} at {} marked as live.", ref.index, object.get_address());
            return true;
        }

        if (!chunk->marked)
            chunk->marked = true;

        // Mark all cells that belong to the object.
        u64 index = cell_index(*chunk, object);
        u64 cells = cell_count(object.get_size());
        if (chunk->bitmap.test(index))
            return false; // We have been here before, no need to mark all cells again.

        EXTPP_BIN_TRACE("Object {} at {} (cells {}, {}) marked as live.", ref.index, object.get_address(), index, cells);

        // TODO: Bulk set?
        for (u64 i = 0; i < cells; ++i)
            chunk->bitmap.set(index + i);
        return true;
    }

    /// Iterate over the object table and free those slots that have not been marked.
    void sweep_objects(marker& m) {
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
        u64 index = 0;
        for (auto pos = m_objects.begin(), e = m_objects.end(); pos != e; ++pos, ++index) {
            if (pos->is_free())
                continue;

            auto chunk = m.find_chunk(pos->get_address());
            if (!chunk->marked || !chunk->large_object && !chunk->bitmap.test(cell_index(*chunk, *pos))) {
                EXTPP_BIN_TRACE("Object {} at {} is dead.", index, pos->get_address());

                m_objects.replace(pos, object_table_entry::make_free(m_anchor->next_free_table_entry));
                m_anchor->next_free_table_entry = index;
                m_anchor.dirty();
            }
        }
    }

    /// Rebuild the free list from scratch by iterating over the bitmap
    /// associated with each chunk.
    void free_space(marker& m) {
        m_free_list.clear();

        EXTPP_BIN_TRACE("Reclaiming free space");

        for (auto chunk = m.begin(), e = m.end(); chunk != e; ++chunk) {
            EXTPP_BIN_TRACE("Chunk {}", chunk->addr);

            // Large object chunks are simply returned to the allocator.
            if (chunk->large_object) {
                if (!chunk->marked) {
                    m_alloc->free(chunk->addr);
                    erase_chunk(chunk->addr);

                    EXTPP_BIN_TRACE("Chunk {} was a large object and has been freed.", chunk->addr);
                }
                continue;
            }

            // Identify the free space in the chunk and add them to the free list.
            // Chunks that are entirely unmarked are completely free.
            const address base_cell = address_cast<cell>(chunk->addr);
            if (!chunk->marked) {
                m_free_list.free(base_cell, chunk->blocks * data_block::num_cells);
                EXTPP_BIN_TRACE("Chunk {} was completely free.", chunk->addr);
                continue;
            }

            // Identify contiguous free regions (sequences of 0s) in the bitmap.
            // FIXME: Think about size_t ... and 64bit cell indicies?
            const detail::bitset& bitmap = chunk->bitmap;
            size_t i = bitmap.find_unset(0);
            while (i != detail::bitset::npos) {
                size_t j = bitmap.find_set(i + 1);
                if (j == detail::bitset::npos)
                    j = bitmap.size();

                EXTPP_BIN_TRACE("Free cell region: Index {} ({} cells)", i, j - i);
                m_free_list.free(base_cell + static_cast<u64>(i), static_cast<u64>(j - i));

                if (j == bitmap.size())
                    break;
                i = bitmap.find_unset(j + 1);
            }
        }
    }

    void stop_collecting() {
        EXTPP_CHECK(m_collecting, "Invalid state.");
        m_collecting = false;
    }

private:
    anchor_ptr<anchor> m_anchor;
    extpp::engine<BlockSize>* m_engine;
    extpp::allocator<BlockSize>* m_alloc;

    /// True if garbage collection is currenty running.
    bool m_collecting = false;

    /// Allocate this many blocks at once.
    u32 m_chunk_size = 128;

    /// Contains references to chunks, indexed by their starting address.
    chunk_tree m_chunks;

    /// Contains free cell ranges. Built during the sweeping phase.
    segregated_free_list m_free_list;

    /// Contains the mappings from reference to real disk address.
    object_table m_objects;
};

template<u32 BlockSize>
class bin_marker {
public:
    using bin_type = bin<BlockSize>;
    using reference_type = typename bin_type::reference;
    using chunk_entry_type = typename bin_type::chunk_entry;

private:
    friend bin_type;

    // Given a reference and a callback object, invoke the callback object
    // for every child reference in the object pointed to by `ref`.
    using visitor_type = std::function<void(bin_marker&, bin_type& bin, reference_type ref)>;

    using stack_entry = std::tuple<reference_type, visitor_type>;
    using stack_type = std::vector<stack_entry>;

    /// Given an arbitrary child reference visitor, returns a visitor function
    /// that mark's an objects children.
    template<typename Visitor>
    static auto child_visitor(Visitor&& visitor) {
        return [visitor = std::forward<Visitor>(visitor)](bin_marker& marker, bin_type& bin, reference_type ref) {
            visitor(bin, ref, [&marker, &bin](reference_type child_ref, auto&& child_visitor) {
                marker.mark_object(bin, child_ref, std::forward<decltype(child_visitor)>(child_visitor));
            });
        };
    }

    /// Set the mark bit of the object. If the mark bit was unset, also inspect
    /// the object's child references.
    template<typename Visitor>
    void mark_object(bin_type& bin, reference_type ref, Visitor&& visitor) {
        if (bin.mark(*this, ref)) {
            m_stack.emplace_back(ref, child_visitor(std::forward<Visitor>(visitor)));
        }
    }

public:
    /// Mark the entire object graph reachable from `ref`.
    /// This function is NOT reentrant. DO NOT make nested calls to it.
    template<typename Visitor>
    void operator()(bin_type& bin, reference_type ref, Visitor&& v) {
        EXTPP_ASSERT(m_stack.empty(), "The stack must be empty from the last run.");
        EXTPP_CHECK(!m_running, "Do not make nested calls to this function.");
        m_running = true;

        // The stack contains objects that have been marked (for the first time)
        // but whose child references have not yet been scanned.
        mark_object(bin, ref, std::forward<Visitor>(v));
        while (!m_stack.empty()) {
            reference_type ref;
            visitor_type visitor;
            std::tie(ref, visitor) = std::move(m_stack.back());
            m_stack.pop_back();

            visitor(*this, bin, ref);
        }

        m_running = false;
    }

private:
    struct chunk_bitmap_entry : chunk_entry_type {
        chunk_bitmap_entry(const chunk_entry_type& e)
            : chunk_entry_type(e)
        {
            if (!this->large_object)
                bitmap.resize(e.blocks * bin_type::data_block::num_cells);
        }

        chunk_bitmap_entry(const chunk_bitmap_entry&) = delete;
        chunk_bitmap_entry& operator=(const chunk_bitmap_entry&) = delete;

        // True if *anything* in this chunk has been marked as live.
        mutable bool marked = false;

        // Not used for large objects (only `marked` is used in that case).
        mutable detail::bitset bitmap;

        friend bool operator<(const chunk_bitmap_entry& lhs, const chunk_bitmap_entry& rhs) {
            return lhs.addr < rhs.addr;
        }

        friend bool operator<(raw_address<BlockSize> lhs, const chunk_bitmap_entry& rhs) {
            return lhs < rhs.addr;
        }

        friend bool operator<(const chunk_bitmap_entry& lhs, raw_address<BlockSize> rhs) {
            return lhs.addr < rhs;
        }
    };

    using chunk_set = std::set<chunk_bitmap_entry, std::less<>>;
    using chunk_iter = typename chunk_set::iterator;

private:
    auto begin() const { return m_chunks.begin(); }
    auto end() const { return m_chunks.end(); }

    void reset() {
        m_chunks.clear();
    }

    void add_chunk(const chunk_entry_type& e) {
        chunk_iter pos;
        bool inserted;
        std::tie(pos, inserted) = m_chunks.emplace(e);
        EXTPP_ASSERT(inserted, "Chunk could not be inserted");
    }

    chunk_iter find_chunk(raw_address<BlockSize> object_in_chunk) {
        chunk_iter pos = m_chunks.upper_bound(object_in_chunk);
        EXTPP_ASSERT(pos != m_chunks.begin(), "No chunk for that object.");
        --pos;
        EXTPP_ASSERT(pos->addr <= object_in_chunk
                     && pos->addr + pos->blocks * BlockSize >= object_in_chunk,
                     "Object not in range for chunk.");
        return pos;
    }

private:
    chunk_set m_chunks;
    stack_type m_stack;
    bool m_running = false;
};

template<u32 BlockSize>
class bin_collector {
public:
    using bin_type = bin<BlockSize>;
    using reference_type = typename bin_type::reference;
    using marker_type = bin_marker<BlockSize>;

public:
    bin_collector(const bin_collector&) = delete;
    bin_collector& operator=(const bin_collector&) = delete;

    ~bin_collector() {
        m_bin->stop_collecting();
    }

    void visit(reference_type ref) {
        m_marker(*m_bin, ref, [](bin_type&, reference_type, auto&&) {});
    }

    template<typename ChildReferenceVisitor>
    void visit(reference_type ref, ChildReferenceVisitor&& v) {
        m_marker(*m_bin, ref, std::forward<ChildReferenceVisitor>(v));
    }

    void operator()() {
        m_bin->sweep_objects(m_marker);
        m_bin->free_space(m_marker);
    }

private:
    friend bin_type;

    bin_collector(bin_type& b)
        : m_bin(&b)
    {
        m_bin->start_collecting();
        m_bin->initialize_marker(m_marker);
    }

private:
    bin_type* m_bin;
    marker_type m_marker;
};

} // namespace extpp

#endif // EXTPP_BIN_HPP
