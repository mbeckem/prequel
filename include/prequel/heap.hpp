#ifndef PREQUEL_HEAP_HPP
#define PREQUEL_HEAP_HPP

#include <prequel/allocator.hpp>
#include <prequel/btree.hpp>
#include <prequel/defs.hpp>
#include <prequel/detail/operators.hpp>
#include <prequel/engine.hpp>

#include <vector>

namespace prequel {

class heap;
class heap_reference;
struct heap_statistics;

/**
 * References an object managed by the heap.
 * Objects can be accessed, altered or deleted by passing the reference
 * to the heap object that created it.
 *
 * References are comparable. The result of a comparison gives a hint about
 * the location of the objects on disk. If you access a set of references ordered
 * by `<`, then the result will be an (almost) sequential scan.
 */
class heap_reference : public detail::make_comparable<heap_reference> {
    /*
     * References point to slots (small objects), which are aligned on a 4-byte-boundary,
     * or directly to large objects, which are block aligned.
     * Therefore, we have at least 2 bits available. We use one of them to tag the reference
     * value with its type (large or small).
     */
public:
    static constexpr u64 invalid_value = u64(-1);

private:
    static constexpr u64 large_bit = u64(1) << 63;

public:
    heap_reference() = default;

    /**
     * Returns true when the heap reference points to a valid object.
     */
    /// @{
    bool valid() const { return m_value != invalid_value; }
    explicit operator bool() const { return valid(); }
    /// @}
    ///

    /// Unspecified value that can serve as an identity for this object.
    u64 value() const { return m_value; }

    friend bool operator<(const heap_reference& a, const heap_reference& b) {
        // +1: invalid value comes first (like NULL)
        return (a.m_value + 1) < (b.m_value + 1);
    }

    friend bool operator==(const heap_reference& a, const heap_reference& b) {
        return a.m_value == b.m_value;
    }

private:
    friend heap;

    explicit heap_reference(u64 value)
        : m_value(value) {}

    // pre: valid()
    bool is_small_object() const;
    bool is_large_object() const;
    raw_address address() const;

    static heap_reference make_large_object(raw_address addr);
    static heap_reference make_small_object(raw_address slot_addr);

private:
    friend binary_format_access;

    static constexpr auto get_binary_format() {
        return make_binary_format(&heap_reference::m_value);
    }

private:
    u64 m_value = invalid_value;
};

/**
 * A heap stores objects of variable size.
 *
 * Objects are saved unordered. A new object will be written wherever the heap
 * finds some free space in its existing set of file blocks. If the heap
 * does not find a sufficient amount of free space, a new block will be requested
 * from the allocator.
 *
 * TODO: Implementation is not as good as i want it to be. Needs at least a garbage collection step.
 */
class heap {
private:
    struct page_entry {
        // 4 bits are used for metadata.
        static constexpr u64 min_block_size = 16;

        /// Most significant 4 bits (most are just reserved for now).
        static constexpr u64 metadata_mask = ~u64(0) << 60;

        static constexpr u64 large_object_bit = u64(1) << 63;

    public:
        /// Points to the block, with the most significant bits containing metadata.
        u64 m_value = 0;

        /// Number of allocated blocks. TODO make this a byte counter for large objects?
        u32 m_block_count = 0;

    public:
        page_entry() = default;

        page_entry(block_index index, bool large, u32 block_count) {
            PREQUEL_ASSERT(index, "Invalid block.");
            PREQUEL_ASSERT((index.value() & metadata_mask) == 0,
                           "Block index has invalid bits (block size too small).");
            m_value |= large ? large_object_bit : 0;
            m_value |= index.value();
            m_block_count = block_count;
        }

        block_index block() const { return block_index(m_value & ~metadata_mask); }

        u32 block_count() const { return m_block_count; }

        bool large_object() const { return (m_value & large_object_bit) != 0; }

        /// Entries are indexed by their address.
        struct derive_key {
            block_index operator()(const page_entry& entry) const { return entry.block(); }
        };

        static constexpr auto get_binary_format() {
            return make_binary_format(&page_entry::m_value, &page_entry::m_block_count);
        }
    };

    struct free_map_entry {
        /// Block index represented by this entry.
        /// Optimization note: Does not need all 64 bits.
        block_index block;

        /// Total number of bytes available for user data.
        u32 available = 0;

        free_map_entry() = default;
        free_map_entry(block_index block, u32 available)
            : block(block)
            , available(available) {}

        /// Entries are ordered by the amount of bytes available.
        /// Ties are broken by comparing the block address.
        bool operator<(const free_map_entry& other) const {
            return std::tie(available, block) < std::tie(other.available, other.block);
        }

        static constexpr auto get_binary_format() {
            return make_binary_format(&free_map_entry::block, &free_map_entry::available);
        }
    };

    /// Indexes pages by address.
    /// TODO: Distinguish between small and large objects (there are unneeded bits).
    using page_map_t = btree<page_entry, page_entry::derive_key>;

    /// Indexes pages by free byte count. Only contains pages with free space.
    using free_map_t = btree<free_map_entry>;

public:
    using reference = heap_reference;

    class anchor {
        page_map_t::anchor page_map;
        free_map_t::anchor free_map;

        /// Total number of all currently live objects.
        u64 objects_count = 0;

        /// Total size of all currently live objects.
        u64 objects_size = 0;

        /// Total number of blocks allocated for object storage.
        u64 blocks_count = 0;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::page_map, &anchor::free_map, &anchor::objects_size,
                                      &anchor::objects_count, &anchor::blocks_count);
        }

        friend heap;
        friend binary_format_access;
    };

public:
    heap(anchor_handle<anchor> _anchor, allocator& _alloc);

    heap(heap&&) noexcept = default;
    heap& operator=(heap&&) noexcept = default;

    engine& get_engine() const { return m_page_map.get_engine(); }
    allocator& get_allocator() const { return m_page_map.get_allocator(); }

    /**
     * Returns the total space of this datastructure on disk, in bytes.
     * Includes the size of the object storage, any free space currently not used
     * for anything and the storage required for metadata (such as the size of objects).
     */
    u64 byte_size() const;

    /**
     * Returns the total space currently occupied by object storage (including free space
     * left behind by freed objects). Does not include bookkeeping datastructures, but does
     * include space required by inline-metadata such as the object size.
     */
    u64 heap_size() const;

    /**
     * Returns the number of live (allocated but not freed) objects.
     */
    u64 objects_count() const;

    /**
     * Returns the total size of all live objects, in bytes.
     */
    u64 objects_size() const;

public:
    /**
     * Creates a new object of the given size (in bytes).
     * The object will be zeroed.
     *
     * Returns a reference to the new object.
     */
    heap_reference allocate(u32 object_size);

    /**
     * Creates a new object of the given size (in bytes).
     * The content of the new object is specified by `data`,
     * which must have at least `bytes` readable bytes.
     *
     * Returns a reference to the new object.
     */
    heap_reference allocate(const byte* object, u32 object_size);

    /**
     * Frees the object referenced by `ref`.
     * The reference (and all copies pointing to the same object) become
     * invalid and must no longer be used with this heap.
     *
     * It is possible that the heap will detect some accidential misuse (like
     * double free), in which case the heap will throw an exception. This behavior
     * is not guaranteed.
     */
    void free(heap_reference ref);

    /**
     * Returns the size of the referenced object, in bytes.
     */
    u32 size(heap_reference ref) const;

    /**
     * Loads the content of the referenced objects into the provided buffer.
     * The buffer size must match the size of the object, otherwise an exception will be thrown.
     */
    void load(heap_reference ref, byte* buffer, u32 buffer_size) const;

    /**
     * Updates the content of the referenced object by copying the provided buffer.
     * The buffer size must match the size of the object, otherwise an exception will be thrown.
     */
    void store(heap_reference ref, const byte* buffer, u32 buffer_size);

    void dump(std::ostream& os) const;

    void validate() const;

private:
    class page_handle;

private:
    template<typename ObjectVisitor>
    void visit_object(heap_reference ref, ObjectVisitor&& visitor) const;

private:
    heap_reference allocate_impl(const byte* data, u32 bytes);

    raw_address allocate_large_object(const byte* data, u32 bytes);
    void free_large_object(raw_address address);

    raw_address allocate_small_object(const byte* data, u32 bytes);
    void free_small_object(raw_address address);

    u32 page_allocate(page_handle& page, u32 object_size);
    void page_compact(page_handle& page);
    void page_free(page_handle& page, u32 slot_index);

    block_index allocate_blocks(u64 n);
    void free_blocks(block_index index, u64 n);

private:
    void set_blocks_count(u64 blocks_count);
    void set_objects_count(u64 objects_count);
    void set_objects_size(u64 objects_size);

    u64 blocks_count() const;

private:
    anchor_handle<anchor> m_anchor;
    page_map_t m_page_map;
    free_map_t m_free_map;
    u32 m_block_size;
    u32 m_max_small_object;

    // Buffer for page compactions. Tuple of (slot_index, object_location, object_size).
    std::vector<std::tuple<u32, u32, u32>> m_slot_buffer;
};

} // namespace prequel

#endif // PREQUEL_HEAP_HPP
