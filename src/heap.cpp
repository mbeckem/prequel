#include <prequel/heap.hpp>

#include <prequel/assert.hpp>
#include <prequel/deferred.hpp>
#include <prequel/engine.hpp>
#include <prequel/serialization.hpp>

#include <optional>

namespace prequel {

namespace {

/**
 * Slots point to objects within the same page.
 * Slots have a fixed address, but the object's storage can move.
 */
class slot {
public:
    slot() = default;

    slot(u32 offset, u32 size)
        : m_offset(offset)
        , m_size(size) {
        PREQUEL_ASSERT(offset > 0, "Invalid offset.");
    }

    bool valid() const { return m_offset != 0; }

    u32 offset() const {
        PREQUEL_ASSERT(valid(), "slot must be valid.");
        return m_offset;
    }

    u32 size() const {
        PREQUEL_ASSERT(valid(), "slot must be valid.");
        return m_size;
    }

    void reset() { *this = slot(); }

private:
    friend binary_format_access;

    static constexpr auto get_binary_format() {
        return binary_format(&slot::m_offset, &slot::m_size);
    }

private:
    u32 m_offset = 0; // offset from the block start (0 means invalid)
    u32 m_size = 0;   // size of the value, in bytes
};

struct page_header {
    /**
     * Number of existing slots (slot instances following immediately after this header).
     */
    u32 slot_count = 0;

    /**
     * Bytes before this offset in the block are considered free (up until the array of slots
     * or the page header).
     */
    u32 free_ptr = 0;

    /**
     * Free bytes (fragmented) higher than the free pointer.
     * These bytes only become available for allocations after the page has been compacted.
     */
    u32 free_fragmented = 0;

    static constexpr auto get_binary_format() {
        return binary_format(&page_header::slot_count, &page_header::free_ptr,
                             &page_header::free_fragmented);
    }
};

// This is important for slot alignment. Other parts of the code rely on the fact
// that slot addresses are always a multiple of 4.
static_assert(serialized_size<page_header>() % 4 == 0, "Header size is a multiple of 4.");

} // namespace

bool heap_reference::is_large_object() const {
    PREQUEL_ASSERT(valid(), "Invalid object.");
    return (m_value & large_bit) != 0;
}

bool heap_reference::is_small_object() const {
    return !is_large_object();
}

raw_address heap_reference::address() const {
    PREQUEL_ASSERT(valid(), "Invalid object.");
    return raw_address(m_value << 1);
}

heap_reference heap_reference::make_small_object(raw_address addr) {
    PREQUEL_ASSERT(addr, "Invalid address.");
    PREQUEL_ASSERT((addr.value() % 2) == 0, "Address must be a multiple of two.");
    return heap_reference(addr.value() >> 1);
}

heap_reference heap_reference::make_large_object(raw_address addr) {
    PREQUEL_ASSERT(addr, "Invalid address.");
    PREQUEL_ASSERT((addr.value() % 2) == 0, "Address must be a multiple of two.");
    return heap_reference((addr.value() >> 1) | large_bit);
}

/**
 * Page layout
 * ===========
 *
 * The page starts with the small page heaeder, followed by an arrow of slots.
 * Slots point into the rest of the page, where the values are located.
 * The slot arrow grows from "left" to "right", i.e. in increasing offsets.
 * Values are inserted from  the "right", i.e. in decreasing offsets.
 * Both parts of the page will eventually meet, at which point it will be considered full.
 *
 *
 * Optimization note
 * =================
 *
 * Slot instances are currently slightly too large in order to be general.
 * We could easily implement multiple page layouts at once (e.g. 32 bit slots
 * for small object sizes).
 */
class heap::page_handle {
public:
    page_handle(block_handle block)
        : m_block(std::move(block)) {
        PREQUEL_ASSERT(m_block.valid(), "Invalid block.");

        if (m_block.block_size() <= 128)
            PREQUEL_THROW(bad_argument("Block sizes this small are not supported."));
    }

    void init() {
        page_header header;
        header.free_ptr = m_block.block_size();
        m_block.set(0, header);
    }

    // The number of bytes available by decrementing the free pointer.
    u32 unfragmented_free_bytes() const {
        const page_header header = get_header();
        const u32 front_used =
            serialized_size<page_header>() + header.slot_count * serialized_size<slot>();
        PREQUEL_ASSERT(header.free_ptr >= front_used, "Corrupted free pointer.");
        return header.free_ptr - front_used;
    }

    // Total number of free bytes if we would compact the page.
    u32 free_bytes() const {
        const page_header header = get_header();
        const u32 free = unfragmented_free_bytes() + header.free_fragmented;
        PREQUEL_ASSERT(free <= m_block.block_size() - serialized_size<page_header>()
                                   - header.slot_count * serialized_size<slot>(),
                       "Corrupted free counter.");
        return free;
    }

    // Like free_bytes(), but includes the size we would need for a new slot
    // if there are no free slots available.
    u32 effective_free_bytes() const {
        const u32 free = free_bytes();
        if (find_free_slot()) {
            return free;
        }
        if (free > serialized_size<slot>()) {
            return free - serialized_size<slot>();
        }
        return 0;
    }

    // Returns the index of a free slot or an invalid optional if there is no such slot.
    std::optional<u32> find_free_slot() const {
        u32 slot_count = get_header().slot_count;
        for (u32 i = 0; i < slot_count; ++i) {
            slot slt = get_slot(i);
            if (!slt.valid())
                return i;
        }
        return std::nullopt;
    }

    // Remove all invalid slots at the end of the array.
    void reclaim_slots() {
        page_header header = get_header();

        u32 size = header.slot_count;
        while (size > 0 && !get_slot(size - 1).valid()) {
            --size;
        }

        if (size != header.slot_count) {
            header.slot_count = size;
            set_header(header);
        }
    }

    page_header get_header() const { return m_block.get<page_header>(0); }

    void set_header(const page_header& header) { m_block.set<page_header>(0, header); }

    slot get_slot(u32 slot_index) const { return m_block.get<slot>(slot_to_offset(slot_index)); }

    void set_slot(u32 slot_index, const slot& slt) { m_block.set(slot_to_offset(slot_index), slt); }

    const block_handle& block() const { return m_block; }

public:
    static constexpr u32 slot_to_offset(u32 slot_index) {
        return serialized_size<page_header>() + slot_index * serialized_size<slot>();
    }

    static constexpr u32 offset_to_slot(u32 offset) {
        PREQUEL_ASSERT(offset >= serialized_size<page_header>(), "Offset lies in page header.");
        offset -= serialized_size<page_header>();
        PREQUEL_ASSERT(offset % serialized_size<slot>() == 0,
                       "Offset is not a multiple of the slot size.");
        return offset / serialized_size<slot>();
    }

private:
    block_handle m_block;
};

heap::heap(anchor_handle<anchor> _anchor, allocator& _alloc)
    : m_anchor(std::move(_anchor))
    , m_page_map(m_anchor.member<&anchor::page_map>(), _alloc)
    , m_free_map(m_anchor.member<&anchor::free_map>(), _alloc) {
    m_block_size = get_engine().block_size();
    if (m_block_size < page_entry::min_block_size) {
        PREQUEL_THROW(bad_argument(fmt::format("Invalid block size (must be at least {} bytes).",
                                               page_entry::min_block_size)));
    }

    // All objects larger than this will be allocated with a block sequence on their own.
    m_max_small_object = (m_block_size * 3) / 4;

    // Reserve theoretical upper bound; no further memory allocations.
    m_slot_buffer.reserve(m_block_size / serialized_size<slot>());
}

u64 heap::byte_size() const {
    return m_page_map.byte_size() + m_free_map.byte_size() + heap_size();
}

u64 heap::heap_size() const {
    return blocks_count() * m_block_size;
}

u64 heap::objects_count() const {
    return m_anchor.get<&anchor::objects_count>();
}

void heap::set_objects_count(u64 objects_count) {
    m_anchor.set<&anchor::objects_count>(objects_count);
}

u64 heap::objects_size() const {
    return m_anchor.get<&anchor::objects_size>();
}

void heap::set_objects_size(u64 objects_size) {
    m_anchor.set<&anchor::objects_size>(objects_size);
}

u64 heap::blocks_count() const {
    return m_anchor.get<&anchor::blocks_count>();
}

void heap::set_blocks_count(u64 blocks_count) {
    m_anchor.set<&anchor::blocks_count>(blocks_count);
}

heap_reference heap::allocate(u32 object_size) {
    return allocate_impl(nullptr, object_size);
}

heap_reference heap::allocate(const byte* object, u32 object_size) {
    if (object == nullptr) {
        PREQUEL_THROW(bad_argument("Invalid data pointer."));
    }

    return allocate_impl(object, object_size);
}

// null data == zero memory
heap_reference heap::allocate_impl(const byte* object, u32 object_size) {
    if (object_size == 0) {
        PREQUEL_THROW(bad_argument("Zero sized allocation."));
    }

    heap_reference ref;
    if (object_size > m_max_small_object) {
        const raw_address header_address = allocate_large_object(object, object_size);
        ref = heap_reference::make_large_object(header_address);
    } else {
        const raw_address slot_address = allocate_small_object(object, object_size);
        ref = heap_reference::make_small_object(slot_address);
    }

    set_objects_count(objects_count() + 1);
    set_objects_size(objects_size() + object_size);
    return ref;
}

void heap::free(heap_reference ref) {
    if (!ref.valid()) {
        return;
    }

    const u32 object_size = size(ref);
    if (ref.is_large_object()) {
        free_large_object(ref.address());
    } else {
        free_small_object(ref.address());
    }

    PREQUEL_ASSERT(objects_count() > 0, "Corrupted objects count.");
    PREQUEL_ASSERT(objects_size() >= objects_size(), "Corrupted objects size.");
    set_objects_count(objects_count() - 1);
    set_objects_size(objects_size() - object_size);
}

u32 heap::size(heap_reference ref) const {
    struct visitor {
        u32 result = 0;

        void small_object(const page_handle& page, u32 offset, u32 size) {
            unused(page, offset);
            result = size;
        }

        void large_object(raw_address address, u32 size) {
            unused(address);
            result = size;
        }
    };

    visitor v;
    visit_object(ref, v);
    PREQUEL_ASSERT(v.result != 0, "Zero result cannot happen.");
    return v.result;
}

void heap::load(heap_reference ref, byte* buffer, u32 buffer_size) const {
    struct visitor {
        engine* eng = nullptr;
        byte* buffer = nullptr;
        u32 buffer_size = 0;

        void small_object(const page_handle& page, u32 offset, u32 size) {
            if (size != buffer_size) {
                PREQUEL_THROW(bad_argument("Size of the buffer does not match the object's size."));
            }
            std::memmove(buffer, page.block().data() + offset, size);
        }

        void large_object(raw_address address, u32 size) {
            if (size != buffer_size) {
                PREQUEL_THROW(bad_argument("Size of the buffer does not match the object's size."));
            }
            read(*eng, address, buffer, buffer_size);
        }
    };

    visitor v;
    v.eng = &get_engine();
    v.buffer = buffer;
    v.buffer_size = buffer_size;
    visit_object(ref, v);
}

void heap::store(heap_reference ref, const byte* buffer, u32 buffer_size) {
    struct visitor {
        engine* eng = nullptr;
        const byte* buffer = nullptr;
        u32 buffer_size = 0;

        void small_object(const page_handle& page, u32 offset, u32 size) {
            if (size != buffer_size) {
                PREQUEL_THROW(bad_argument("Size of the buffer does not match the object's size."));
            }
            std::memmove(page.block().writable_data() + offset, buffer, size);
        }

        void large_object(raw_address address, u32 size) {
            if (size != buffer_size) {
                PREQUEL_THROW(bad_argument("Size of the buffer does not match the object's size."));
            }
            write(*eng, address, buffer, buffer_size);
        }
    };

    visitor v;
    v.eng = &get_engine();
    v.buffer = buffer;
    v.buffer_size = buffer_size;
    visit_object(ref, v);
}

void heap::dump(std::ostream& os) const {
    fmt::print(os,
               "Heap state:\n"
               "  Storage allocated:  {} blocks\n"
               "  Objects allocated:  {} objects\n"
               "  Objects total size: {} bytes\n",
               blocks_count(), objects_count(), objects_size());

    if (!m_page_map.empty()) {
        fmt::print(os, "\n");
        fmt::print(os, "Allocated block ranges ({} total):\n", m_page_map.size());

        for (auto c = m_page_map.create_cursor(m_page_map.seek_min); c; c.move_next()) {
            page_entry entry = c.get();
            fmt::print(os, "  Start: {}, Size: {}, Large object: {}\n", entry.block(),
                       entry.block_count(), entry.large_object());
        }
    }

    if (!m_free_map.empty()) {
        fmt::print(os, "\n");
        fmt::print(os, "Free space on blocks:\n");

        for (auto c = m_free_map.create_cursor(m_free_map.seek_min); c; c.move_next()) {
            free_map_entry entry = c.get();
            fmt::print(os, "  Start: {}, Available: {}\n", entry.block, entry.available);
        }
    }
}

void heap::validate() const {
    m_page_map.validate();
    m_free_map.validate();

    page_map_t::cursor pc = m_page_map.create_cursor();
    free_map_t::cursor fc = m_free_map.create_cursor();

    u64 computed_byte_size = m_page_map.byte_size() + m_free_map.byte_size();
    u64 computed_object_count = 0;
    u64 computed_object_size = 0;

    for (pc.move_min(); pc; pc.move_next()) {
        page_entry entry = pc.get();
        computed_byte_size += entry.block_count() * m_block_size;

        if (!entry.large_object()) {
            page_handle page = get_engine().read(entry.block());
            u32 available = page.effective_free_bytes();

            if (!fc.find(free_map_entry(entry.block(), available))) {
                PREQUEL_THROW(corruption_error("Unable to find free map entry for a heap page."));
            }

            u32 slot_count = page.get_header().slot_count;
            for (u32 i = 0; i < slot_count; ++i) {
                slot slt = page.get_slot(i);
                if (!slt.valid())
                    continue;

                computed_object_count += 1;
                computed_object_size += slt.size();
            }
        } else {
            ++computed_object_count;
            computed_object_size +=
                size(heap_reference::make_large_object(get_engine().to_address(entry.block())));
        }
    }

    for (fc.move_min(); fc; fc.move_next()) {
        free_map_entry free_entry = fc.get();

        if (!pc.find(free_entry.block)) {
            PREQUEL_THROW(corruption_error("Unable to find page entry for heap page in free map."));
        }

        page_entry entry = pc.get();
        if (entry.large_object()) {
            PREQUEL_THROW(corruption_error("Large object allocations cannot have free space."));
        }

        page_handle page = get_engine().read(entry.block());
        if (page.effective_free_bytes() != free_entry.available) {
            PREQUEL_THROW(corruption_error("Wrong number of available bytes in heap page."));
        }
    }

    if (computed_byte_size != byte_size()) {
        PREQUEL_THROW(corruption_error("Wrong byte size."));
    }
    if (computed_object_count != objects_count()) {
        PREQUEL_THROW(corruption_error("Wrong number of objects."));
    }
    if (computed_object_size != objects_size()) {
        PREQUEL_THROW(corruption_error("Wrong size of objects."));
    }
}

/*
 * Lookup the object's location on disk and pass all necessary information
 * about the object (large or small) to the visitor.
 */
template<typename ObjectVisitor>
void heap::visit_object(heap_reference ref, ObjectVisitor&& visitor) const {
    if (!ref.valid()) {
        PREQUEL_THROW(bad_argument("Invalid reference."));
    }

    const raw_address address = ref.address(); // Address on disk (header or slot)
    const block_index block = get_engine().to_block(address);

    // TODO: Introduce a runtime debugging flag for checked derefencing (this is slow)
    {
        auto cursor = m_page_map.find(block);
        if (!cursor) {
            PREQUEL_THROW(
                bad_argument("Corrupted reference, the storage is not in use by this heap."));
        }
        if (cursor.get().large_object() != ref.is_large_object()) {
            PREQUEL_THROW(bad_argument("Corrupted reference, the storage type is unexpected."));
        }
    }

    if (ref.is_small_object()) {
        const u32 offset_in_page = get_engine().to_offset(address);
        const u32 slot_index = page_handle::offset_to_slot(offset_in_page);

        const page_handle page = get_engine().read(block);
        const page_header header = page.get_header();
        if (slot_index >= header.slot_count) {
            PREQUEL_THROW(bad_argument("Corrupted reference, invalid slot index."));
        }

        const slot slt = page.get_slot(slot_index);
        visitor.small_object(page, slt.offset(), slt.size());
    } else {
        PREQUEL_ASSERT(get_engine().to_offset(address) == 0, "Must be at the start of the block.");

        // address points to the u32 header (the size), the object follows.
        u32 size = read(get_engine(), raw_address_cast<u32>(address));

        visitor.large_object(address + serialized_size<u32>(), size);
    }
}

// null data == zero memory
raw_address heap::allocate_large_object(const byte* object, u32 object_size) {
    /*
     * Disk layout: Sequence of blocks large enough for the object and the
     * serialized header in front of it (the size, 4 bytes).
     */
    using header = u32;

    const u32 allocation_size = object_size + serialized_size<header>();
    const u32 required_blocks = ceil_div(allocation_size, m_block_size);

    // Allocate a sequence of blocks for the new object.
    const block_index block = allocate_blocks(required_blocks);
    deferred block_guard = [&] { free_blocks(block, required_blocks); };

    // Insert the page into the index.
    auto page_insert_result = m_page_map.insert(page_entry(block, true, required_blocks));
    PREQUEL_ASSERT(page_insert_result.inserted, "The block index was not unique.");
    deferred page_guard = [&] { page_insert_result.position.erase(); };

    // Write the header at the start of the first block.
    const address<header> header_address(get_engine().to_address(block));
    write(get_engine(), header_address, object_size);

    // Initialize the rest of the storage with the object.
    const raw_address object_address = header_address.raw() + serialized_size<header>();
    if (object) {
        write(get_engine(), object_address, object, object_size);
    } else {
        zero(get_engine(), object_address, object_size);
    }

    page_guard.disable();
    block_guard.disable();
    return header_address;
}

// Address points to the header == the start of the block
void heap::free_large_object(raw_address addr) {
    PREQUEL_ASSERT(addr.valid(), "Address must be valid.");

    const block_index block = get_engine().to_block(addr);

    auto cursor = m_page_map.find(block);
    if (!cursor) {
        PREQUEL_THROW(bad_argument("Corrupted reference, the storage is not in use by this heap."));
    }

    // Must be consistent
    const page_entry entry = cursor.get();
    if (!entry.large_object()) {
        PREQUEL_THROW(
            bad_argument("Corrupted reference does not match the storage's index entry."));
    }

    free_blocks(block, entry.block_count());
    cursor.erase();
}

raw_address heap::allocate_small_object(const byte* object, u32 object_size) {
    // Initialize the new object.
    auto init_at_slot = [&](page_handle& page, u32 slot_index) {
        const slot slt = page.get_slot(slot_index);
        PREQUEL_ASSERT(slt.valid(), "Slot must be valid.");
        PREQUEL_ASSERT(slt.size() == object_size, "Invalid size for new allocation.");
        if (object) {
            std::memcpy(page.block().writable_data() + slt.offset(), object, object_size);
        } else {
            std::memset(page.block().writable_data() + slt.offset(), 0, object_size);
        }

        // Form a pointer to the new object. Pointers point to the raw address of their slot,
        // this enables the "real" object data to be moved in page compactions without making
        // existing pointers invalid.
        return get_engine().to_address(page.block().index())
               + page_handle::slot_to_offset(slot_index);
    };

    // Try to allocate the new object on an existing page with sufficient free space.
    // This code currently selects the page with the least amount of free space
    // that is still sufficient to satisfy the size request. This approach will likely
    // have to be revisited.
    {
        const free_map_entry search_key(block_index(), object_size);
        if (auto free_entry_pos = m_free_map.lower_bound(search_key); free_entry_pos) {
            PREQUEL_ASSERT(free_entry_pos.get().available >= object_size,
                           "Invalid free entry for the requested byte size.");

            const block_index block = free_entry_pos.get().block;
            page_handle page = get_engine().read(block);
            PREQUEL_ASSERT(page.effective_free_bytes() == free_entry_pos.get().available,
                           "Inconsistent count of available bytes.");

            // Allocate a new slot for the object.
            const u32 slot_index = page_allocate(page, object_size);
            deferred slot_guard = [&] { page_free(page, slot_index); };

            const raw_address slot_address = init_at_slot(page, slot_index);

            // Register the remaining free space.
            if (u32 available = page.effective_free_bytes(); available > 0) {
                m_free_map.insert(free_map_entry(block, available));
            }
            slot_guard.disable();
            free_entry_pos.erase();
            return slot_address;
        }
    }

    // Allocate a new page for the object.
    const block_index block = allocate_blocks(1);
    deferred block_guard = [&] { free_blocks(block, 1); };

    page_handle page = get_engine().overwrite_zero(block);
    page.init();

    // Insert the page into the index.
    auto page_insert_result = m_page_map.insert(page_entry(block, false, 1));
    PREQUEL_ASSERT(page_insert_result.inserted, "The block index was not unique.");
    deferred page_guard = [&] { page_insert_result.position.erase(); };

    // Allocate a new slot for the object.
    const u32 slot_index = page_allocate(page, object_size);
    deferred slot_guard = [&] { page_free(page, slot_index); };

    const raw_address slot_address = init_at_slot(page, slot_index);

    // Register the remaining free space.
    if (u32 available = page.effective_free_bytes(); available > 0) {
        m_free_map.insert(free_map_entry(block, available));
    }
    slot_guard.disable();
    page_guard.disable();
    block_guard.disable();
    return slot_address;
}

void heap::free_small_object(raw_address slot_address) {
    const block_index page_index = get_engine().to_block(slot_address);
    const u32 slot_index = page_handle::offset_to_slot(get_engine().to_offset(slot_address));

    page_handle page = get_engine().read(page_index);
    const u32 old_available = page.effective_free_bytes();

    // Find the index entry for the page.
    auto cursor = m_page_map.find(page_index);
    if (!cursor) {
        PREQUEL_THROW(bad_argument("Corrupted reference, the storage is not in use by this heap."));
    }

    // Must be consistent
    const page_entry entry = cursor.get();
    if (entry.large_object()) {
        PREQUEL_THROW(
            bad_argument("Corrupted reference does not match the storage's index entry."));
    }

    // FIXME remove the free map implementation because it "allocates-on-free", which is just terrible.
    auto size_cursor = m_free_map.find(free_map_entry(page_index, old_available));
    PREQUEL_ASSERT(size_cursor, "Matching free map entry must exist.");

    // Free the storage on the page.
    page_free(page, slot_index);

    // Free completely empty pages and otherwise update the free map.
    if (page.get_header().slot_count == 0) {
        cursor.erase();
        size_cursor.erase();
        free_blocks(page_index, 1);
    } else {
        // Insert might allocate... replace with augmented b+tree and first fit allocation.
        size_cursor.erase();
        bool inserted = size_cursor.insert(free_map_entry(page_index, page.effective_free_bytes()));
        PREQUEL_ASSERT(inserted, "Must have been inserted successfully.");
        unused(inserted);
    }
}

/*
 * Allocates a fresh slot for the object of the given size (without initializing
 * the object data) and returns the index of the slot.
 * The page must have enough memory for both the slot and the object.
 */
u32 heap::page_allocate(page_handle& page, u32 object_size) {
    PREQUEL_ASSERT(page.effective_free_bytes() >= object_size, "Page must have enough free space.");

    // We need more storage than the object itself if we have to allocate a new slot.
    const std::optional<u32> existing_free_slot = page.find_free_slot();
    const u32 required_size = object_size + (existing_free_slot ? 0 : serialized_size<slot>());
    if (required_size > page.unfragmented_free_bytes()) {
        page_compact(page);
        PREQUEL_ASSERT(required_size <= page.unfragmented_free_bytes(),
                       "Not enough memory freed by page compaction.");
    }

    // Allocate a new slot at the end of the slots array or reuse the existing free slot.
    const u32 slot_index = [&]() {
        if (existing_free_slot)
            return *existing_free_slot;

        PREQUEL_ASSERT(page.unfragmented_free_bytes() >= serialized_size<slot>(),
                       "Not enough free space for the new slot.");
        page_header header = page.get_header();

        // Initialize the new slot to empty.
        u32 index = header.slot_count;
        header.slot_count += 1;
        page.set_slot(index, slot());
        page.set_header(header);
        return index;
    }();

    // Decrement the free pointer to allocate space for the new object.
    page_header header = page.get_header();
    header.free_ptr -= object_size;
    page.set_header(header);
    page.set_slot(slot_index, slot(header.free_ptr, object_size));
    return slot_index;
}

/*
 * Compaction algorithm for a single page:
 * - Iterate over the slots within that page in the reverse order of their object's locations
 *   while skipping empty slots, i.e. visit the objects in descending address-order.
 *   We will therefore visit the object closest to the end of the page first.
 * - Shift every object as much as possible to the end of the page, without leaving any gaps.
 * - Set `fragmented_free` counter to zero. Place the free pointer at the start of the first object.
 * - Everything below the free pointer (up to the slots at the start of the page) is free and unfragmented.
 */
void heap::page_compact(page_handle& page) {
    page_header header = page.get_header();

    // Tuple: (slot index, object offset, object size)
    std::vector<std::tuple<u32, u32, u32>>& slots = m_slot_buffer;
    {
        slots.clear();
        for (u32 i = 0; i < header.slot_count; ++i) {
            slot slt = page.get_slot(i);
            if (slt.valid()) {
                slots.push_back(std::tuple(i, slt.offset(), slt.size()));
            }
        }
        std::sort(slots.begin(), slots.end(), [&](const auto& lhs, const auto& rhs) {
            return std::get<1>(lhs) > std::get<1>(rhs); // Order by offset, descending.
        });
    }

    byte* data = page.block().writable_data();
    u32 dest_offset = m_block_size;
    for (const auto& [index, offset, size] : slots) {
        dest_offset -= size;
        if (dest_offset == offset)
            continue;

        // Shift object back.
        std::memmove(data + dest_offset, data + offset, size);

        // Register the objects new location.
        slot new_slot(dest_offset, size);
        page.set_slot(index, new_slot);
    }

    header.free_fragmented = 0;
    header.free_ptr = dest_offset;
    page.set_header(header);
}

void heap::page_free(page_handle& page, u32 slot_index) {
    page_header header = page.get_header();
    if (slot_index >= header.slot_count) {
        PREQUEL_THROW(
            bad_argument("Invalid object address (possible double free or data corruption)."));
    }

    slot slt = page.get_slot(slot_index);
    if (!slt.valid()) {
        PREQUEL_THROW(
            bad_argument("Invalid object address (possible double free or data corruption)."));
    }

    if (slt.offset() == header.free_ptr) {
        // Unlikely happy case: the object is the most recently allocated one (on this page)
        header.free_ptr += slt.size();
    } else {
        header.free_fragmented += slt.size();
    }
    page.set_header(header);
    page.set_slot(slot_index, slot());
    page.reclaim_slots();
}

block_index heap::allocate_blocks(u64 n) {
    block_index index = get_allocator().allocate(n);
    set_blocks_count(blocks_count() + n);
    return index;
}

void heap::free_blocks(block_index index, u64 n) {
    get_allocator().free(index, n);

    PREQUEL_ASSERT(blocks_count() >= n, "Corrupted blocks count.");
    set_blocks_count(blocks_count() - n);
}

} // namespace prequel
