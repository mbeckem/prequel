#include <prequel/default_allocator.hpp>

#include <prequel/detail/deferred.hpp>

#include <fmt/ostream.h>

#include <optional>

namespace prequel {

struct default_allocator::impl_t {
    using extent_position_cursor = extent_position_tree::cursor;
    using extent_size_cursor = extent_size_tree::cursor;

public:
    impl_t(anchor_handle<anchor> _anchor, engine& _engine, bool can_grow = true)
        : m_anchor(std::move(_anchor))
        , m_engine(_engine)
        , m_can_grow(can_grow)
        , m_meta_freelist(m_anchor.member<&anchor::meta_freelist>(), m_engine)
        , m_meta_alloc(this, m_engine)
        , m_extents_by_position(m_anchor.member<&anchor::extents_by_position>(), m_meta_alloc)
        , m_extents_by_size(m_anchor.member<&anchor::extents_by_size>(), m_meta_alloc) {}

    impl_t(const impl_t&) = delete;
    impl_t& operator=(const impl_t&) = delete;

    void add_region(block_index block, u64 size);

    block_index allocate(u64 request);

    block_index reallocate(block_index block, u64 size, u64 new_size);
    block_index reallocate(extent_t extent, u64 new_size) {
        return reallocate(extent.block, extent.size, new_size);
    }

    void free(block_index block, u64 size);
    void free(extent_t extent) { free(extent.block, extent.size); }

    allocation_stats stats() const;

    void dump(std::ostream& os) const;

    void validate() const;

    bool can_grow() const { return m_can_grow; }
    void can_grow(bool can_grow) { m_can_grow = can_grow; }

    u32 min_chunk() const { return m_min_chunk; }
    void min_chunk(u32 chunk_size) {
        if (chunk_size == 0)
            PREQUEL_THROW(bad_argument("Invalid chunk size."));
        m_min_chunk = chunk_size;
    }

private:
    void lazy_initialize();
    void fix_freelist();
    bool take_metadata(extent_t& free, u32 needed);

    extent_t allocate_from_front();

    // Removes the extent from both datastructures and keeps the free block counter in sync.
    void allocate_extent(const extent_t& extent, extent_position_cursor& by_pos,
                         extent_size_cursor& by_size);

    void free_internal(const extent_t& free);

    // used by the inner metadata allocator
    block_index allocate_metadata_block();
    void free_metadata_block(block_index addr);

    extent_t allocate_from_engine(u64 min_size);

private:
    /// Returns the appropriate allocation size for the requested number of blocks.
    u64 chunk_size(u64 blocks) const {
        PREQUEL_ASSERT(blocks > 0, "Zero sized allocation.");
        if (blocks < (u64(1) << 63))
            blocks = round_towards_pow2(blocks);
        return std::max(u64(m_min_chunk), blocks);
    }

    /// Returns the number of blocks that must be in the free list in order for a subsequent
    /// tree insertion (both trees) to succeed.
    u32 required_free_meta_blocks() const {
        const u32 req_pos = m_extents_by_position.height() + 1;
        const u32 req_size = m_extents_by_size.height() + 1;
        return req_pos + req_size;
    }

    /// Reserve this many blocks before allocating anything else.
    u32 required_total_meta_blocks() const { return 6; }

    /// Number of metadata blocks we need before we can continue normal operations.
    u32 needed_metadata_blocks() const {
        const u32 needed_total = meta_total() < required_total_meta_blocks()
                                     ? required_total_meta_blocks() - meta_total()
                                     : 0;
        const u32 needed_free = meta_free() < required_free_meta_blocks()
                                    ? required_free_meta_blocks() - meta_free()
                                    : 0;
        return std::max(needed_free, needed_total);
    }

    /// Returns true iff the extents overlap each other.
    bool extents_overlap(const extent_t& a, const extent_t& b) const {
        PREQUEL_ASSERT(a.block && b.block, "Exents must be valid.");
        /*
         * Extents are half open intervals.
         * a is fully to the left of b if (a.block + a.size) <= b.block,
         * b is fully to the left of a if (b.block + b.size) <= a.block.
         * This is the logical negation because all other cases mean that they DO overlap:
         */
        return (a.block < b.block + b.size) && (b.block < a.block + a.size);
    }

    /// Returns true iff the extents touch each other.
    bool extents_touch(const extent_t& left, const extent_t& right) const {
        PREQUEL_ASSERT(left.block && right.block, "Extents must be valid.");
        return left.block + left.size == right.block;
    }

    /// Returns true iff the extent is exactly at the end of file.
    bool borders_end(const extent_t& e) const {
        PREQUEL_ASSERT(e.block, "Extent must be valid.");
        return e.block + e.size == block_index(m_engine.size());
    }

    /// Returns a cursor to the smallest free extent >= request.
    extent_size_cursor find_best_fit(u64 request) {
        // Invalid blockindex is < all valid block indices.
        extent_t key;
        key.block = block_index();
        key.size = request;
        return m_extents_by_size.lower_bound(key);
    }

    extent_position_cursor find_position_entry(const extent_t& extent) {
        auto cursor = m_extents_by_position.find(extent.block);
        PREQUEL_ASSERT(cursor, "Position entry for that extent must exist.");
        return cursor;
    }

    extent_size_cursor find_size_entry(const extent_t& extent) {
        auto cursor = m_extents_by_size.find(extent);
        PREQUEL_ASSERT(cursor, "Size entry for that extent must exist.");
        return cursor;
    }

private:
    // Allocates data for the internal btrees. Uses the meta free list for allocations.
    struct metadata_allocator : public allocator {
        impl_t* parent; // impl address remains stable, it's on the heap.

        metadata_allocator(impl_t* _impl, engine& _engine)
            : allocator(_engine)
            , parent(_impl) {}

        metadata_allocator(const metadata_allocator&) = delete;
        metadata_allocator& operator=(const metadata_allocator&) = delete;

    protected:
        block_index do_allocate(u64 n) override {
            if (n != 1)
                PREQUEL_THROW(bad_argument("Cannot allocate sizes other than 1."));
            return parent->allocate_metadata_block();
        }

        block_index do_reallocate(block_index, u64, u64) override {
            PREQUEL_THROW(bad_argument("Cannot reallocate meta data blocks."));
        }

        void do_free(block_index addr, u64 n) override {
            if (n != 1)
                PREQUEL_THROW(bad_argument("Cannot free sizes other than 1."));
            parent->free_metadata_block(addr);
        }
    };

private:
    void set_meta_free(u64 meta_free) { m_anchor.set<&anchor::meta_free>(meta_free); }

    void set_meta_total(u64 meta_total) { m_anchor.set<&anchor::meta_total>(meta_total); }

    void set_data_free(u64 data_free) { m_anchor.set<&anchor::data_free>(data_free); }

    void set_data_total(u64 data_total) { m_anchor.set<&anchor::data_total>(data_total); }

    u64 meta_free() const { return m_anchor.get<&anchor::meta_free>(); }
    u64 meta_total() const { return m_anchor.get<&anchor::meta_total>(); }
    u64 data_free() const { return m_anchor.get<&anchor::data_free>(); }
    u64 data_total() const { return m_anchor.get<&anchor::data_total>(); }

private:
    anchor_handle<anchor> m_anchor;
    engine& m_engine;
    bool m_can_grow = true;

    /// Minimum allocation size for block chunks on file growth.
    u32 m_min_chunk = 128;

    /// Free metadata blocks.
    detail::free_list m_meta_freelist;

    /// Allocates metadata blocks for internal datastructures.
    metadata_allocator m_meta_alloc;

    /// Indexes free extents using their position.
    extent_position_tree m_extents_by_position;

    /// Indexes free extents using their size.
    extent_size_tree m_extents_by_size;
};

// --------------------------------
//
//   Allocator implementation
//
// --------------------------------

void default_allocator::impl_t::add_region(block_index block, u64 size) {
    if (!block)
        PREQUEL_THROW(bad_argument("Invalid block index."));
    if (size == 0)
        PREQUEL_THROW(bad_argument("Empty region."));

    if (block.value() >= m_engine.size() || size > m_engine.size() - block.value())
        PREQUEL_THROW(bad_argument("Extent is out of bounds."));

    free(block, size);
    set_data_total(data_total() + size);
}

block_index default_allocator::impl_t::allocate(u64 request) {
    PREQUEL_ASSERT(request > 0, "Invalid allocation size.");

    // Reserve some metadata blocks on the first allocation.
    lazy_initialize();

    // Reserve capacity in the freelist.
    fix_freelist();

    // Attempt to use (part of) an existing free extent.
    // The remainder (i.e. the unneeded space) will remain free.
    if (extent_size_cursor best_fit_size = find_best_fit(request); best_fit_size) {
        const extent_t free_extent = best_fit_size.get();
        PREQUEL_ASSERT(free_extent.size >= request, "Best fit must be large enough.");

        extent_position_cursor best_fit_pos = find_position_entry(free_extent);
        allocate_extent(free_extent, best_fit_pos, best_fit_size);

        const extent_t remainder(free_extent.block + request, free_extent.size - request);
        free(remainder);
        return free_extent.block;
    }

    // If the last extent borders the end of file, we can grow it.
    if (extent_position_cursor last_pos =
            m_extents_by_position.create_cursor(m_extents_by_position.seek_max);
        last_pos) {
        const extent_t last_extent = last_pos.get();
        PREQUEL_ASSERT(last_extent.size < request,
                       "Must not be able to satisfy allocation (see best fit above).");

        if (borders_end(last_extent)) {
            extent_size_cursor last_size = find_size_entry(last_extent);

            const extent_t allocated = allocate_from_engine(request - last_extent.size);
            PREQUEL_ASSERT(extents_touch(last_extent, allocated), "Must be contiguous.");

            allocate_extent(last_extent, last_pos, last_size);

            const extent_t remainder(last_extent.block + request,
                                     last_extent.size + allocated.size - request);
            free(remainder);
            return last_extent.block;
        }
    }

    // Otherwise: just try to allocate from the end of the file.
    const extent_t allocated = allocate_from_engine(request);
    const extent_t remainder(allocated.block + request, allocated.size - request);
    free(remainder);
    return allocated.block;
}

block_index default_allocator::impl_t::reallocate(block_index block, u64 size, u64 new_size) {
    // Checked by base allocator class.
    PREQUEL_ASSERT(block, "Invalid block index.");
    PREQUEL_ASSERT(size > 0, "Invalid allocation size.");

    // Reserve some metadata blocks on the first allocation.
    lazy_initialize();

    // Size unchanged.
    if (size == new_size) {
        return block;
    }

    // Shrink.
    if (new_size < size) {
        const extent_t remainder(block + new_size, size - new_size);
        free(remainder);
        return block;
    }

    {
        const extent_t extent(block, size);

        // Amount of needed blocks.
        const u64 need = new_size - size;

        extent_position_cursor next_pos = m_extents_by_position.lower_bound(block);

        // Try to take space from the righ neighbor, if its free.
        if (next_pos) {
            const extent_t next_extent = next_pos.get();

            // The right neighbor is contiguous and satisfiy the request on its own.
            if (extents_touch(extent, next_extent) && next_extent.size >= need) {
                extent_size_cursor next_size = find_size_entry(next_extent);

                allocate_extent(next_extent, next_pos, next_size);

                const extent_t remainder(next_extent.block + need, next_extent.size - need);
                free(remainder);
                return block;
            }
        }

        // Try to allocate from the left neighbor, if any.
        extent_position_cursor prev_pos;
        if (next_pos) {
            prev_pos = next_pos;
            prev_pos.move_prev();
        } else {
            prev_pos = m_extents_by_position.create_cursor(m_extents_by_position.seek_max);
        }

        // Try to take storage from the left.
        if (prev_pos) {
            const extent_t prev_extent = prev_pos.get();

            // The left extent is contiguous can satisfy the request.
            // Move the data to the new location and return it.
            if (extents_touch(prev_extent, extent) && prev_extent.size >= need) {
                extent_size_cursor prev_size = find_size_entry(prev_extent);

                allocate_extent(prev_extent, prev_pos, prev_size);
                copy_blocks(m_engine, block, prev_extent.block, size);

                const extent_t remainder(prev_extent.block + new_size,
                                         prev_extent.size + size - new_size);
                free(remainder);
                return prev_extent.block;
            }
        }

        // Do this after checking the left neighbor in order to be space conservative
        // and to not throw an error when growth is forbidden.
        if (next_pos) {
            const extent_t next_extent = next_pos.get();

            // The right neighbor is not large enough but it borders the end of the file,
            // so we can just grow the file.
            if (extents_touch(extent, next_extent) && borders_end(next_extent)) {
                const u64 grow_by = need - next_extent.size;
                const extent_t allocated = allocate_from_engine(grow_by);
                PREQUEL_ASSERT(extents_touch(next_extent, allocated), "Must be contiguous.");

                extent_size_cursor next_size = find_size_entry(next_extent);
                allocate_extent(next_extent, next_pos, next_size);

                const extent_t remainder(allocated.block + grow_by, allocated.size - grow_by);
                free(remainder);
                return block;
            }
        }

        // If the extent is itself at the file end, just grow it.
        if (borders_end(extent)) {
            const extent_t allocated = allocate_from_engine(need);
            PREQUEL_ASSERT(extents_touch(extent, allocated), "Must be contiguous.");

            const extent_t remainder(allocated.block + need, allocated.size - need);
            free(remainder);
            return block;
        }
    }

    // Failed to reuse a free extent, allocate new storage and move over.
    const block_index new_block = allocate(new_size);
    copy_blocks(m_engine, block, new_block, size);
    free(block, size);
    return new_block;
}

void default_allocator::impl_t::free(block_index block, u64 size) {
    // Checked by base allocator class.
    PREQUEL_ASSERT(block, "Invalid block index.");
    if (size == 0)
        return;

    // Reserve capacity in the freelist.
    fix_freelist();

    // Take data from the free block for the freelist (if necessary),
    // then free the rest.
    extent_t free_extent(block, size);
    take_metadata(free_extent, needed_metadata_blocks());
    if (free_extent.size > 0) {
        free_internal(free_extent);
    }
}

default_allocator::allocation_stats default_allocator::impl_t::stats() const {
    PREQUEL_ASSERT(data_free() + meta_total() <= data_total(), "Inconsistent state.");

    allocation_stats stats;
    stats.data_total = data_total();
    stats.data_free = data_free();
    stats.meta_data = meta_total();
    stats.data_used = stats.data_total - stats.data_free - stats.meta_data;
    return stats;
}

void default_allocator::impl_t::lazy_initialize() {
    if (meta_total() >= required_total_meta_blocks()) {
        return;
    }

    const u32 required = required_total_meta_blocks() - meta_total();
    extent_t extent = allocate_from_engine(required);
    free(extent.block, extent.size);
    PREQUEL_ASSERT(meta_total() > 0, "Metadata was reserved.");
}

void default_allocator::impl_t::fix_freelist() {
    // Drain the free list by freeing surplus blocks.
    while (meta_free() > required_free_meta_blocks() && meta_total() > required_total_meta_blocks()) {
        extent_t free(m_meta_freelist.pop(), 1);
        set_meta_free(meta_free() - 1);
        set_meta_total(meta_total() - 1);

        free_internal(free);
    }

    // Fill the free list by allocating from the start of the extent tree.
    while (meta_free() < required_free_meta_blocks()) {
        // Make sure to preallocate enough blocks.
        // We allocate a few blocks to fill up the total [enough for a few nodes per tree]
        // and then make sure that there are always enough blocks in the freelist.
        const u32 needed = needed_metadata_blocks();

        // this can cause the tree to shrink; thereby putting blocks
        // we thought we would need back on the free list.
        // this is why the "take" parameter is computed beforehand;
        // this might overestimate, but it is guaranteed to always terminate.
        extent_t front = allocate_from_front();
        if (!front.block)
            break;

        take_metadata(front, needed);
        if (front.size > 0)
            free_internal(front);
    }
}

/*
 * Allocates the extent with the lowest address.
 */
default_allocator::extent_t default_allocator::impl_t::allocate_from_front() {
    auto by_pos = m_extents_by_position.create_cursor(m_extents_by_position.seek_min);
    if (!by_pos) {
        return extent_t();
    }

    const extent_t free_extent = by_pos.get();
    PREQUEL_ASSERT(free_extent.block && free_extent.size > 0, "Invalid extent.");

    auto by_size = find_size_entry(free_extent);

    allocate_extent(free_extent, by_pos, by_size);
    return free_extent;
}

void default_allocator::impl_t::allocate_extent(const extent_t& extent,
                                                extent_position_cursor& by_pos,
                                                extent_size_cursor& by_size) {
    PREQUEL_ASSERT(extent.block, "Invalid extent.");
    PREQUEL_ASSERT(by_pos && by_pos.get() == extent, "Inconsistent by_pos cursor.");
    PREQUEL_ASSERT(by_size && by_size.get() == extent, "Inconsistent by_size cursor.");

    // This can free metadatablocks because the tree will shr
    by_pos.erase();
    by_size.erase();
    set_data_free(data_free() - extent.size);
}

/*
 * Inserts the extent into the free space tree, merging it with neighboring extents if possible.
 * Must have enough blocks in the free list.
 */
void default_allocator::impl_t::free_internal(const extent_t& free) {
    PREQUEL_ASSERT(free.block && free.size > 0, "Empty or invalid extent.");
    PREQUEL_ASSERT(meta_free() >= required_free_meta_blocks(),
                   "Not enough free metadata blocks for insertion.");

    // Merge left and right neighbor into the entry if possible, then insert it into the tree.
    extent_t entry = free;
    extent_t right;
    extent_t left;
    extent_position_cursor by_pos_right;
    extent_position_cursor by_pos_left;
    extent_size_cursor by_size;

    // Find the right neighbor (if mergeable).
    if (by_pos_right = m_extents_by_position.lower_bound(entry.block); by_pos_right) {
        right = by_pos_right.get();
        if (extents_overlap(entry, right))
            PREQUEL_THROW(bad_argument("Free extents overlap, storage corrupted."));

        if (extents_touch(entry, right)) {
            entry.size += right.size;
        } else {
            right = extent_t();
        }
    }

    // Left neighbor is left of the right neighbor (if it exists) or at the maximum.
    if (by_pos_right) {
        by_pos_left = by_pos_right;
        by_pos_left.move_prev();
    } else {
        by_pos_left = m_extents_by_position.create_cursor(m_extents_by_position.seek_max);
    }

    // Find the left neighbor (if mergeable).
    if (by_pos_left) {
        left = by_pos_left.get();
        if (extents_overlap(entry, left))
            PREQUEL_THROW(bad_argument("Free extents overlap, storage corrupted."));

        if (extents_touch(left, entry)) {
            entry.block = left.block;
            entry.size += left.size;
        } else {
            left = extent_t();
        }
    }

    // Erase the left and right member (if mergeable).
    by_size = m_extents_by_size.create_cursor();
    if (right.block) {
        bool found = by_size.find(right);
        PREQUEL_ASSERT(found, "Corresponding size entry for right block must exist.");
        unused(found);

        by_pos_right.erase();
        by_size.erase();
    }
    if (left.block) {
        bool found = by_size.find(left);
        PREQUEL_ASSERT(found, "Corresponding size entry for left block must exist.");
        unused(found);

        by_pos_left.erase();
        by_size.erase();
    }

    // Insert the (possibly merged) extent.
    bool insert_by_pos = by_pos_left.insert(entry);
    bool insert_by_size = by_size.insert(entry);
    PREQUEL_ASSERT(insert_by_pos && insert_by_size, "Insertion must have succeeded.");
    unused(insert_by_pos, insert_by_size);

    set_data_free(data_free() + free.size);
}

bool default_allocator::impl_t::take_metadata(extent_t& free, u32 needed) {
    if (free.size == 0 || needed == 0)
        return false;

    u32 take = needed;
    if (free.size < take)
        take = free.size;

    // Push in reversed order b/c freelist behaves like a stack.
    for (u64 i = take; i-- > 0;) {
        m_meta_freelist.push(free.block + i);
    }
    set_meta_free(meta_free() + take);
    set_meta_total(meta_total() + take);

    free.block += take;
    free.size -= take;
    return true;
}

block_index default_allocator::impl_t::allocate_metadata_block() {
    PREQUEL_ASSERT(!m_meta_freelist.empty(), "Must have enough metadata blocks preallocated.");
    PREQUEL_ASSERT(meta_free() > 0, "Inconsistent meta free value.");
    block_index block = m_meta_freelist.pop();
    set_meta_free(meta_free() - 1);
    return block;
}

void default_allocator::impl_t::free_metadata_block(block_index index) {
    m_meta_freelist.push(index);
    set_meta_free(meta_free() + 1);
}

default_allocator::extent_t default_allocator::impl_t::allocate_from_engine(u64 min_size) {
    PREQUEL_ASSERT(min_size > 0, "Zero sized allocation.");
    if (!m_can_grow) {
        PREQUEL_THROW(bad_alloc("No space left on device."));
    }

    const block_index start(m_engine.size());
    const u64 alloc_size = chunk_size(min_size); // TODO: Respect max file size
    m_engine.grow(alloc_size);
    set_data_total(data_total() + alloc_size);

    return extent_t(start, alloc_size);
}

void default_allocator::impl_t::dump(std::ostream& os) const {
    allocation_stats st = stats();
    fmt::print(os,
               "Default allocator state:\n"
               "  Data total:       {} blocks\n"
               "  Data used:        {} blocks\n"
               "  Data free:        {} blocks\n"
               "  Metadata:         {} blocks\n",
               st.data_total, st.data_used, st.data_free, st.meta_data);

    if (m_extents_by_position.size() > 0) {
        fmt::print(os, "\n");
        fmt::print(os, "Free extents ({} total):\n", m_extents_by_position.size());
        for (auto c = m_extents_by_position.create_cursor(m_extents_by_position.seek_min); c;
             c.move_next()) {
            extent_t extent = c.get();
            fmt::print(os, "  Start: {}, Size: {}\n", extent.block, extent.size);
        }
    }
}

void default_allocator::impl_t::validate() const {
    m_extents_by_position.validate();
    m_extents_by_size.validate();

    // Every entry in one tree must be mirrored in the other.
    u64 data_free = 0;

    {
        auto by_pos = m_extents_by_position.create_cursor();
        auto by_size = m_extents_by_size.create_cursor();

        by_pos.move_min();
        for (; by_pos; by_pos.move_next()) {
            const extent_t extent = by_pos.get();
            if (!by_size.find(extent)) {
                PREQUEL_THROW(corruption_error(
                    fmt::format("Failed to find by-size entry for extent at {}.", extent.block)));
            }
            if (extent != by_size.get()) {
                PREQUEL_THROW(corruption_error(
                    fmt::format("Unexpected extent in by-size tree at {}.", extent.block)));
            }

            data_free += extent.size;
        }

        by_size.move_min();
        for (; by_size; by_size.move_next()) {
            const extent_t extent = by_size.get();
            if (!by_pos.find(extent.block)) {
                PREQUEL_THROW(corruption_error(
                    fmt::format("Failed to find by-pos entry for extent at {}.", extent.block)));
            }
            if (extent != by_pos.get()) {
                PREQUEL_THROW(corruption_error(
                    fmt::format("Unexpected extent in by-pos tree at {}.", extent.block)));
            }
        }
    }

    if (data_free != this->data_free())
        PREQUEL_THROW(corruption_error("Unexpected number of free blocks."));
    if (meta_free() + m_extents_by_position.nodes() + m_extents_by_size.nodes() != this->meta_total())
        PREQUEL_THROW(corruption_error("Unexpected number of metadata blocks."));
}

// --------------------------------
//
//   Allocator public interface
//
// --------------------------------

default_allocator::default_allocator(anchor_handle<anchor> _anchor, engine& _engine)
    : allocator(_engine)
    , m_impl(std::make_unique<impl_t>(std::move(_anchor), _engine)) {}

default_allocator::default_allocator(anchor_handle<anchor> _anchor, engine& _engine, bool can_grow)
    : allocator(_engine)
    , m_impl(std::make_unique<impl_t>(std::move(_anchor), _engine, can_grow)) {}

default_allocator::~default_allocator() {}

void default_allocator::add_region(block_index block, u64 size) {
    impl().add_region(block, size);
}

default_allocator::allocation_stats default_allocator::stats() const {
    return impl().stats();
}

u32 default_allocator::min_chunk() const {
    return impl().min_chunk();
}

void default_allocator::min_chunk(u32 chunk_size) {
    impl().min_chunk(chunk_size);
}

bool default_allocator::can_grow() const {
    return impl().can_grow();
}

void default_allocator::can_grow(bool can_grow) {
    impl().can_grow(can_grow);
}

void default_allocator::dump(std::ostream& o) const {
    impl().dump(o);
}

void default_allocator::validate() const {
    impl().validate();
}

block_index default_allocator::do_allocate(u64 size) {
    return impl().allocate(size);
}

block_index default_allocator::do_reallocate(block_index block, u64 size, u64 new_size) {
    return impl().reallocate(block, size, new_size);
}

void default_allocator::do_free(block_index block, u64 size) {
    impl().free(block, size);
}

default_allocator::impl_t& default_allocator::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid default allocator.");
    return *m_impl;
}

} // namespace prequel
