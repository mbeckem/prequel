#include <extpp/default_allocator.hpp>

#include <fmt/ostream.h>

namespace extpp {

block_source::~block_source() {}

namespace {

class engine_block_source : public block_source {
public:
    engine_block_source(engine* _engine)
        : m_engine(_engine)
    {
        EXTPP_ASSERT(m_engine, "Engine must not be null.");
    }

    block_index begin() override { return block_index(0); }

    /// TODO: Max size for engines.
    u64 available() override { return u64(-1); }

    u64 size() override { return m_engine->size(); }

    void grow(u64 n) override { m_engine->grow(n); }

private:
    engine* m_engine = nullptr;
};

} // namespace

// TODO: Metadata blocks will interleave with normal data allocations
// and cause needless fragmentation because those allocations are not
// immediate neighbors. This needs improvement.
struct default_allocator::impl_t {
    using extent_cursor = extent_tree_t::cursor;
    using free_extent_cursor = free_extent_tree_t::cursor;

public:
    impl_t(anchor_handle<anchor> _anchor, engine& _engine)
        : impl_t(std::move(_anchor), _engine, nullptr, std::make_unique<engine_block_source>(&_engine))
    {}


    impl_t(anchor_handle<anchor> _anchor, engine& _engine, block_source& _source)
        : impl_t(std::move(_anchor), _engine, &_source, {})
    {}

private:
    // Either source or unique source must be valid.
    impl_t(anchor_handle<anchor> _anchor, engine& _engine, block_source* source, std::unique_ptr<block_source> unique_source)
        : m_anchor(std::move(_anchor))
        , m_engine(_engine)
        , m_source(source ? *source : *unique_source)
        , m_owned_source(std::move(unique_source))
        , m_meta_freelist(m_anchor.member<&anchor::meta_freelist>(), m_engine)
        , m_meta_alloc(this, m_engine)
        , m_extents(m_anchor.member<&anchor::extents>(), m_meta_alloc)
        , m_free_extents(m_anchor.member<&anchor::free_extents>(), m_meta_alloc)
    {
        m_metadata_total = m_anchor.get<&anchor::metadata_total>();
        m_metadata_free = m_anchor.get<&anchor::metadata_free>();

        m_data_total = m_anchor.get<&anchor::data_total>();
        m_data_free = m_anchor.get<&anchor::data_free>();
    }

public:
    impl_t(const impl_t&) = delete;
    impl_t& operator=(const impl_t&) = delete;

    block_index allocate(u64 request);
    block_index reallocate(block_index block, u64 request);
    void free(block_index block);

    u64 allocated_size(block_index index) const;

    u32 min_chunk() const { return m_min_chunk; }
    void min_chunk(u32 chunk_size) {
        if (chunk_size == 0)
            EXTPP_THROW(invalid_argument("Invalid chunk size"));
        m_min_chunk = chunk_size;
    }

    u32 min_meta_chunk() const { return m_min_meta_chunk; }
    void min_meta_chunk(u32 chunk_size) {
        if (chunk_size == 0)
            EXTPP_THROW(invalid_argument("Invalid meta chunk size"));
        m_min_meta_chunk = chunk_size;
    }

    allocation_stats_t stats() const;

    void dump(std::ostream& os) const;

    void validate() const;

private:
    block_index allocate_best_fit(u64 request);
    block_index allocate_new_space(u64 request);

    bool grow_in_place(extent_cursor& pos, extent_t& extent, u64 additional);
    std::pair<block_index, u64> allocate_blocks(u64 additional, u32 chunk_size);
    std::pair<block_index, u64> allocate_data_blocks(u64 additional);
    std::pair<block_index, u64> allocate_metadata_blocks(u64 additional);

    enum merge_direction_t {
        merge_none = 0,
        merge_left = 1 << 0,
        merge_right = 1 << 1,
    };

    // merge: flags merge_left / merge_right to merge with potential left/right neighbor.
    void register_free(extent_t extent, int merge);
    void register_free(extent_t& extent, extent_cursor& pos, int merge);

    void allocate_metadata_chunk();
    block_index allocate_metadata_block();
    void free_metadata_block(block_index addr);

private:
    /// Returns the appropriate allocation size for the requested number of blocks.
    u64 chunk_size(u64 blocks, u32 minimum) const {
        EXTPP_ASSERT(blocks > 0, "Zero sized allocation.");
        if (blocks < (u64(1) << 63))
            blocks = round_towards_pow2(blocks);
        return std::max(u64(minimum), blocks);
    }

    /// Add a new extent to the extents tree. The block address must be unique.
    extent_cursor add_extent(const extent_t& e) {
        EXTPP_ASSERT(e.size > 0, "Cannot register zero-sized extents.");

        auto result = m_extents.insert(e);
        EXTPP_ASSERT(result.second, "Block address was not unique.");
        return std::move(result.first);
    }

    /// Add a new extent to the free list. It is an error if an entry for `e`
    /// already exists.
    free_extent_cursor add_free(const extent_t& e) {
        EXTPP_ASSERT(e.free, "Extent must be free.");
        free_extent_t free;
        free.block = e.block;
        free.size = e.size;

        auto result = m_free_extents.insert(free);
        EXTPP_ASSERT(result.second, "Free extent entry was not unique.");
        return std::move(result.first);
    }

    /// Removes an extent from the free list. It is an error if the
    /// entry does not exist.
    void remove_free(const extent_t& e) {
        free_extent_t key;
        key.size = e.size;
        key.block = e.block;

        free_extent_cursor pos = m_free_extents.find(key);
        EXTPP_ASSERT(pos, "Free extent entry was not found.");
        pos.erase();
    }

    /// Returns the smallest extent that can fulfill the request and removes
    /// it from the free list. Returns an invalid extent if no such extent was found.
    free_extent_t pop_best_fit(u64 n) const {
        free_extent_cursor pos;
        if (n == 1) {
            pos = m_free_extents.create_cursor(m_free_extents.seek_min);
        } else {
            free_extent_t key;
            key.size = n;
            key.block = {};
            pos = m_free_extents.lower_bound(key);
        }

        if (!pos)
            return {};

        free_extent_t result = pos.get();
        pos.erase();
        return result;
    }

    /// Returns true iff the extents touch each other.
    bool extents_touch(const extent_t& left, const extent_t& right) const {
        EXTPP_ASSERT(left.block && right.block, "Extents must be valid.");
        return left.block + left.size == right.block;
    }

    /// Returns true iff the extent is exactly at the end of file.
    bool borders_end(const extent_t& e) const {
        EXTPP_ASSERT(e.block, "Extent must be valid.");
        return e.block + e.size == block_index(m_source.begin() + m_source.size());
    }

private:
    // Special-purpose allocator for the internal datastructures.
    struct metadata_allocator : public allocator {
        impl_t* parent; // impl address remains stable, it's on the heap.

        metadata_allocator(impl_t* _impl, engine& _engine)
            : allocator(_engine)
            , parent(_impl)
        {}

        metadata_allocator(const metadata_allocator&) = delete;
        metadata_allocator& operator=(const metadata_allocator&) = delete;

    protected:
        block_index do_allocate(u64 n) override {
            if (n != 1)
                EXTPP_THROW(invalid_argument("Cannot allocate sizes other than 1."));
            return parent->allocate_metadata_block();
        }

        block_index do_reallocate(block_index, u64) override {
            EXTPP_THROW(invalid_argument("Cannot reallocate meta data blocks."));
        }

        void do_free(block_index addr) override {
            parent->free_metadata_block(addr);
        }
    };

private:
    void set_metadata_total(u64 metadata_total) {
        m_metadata_total = metadata_total;
        m_anchor.set<&anchor::metadata_total>(metadata_total);
    }

    void set_metadata_free(u64 metadata_free) {
        m_metadata_free = metadata_free;
        m_anchor.set<&anchor::metadata_free>(metadata_free);
    }

    void set_data_total(u64 data_total) {
        m_data_total = data_total;
        m_anchor.set<&anchor::data_total>(data_total);
    }

    void set_data_free(u64 data_free) {
        m_data_free = data_free;
        m_anchor.set<&anchor::data_free>(data_free);
    }

    u64 metadata_total() const { return m_metadata_total; }
    u64 metadata_free() const { return m_metadata_free; }
    u64 data_total() const { return m_data_total; }
    u64 data_free() const { return m_data_free; }

private:
    anchor_handle<anchor> m_anchor;
    engine& m_engine;
    block_source& m_source;
    std::unique_ptr<block_source> m_owned_source; // Temp storage.

    /// Minimum allocation size for data blocks on file growth.
    u32 m_min_chunk = 128;

    /// Minimum allocation size for metadata blocks on file growth.
    u32 m_min_meta_chunk = 16;

    /// Free metadata blocks.
    detail::free_list m_meta_freelist;

    /// Allocates metadata blocks for internal datastructures.
    metadata_allocator m_meta_alloc;

    /// Tree of existing (used or free) extents.
    extent_tree_t m_extents;

    /// Tree of free extents, for lookup by size.
    free_extent_tree_t m_free_extents;

private:
    /// Total # of allocated metadata blocks.
    u64 m_metadata_total = 0;

    /// # of free metadata blocks.
    u64 m_metadata_free = 0;

    /// Total # of allocated data blocks.
    u64 m_data_total = 0;

    /// # of free data blocks.
    u64 m_data_free = 0;
};

// --------------------------------
//
//   Allocator implementation
//
// --------------------------------

// Find a free extent with at least `request` blocks and allocate from there.
// If that is not possible, allocate a new extent.
block_index default_allocator::impl_t::allocate(u64 request) {
    if (block_index addr = allocate_best_fit(request))
        return addr;
    return allocate_new_space(request);
}

// Do as many operations as possible in-place.
block_index default_allocator::impl_t::reallocate(block_index block, u64 request) {
    extent_cursor pos = m_extents.find(block);
    if (!pos) {
        EXTPP_THROW(invalid_argument("The block index passed to reallocate() does not point "
                                     "to a previous allocation."));
    }

    extent_t extent = pos.get();
    if (extent.free) {
        EXTPP_THROW(invalid_argument("Calling reallocate() on a previously freed address."));
    }

    // Size unchanged:
    if (request == extent.size)
        return block;

    // Shrink.
    if (request < extent.size) {
        const u64 remainder_size = extent.size - request;
        extent.size = request;
        pos.set(extent);

        register_free(extent_t(extent.block + request, remainder_size, true), merge_right);
        set_data_free(data_free() + remainder_size);
        return block;
    }

    // Try to grow without reallocation.
    if (grow_in_place(pos, extent, request - extent.size)) {
        return block;
    }

    // TODO: The might be a free neighbor to the left.

    // Allocate a new chunk that is large enough and copy the data over.
    block_index new_block = allocate(request);
    copy(m_engine,
         m_engine.to_address(new_block),
         m_engine.to_address(extent.block),
         extent.size * m_engine.block_size());

    free(extent.block);
    return new_block;
}

void default_allocator::impl_t::free(block_index block) {
    extent_cursor pos = m_extents.find(block);
    if (!pos) {
        EXTPP_THROW(invalid_argument("The block index passed to free() does not point "
                                     "to a previous allocation."));
    }

    extent_t extent = pos.get();
    if (extent.free) {
        EXTPP_THROW(invalid_argument("Double free detected."));
    }
    // TODO: Can improve error reporting by detecting if a was
    // freed and the free range was merged with its predecessor/successor.

    const u64 freed_size = extent.size;
    extent.free = true;
    pos.set(extent);

    register_free(extent, pos, merge_left | merge_right);
    set_data_free(data_free() + freed_size);
}

u64 default_allocator::impl_t::allocated_size(block_index index) const {
    extent_cursor pos = m_extents.find(index);
    if (pos) {
        extent_t extent = pos.get();
        if (!extent.free) {
            return extent.size;
        }
    }
    EXTPP_THROW(invalid_argument("The block index passed to allocated_size() does not point "
                                 "to a previous allocation"));
}

/// Try to serve a request by reusing an existing free extent.
/// This implements the best fit strategy, with ties being broken
/// using first fit strategy, i.e. the smallest fitting extent
/// with the lowest address is chosen.
block_index default_allocator::impl_t::allocate_best_fit(u64 request) {
    free_extent_t free_extent = pop_best_fit(request);
    if (!free_extent.block) {
        return block_index();
    }

    extent_cursor pos = m_extents.find(free_extent.block);
    EXTPP_ASSERT(pos, "Extent was not found.");
    extent_t extent = pos.get();

    EXTPP_ASSERT(extent.free, "Extent must be free.");
    EXTPP_ASSERT(extent.block == free_extent.block, "Blocks must match.");
    EXTPP_ASSERT(extent.size == free_extent.size, "Sizes must match.");
    EXTPP_ASSERT(extent.size >= request, "Extent must be large enough.");

    const u64 remainder_size = extent.size - request;
    extent.size = request;
    extent.free = false;
    pos.set(extent);

    if (remainder_size > 0) {
        register_free(extent_t(extent.block + request, remainder_size, true), merge_right);
    }

    set_data_free(data_free() - request);
    return extent.block;
}

/// Satisfies an allocation request by growing the underlying file.
/// We either grow the extent with the highest address (if it is free and borders
/// the end of the file) or we create a new extent.
block_index default_allocator::impl_t::allocate_new_space(u64 request) {
    if (metadata_total() ==  0)
        allocate_metadata_chunk();

    // Check the extent with the highest block index.
    auto [pos, extent] = [&]{
        auto pos = m_extents.create_cursor(m_extents.seek_max);
        if (pos) {
            auto extent = pos.get();
            if (extent.free && borders_end(extent)) {
                EXTPP_ASSERT(extent.size < request,
                             "Extent should have been chosen by best-fit allocation.");
                extent.free = false;
                remove_free(extent);
                set_data_free(data_free() - extent.size);
                return std::make_tuple(std::move(pos), std::move(extent));
            }
        }
        return std::make_tuple(extent_cursor(), extent_t());
    }();

    // If there is no viable candidate, start at the end of the file.
    if (!pos) {
        extent.block = block_index(m_source.begin() + m_source.size());
    }

    // Allocate space at the end of the file.
    const u64 required = request - extent.size;
    const auto [begin, allocated] = allocate_data_blocks(required);
    const u64 remainder = allocated - required;
    EXTPP_ASSERT(extent.block + extent.size == begin, "Unexpected allocated block index.");
    EXTPP_ASSERT(allocated >= required, "Basic allocation invariant.");
    EXTPP_ASSERT(extent.size + allocated >= request, "Insufficient allocation.");
    unused(begin);

    extent.size = request;
    if (pos) {
        pos.set(extent);
    } else {
        m_extents.insert(extent);
    }

    if (remainder > 0) {
        register_free(extent_t(extent.block + extent.size, remainder, true), merge_none);
        set_data_free(data_free() + remainder);
    }

    return extent.block;
}

/// Try to allocate `additional` blocks for the existing extent, without reallocation.
/// Either takes space from the right neighbor, from the end of the file or fails.
bool default_allocator::impl_t::grow_in_place(extent_cursor& pos, extent_t& extent, u64 additional) {
    EXTPP_ASSERT(pos, "Invalid cursor.");
    EXTPP_ASSERT(additional > 0, "Zero sized allocation.");

    // Attempt to allocate from the end of file.
    if (borders_end(extent)) {
        const auto [begin, allocated] = allocate_data_blocks(additional);
        EXTPP_ASSERT(extent.block + extent.size == begin, "Unexpected block index.");
        unused(begin);

        const u64 remainder = allocated - additional;
        extent.size += additional;
        pos.set(extent);

        if (remainder > 0) {
            register_free(extent_t(extent.block + extent.size, remainder, true), merge_none);
            set_data_free(data_free() + remainder);
        }
        return true;
    }

    // Try to allocate from the right free extent (if it exists).
    extent_cursor next_pos = pos;
    next_pos.move_next();
    if (!next_pos) {
        return false;
    }

    extent_t next_extent = next_pos.get();
    if (!next_extent.free || !extents_touch(extent, next_extent)) {
        return false;
    }

    // If the extent is large enough, we can use it.
    if (next_extent.size >= additional) {
        remove_free(next_extent);
    }
    // Otherwise, if the extent borders the end of the file, we can grow and use it.
    else if (next_extent.size < additional && borders_end(next_extent)) {
        remove_free(next_extent);
        const auto [begin, allocated] = allocate_data_blocks(additional - next_extent.size);
        EXTPP_ASSERT(next_extent.block + next_extent.size == begin, "Unexpected block index.");
        unused(begin);

        next_extent.size += allocated;
        set_data_free(data_free() + allocated);
    }
    // Otherwise: can't proceed.
    else {
        return false;
    }

    // Increase the size of the old extent.
    EXTPP_ASSERT(next_extent.size >= additional, "Neighbor is large enough.");
    extent.size += additional;
    pos.set(extent);

    // Shrink the size of the right neighbor.
    next_extent.block += additional;
    next_extent.size -= additional;
    if (next_extent.size > 0) {
        // Note: this part would profit from a more optimized insertion.
        // Walking through the tree is unnecessary because the position will be (almost!)
        // the same. Maybe implement a "replace" function that allows for different keys
        // and does a localized search for the correct spot. (Finger Search).
        next_pos.erase();
        next_pos.insert(next_extent);
        add_free(next_extent);
    } else {
        next_pos.erase();
    }
    set_data_free(data_free() - additional);
    return true;
}

/// Allocate at least `additional` blocks at the end of the file.
/// Returns the number of blocks that have been allocated.
std::pair<block_index, u64> default_allocator::impl_t::allocate_blocks(u64 additional, u32 chunk) {
    const u64 available = m_source.available();
    if (available < additional) {
        EXTPP_THROW(bad_alloc("Not enough space left on device."));
    }

    const block_index begin = m_source.begin();
    const u64 size = m_source.size();
    const u64 request = std::min(chunk_size(additional, chunk), available);
    m_source.grow(request);

    EXTPP_CHECK(m_source.size() == size + request, "Source did not grow by enough blocks.");
    return std::make_pair(begin + size, request);
}

std::pair<block_index, u64> default_allocator::impl_t::allocate_data_blocks(u64 additional) {
    auto range = allocate_blocks(additional, m_min_chunk);
    set_data_total(data_total() + range.second);
    return range;
}

std::pair<block_index, u64> default_allocator::impl_t::allocate_metadata_blocks(u64 additional) {
    auto range = allocate_blocks(additional, m_min_meta_chunk);
    set_metadata_total(metadata_total() + range.second);
    return range;
}

void default_allocator::impl_t::register_free(extent_t extent, int merge_direction) {
    extent_cursor pos;
    bool inserted;
    std::tie(pos, inserted) = m_extents.insert(extent);
    EXTPP_ASSERT(inserted, "The extent already existed.");
    unused(inserted);

    register_free(extent, pos, merge_direction);
}

void default_allocator::impl_t::register_free(extent_t& extent, extent_cursor& pos, int merge_direction) {
    EXTPP_ASSERT(extent.free, "Extent must be free.");

    // Inspect the left neighbor and, if possible, merge with it.
    if (merge_direction & merge_left) {
        extent_cursor neighbor = pos;
        neighbor.move_prev();

        if (neighbor) {
            extent_t prev = neighbor.get();
            if (prev.free && extents_touch(prev, extent)) {
                remove_free(prev);

                extent.block = prev.block;
                extent.size += prev.size;
                neighbor.set(extent);       // Must use neighbor bc of same key (block).
                pos.erase();
                pos = std::move(neighbor);
            }
        }
    }

    // Inspect the right neighbor and, if possible, merge with it.
    if (merge_direction & merge_right) {
        extent_cursor neighbor = pos;
        neighbor.move_next();

        if (neighbor) {
            extent_t next = neighbor.get();
            if (next.free && extents_touch(extent, next)) {
                extent.size += next.size;
                pos.set(extent);
                neighbor.erase();

                remove_free(next);
            }
        }
    }

    add_free(extent);
}

// Allocate a new chunk of metadata storage and put it on the free list.
void default_allocator::impl_t::allocate_metadata_chunk() {
    const auto [begin, allocated] = allocate_metadata_blocks(2);

    for (u64 i = 0; i < allocated; ++i) {
        m_meta_freelist.push(begin + i);
    }
    set_metadata_free(metadata_free() + allocated);
}

block_index default_allocator::impl_t::allocate_metadata_block() {
    // Ensure that there is at least one block on the free list.
    if (m_meta_freelist.empty()) {
        allocate_metadata_chunk();
    }

    block_index block = m_meta_freelist.pop();
    set_metadata_free(metadata_free() - 1);
    return block;
}

void default_allocator::impl_t::free_metadata_block(block_index block) {
    m_meta_freelist.push(block);
    set_metadata_free(metadata_free() + 1);
}

default_allocator::allocation_stats_t default_allocator::impl_t::stats() const {
    allocation_stats_t stats;
    stats.data_total = data_total();
    stats.data_free = data_free();
    stats.data_used = stats.data_total - stats.data_free;
    stats.metadata_total = metadata_total();
    stats.metadata_free = metadata_free();
    stats.metadata_used = stats.metadata_total - stats.metadata_free;
    return stats;
}

void default_allocator::impl_t::dump(std::ostream& os) const {
    auto st = stats();
    fmt::print(os,
               "Default allocator state:\n"
               "  Data total:       {} blocks\n"
               "  Data used:        {} blocks\n"
               "  Data free:        {} blocks\n"
               "  Metadata total:   {} blocks\n"
               "  Metadata used:    {} blocks\n"
               "  Metadata free:    {} blocks\n",
               st.data_total, st.data_used, st.data_free,
               st.metadata_total, st.metadata_used, st.metadata_free);
    fmt::print(os, "\n");

    fmt::print(os, "Allocated extents ({} total):\n", m_extents.size());
    for (auto c = m_extents.create_cursor(m_extents.seek_min); c; c.move_next()) {
        extent_t e = c.get();
        fmt::print(os, "  Start: {}, Length: {}, Free: {}\n", e.block, e.size, e.free);
    }
    fmt::print(os, "\n");

    fmt::print(os, "Freelist entries ({} total):\n", m_free_extents.size());
    for (auto c = m_free_extents.create_cursor(m_free_extents.seek_min); c; c.move_next()) {
        free_extent_t e = c.get();
        fmt::print(os, "  Start: {}, Length: {}\n", e.block, e.size);
    }
}

void default_allocator::impl_t::validate() const {
    m_extents.validate();
    m_free_extents.validate();

    auto ec = m_extents.create_cursor();
    auto ef = m_free_extents.create_cursor();

    {
        u64 data_free = 0;
        u64 data_total = 0;
        ec.move_min();
        for (; ec; ec.move_next()) {
            extent_t e = ec.get();
            data_total += e.size;
            if (e.free) {
                ef.find(free_extent_t(e.size, e.block));
                if (!ef) {
                    EXTPP_THROW(corruption_error(
                        fmt::format("Failed to find freelist entry for free block at {}", e.block)));
                }
                free_extent_t f = ef.get();
                if (f.size != e.size) {
                    EXTPP_THROW(corruption_error(
                        fmt::format("Free entry's size differs from extent's size at {}", e.block)));
                }
                data_free += e.size;
            } else {
                if (ef.find(free_extent_t(e.size, e.block))) {
                    EXTPP_THROW(corruption_error(
                        fmt::format("There is a freelist entry for the nonfree block at {}", e.block)));
                }
            }
        }

        if (data_total != m_data_total) {
            EXTPP_THROW(corruption_error(
                fmt::format("Wrong number of total data blocks (expected {} but observed {})",
                            m_data_total, data_total)));
        }
        if (data_free != m_data_free) {
            EXTPP_THROW(corruption_error(
                fmt::format("Wrong number of free data blocks (expected {} but observed {})",
                            m_data_free, data_free)));
        }


        for (; ef; ef.move_next()) {
            free_extent_t f = ef.get();
            ec.find(f.block);

            if (!ec) {
                EXTPP_THROW(corruption_error(
                    fmt::format("Failed to find extent for free entry at {}", f.block)));
            }

            extent_t e = ec.get();
            if (!e.free) {
                EXTPP_THROW(corruption_error(
                    fmt::format("Extent in freelist is not marked as free at {}", e.block)));
            }
        }
    }

    {
        u64 metadata_used = 0;

        auto counter = [&](auto&&) {
            metadata_used += 1;
            return true;
        };
        m_extents.visit(counter);
        m_free_extents.visit(counter);

        if (m_metadata_free + metadata_used != m_metadata_total) {
            EXTPP_THROW(corruption_error("Wrong number of metadata blocks."));
        }
    }
}

// --------------------------------
//
//   Allocator public interface
//
// --------------------------------

default_allocator::default_allocator(anchor_handle<anchor> _anchor, engine& _engine)
    : allocator(_engine)
    , m_impl(std::make_unique<impl_t>(std::move(_anchor), _engine))
{}

default_allocator::default_allocator(anchor_handle<anchor> _anchor, engine& _engine, block_source& source)
    : allocator(_engine)
    , m_impl(std::make_unique<impl_t>(std::move(_anchor), _engine, source))
{}

default_allocator::~default_allocator() {}

default_allocator::allocation_stats_t default_allocator::stats() const {
    return impl().stats();
}
u64 default_allocator::allocated_size(block_index index) const {
    return impl().allocated_size(index);
}

u32 default_allocator::min_chunk() const { return impl().min_chunk(); }

void default_allocator::min_chunk(u32 chunk_size) {
    impl().min_chunk(chunk_size);
}

u32 default_allocator::min_meta_chunk() const { return impl().min_meta_chunk(); }

void default_allocator::min_meta_chunk(u32 chunk_size) {
    impl().min_meta_chunk(chunk_size);
}

void default_allocator::dump(std::ostream& os) const {
    impl().dump(os);
}

void default_allocator::validate() const {
    impl().validate();
}

block_index default_allocator::do_allocate(u64 n) {
    return impl().allocate(n);
}

block_index default_allocator::do_reallocate(block_index a, u64 n) {
    return impl().reallocate(a, n);
}

void default_allocator::do_free(block_index a) {
    impl().free(a);
}

default_allocator::impl_t& default_allocator::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid default allocator.");
    return *m_impl;
}

} // namespace extpp
