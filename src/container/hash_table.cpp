#include <prequel/container/hash_table.hpp>

#include <prequel/exception.hpp>
#include <prequel/formatting.hpp>
#include <prequel/handle.hpp>
#include <prequel/math.hpp>

#include <fmt/ostream.h>

#include <array>
#include <vector>

namespace prequel {

namespace detail {

namespace {

static constexpr u32 max_key_size = 256;

using key_buffer = std::array<byte, max_key_size>;

// Hash tables with a larger load will grow.
static constexpr double max_fill_factor = 0.8;

// Hash tables with a lower load will shrink.
static constexpr double min_fill_factor = 0.5;

// Max power of two used as a bucket range size.
static constexpr u32 bucket_range_max_power = 20;

// Number of repeats for each power of two lower than bucket_range_max_power.
static constexpr u32 bucket_range_repeat = 4;

// Size of the precomputed size tables.
static constexpr u32 precomputed_bucket_ranges = (bucket_range_max_power + 1) * bucket_range_repeat;

// {1, 1, 1, 1, 2, 2, 2, 2, ..., 2^k, 2^k, 2^k, 2^k}
constexpr auto compute_bucket_range_sizes() {
    std::array<u64, precomputed_bucket_ranges> result{};
    u64 power = 1;
    for (u32 i = 0; i <= bucket_range_max_power; ++i) {
        for (u32 j = 0; j < bucket_range_repeat; ++j) {
            result[i * bucket_range_repeat + j] = power;
        }
        power *= 2;
    }
    return result;
}

// Computes the prefix sums for the given sizes array.
constexpr auto compute_bucket_range_sums(const std::array<u64, precomputed_bucket_ranges>& sizes) {
    std::array<u64, precomputed_bucket_ranges> sums{};
    u64 sum = 0;
    u64 index = 0;
    for (u64 value : sizes) {
        sum += value;
        sums[index++] = sum;
    }
    return sums;
}

// bucket_range_sizes[i] is the size (in buckets) of the given range.
// All other bucket range have the largest size times two.
static constexpr std::array<u64, precomputed_bucket_ranges> bucket_range_sizes =
    compute_bucket_range_sizes();

// bucket_range_size_sums[i] contains the sum of all bucket range sizes up the range i (inclusive).
// Used for efficient binary search.
//
// Simple closed form:
// u64 index = bucket_range_index / 4;
// u64 offset = bucket_range_index % 4;
// return 4 * ((u64(1) << index) - 1) + (u64(1) << index) * (offset + 1);
static constexpr std::array<u64, precomputed_bucket_ranges> bucket_range_size_sums =
    compute_bucket_range_sums(bucket_range_sizes);

/*
 * Primary buckets are stored (nearly) next to each other in contiguous storage.
 * Overflow buckets are allocated on demand and linked into the list.
 *
 * Important: Entries within a single bucket node are ordered by their hash value.
 *
 * TODO: Overflow nodes should collapse in order to reclaim space.
 * (Note that space will already eventually be reclaimed by split or shrink operations).
 */
class bucket_node {
private:
    struct header {
        // Points to the next overflow node (if any).
        block_index next;

        // Number of values in this node.
        u32 size = 0;

        static constexpr auto get_binary_format() {
            return binary_format(&header::next, &header::size);
        }
    };

public:
    bucket_node() = default;

    bucket_node(block_handle handle, u32 value_size, u32 capacity)
        : m_handle(std::move(handle), 0)
        , m_value_size(value_size)
        , m_capacity(capacity) {
        PREQUEL_ASSERT(capacity > 0, "Invalid capacity.");
        PREQUEL_ASSERT(value_size > 0, "Invalid value size.");
        PREQUEL_ASSERT(serialized_size<header>() + capacity * value_size
                           <= m_handle.block().block_size(),
                       "Capacity is too large.");
    }

    void init() const { m_handle.set(header()); }

    // The number of values that can fit into this node, not including the overflow lists.
    u32 capacity() const { return m_capacity; }

    block_index index() const { return m_handle.block().index(); }
    bool valid() const { return index().valid(); }
    explicit operator bool() const { return valid(); }

    bool full() const { return get_size() == capacity(); }
    bool empty() const { return get_size() == 0; }

    block_index get_next() const { return m_handle.get<&header::next>(); }

    void set_next(block_index new_next) const { m_handle.set<&header::next>(new_next); }

    u32 get_size() const { return m_handle.get<&header::size>(); }

    void set_size(u32 new_size) const { m_handle.set<&header::size>(new_size); }

    const byte* get_value(u32 index) const {
        PREQUEL_ASSERT(index < m_capacity, "Index out of bounds.");

        const u32 offset = offset_of_value(index);
        return m_handle.block().data() + offset;
    }

    void set_value(u32 index, const byte* value) const {
        PREQUEL_ASSERT(value, "Value is null.");
        PREQUEL_ASSERT(index < m_capacity, "Index over capacity.");

        const u32 offset = offset_of_value(index);
        byte* data = m_handle.block().writable_data() + offset;
        std::memmove(data, value, m_value_size);
    }

    u32 insert(u32 index, const byte* value) const {
        PREQUEL_ASSERT(!full(), "Node is full.");
        PREQUEL_ASSERT(index < m_capacity, "Index is over capacity.");
        PREQUEL_ASSERT(index <= get_size(), "Index is out of bounds.");

        const u32 size = get_size();
        byte* data = m_handle.block().writable_data();
        std::memmove(data + offset_of_value(index + 1), data + offset_of_value(index),
                     (size - index) * m_value_size);
        std::memmove(data + offset_of_value(index), value, m_value_size);
        set_size(size + 1);
        return index;
    }

    void remove(u32 index) const {
        PREQUEL_ASSERT(index < m_capacity, "Index over capacity.");
        PREQUEL_ASSERT(index < get_size(), "Index out of bounds.");

        const u32 size = get_size();

        byte* data = m_handle.block().writable_data();
        std::memmove(data + offset_of_value(index), data + offset_of_value(index + 1),
                     (size - index - 1) * m_value_size);
        set_size(size - 1);
    }

public:
    static u32 compute_capacity(u32 block_size, u32 value_size) {
        u32 header_size = serialized_size<header>();
        if (block_size <= header_size)
            return 0;

        return (block_size - header_size) / value_size;
    }

private:
    u32 offset_of_value(u32 value_index) const {
        PREQUEL_ASSERT(value_index <= m_capacity, "Value index out of bounds.");
        return serialized_size<header>() + m_value_size * value_index;
    }

private:
    handle<header> m_handle;
    u32 m_value_size = 0;
    u32 m_capacity = 0;
};

// Unique type to make byte array / void pointer mistakes impossible.
struct compatible_key_t {
    const void* data = nullptr;

    compatible_key_t(const void* data_)
        : data(data_) {}
};

} // namespace

// Returns the size (in buckets) of the given bucket range.
static constexpr u64 bucket_range_size(u64 bucket_range_index) {
    if (bucket_range_index < precomputed_bucket_ranges)
        return bucket_range_sizes[bucket_range_index];
    return bucket_range_sizes.back();
}

// Returns the combined size of the first `range_count` buckets.
static constexpr u64 total_bucket_count(u64 range_count) {
    if (range_count == 0)
        return 0;

    u64 range_index = range_count - 1;
    if (range_index < precomputed_bucket_ranges)
        return bucket_range_size_sums[range_index];

    constexpr u64 max_sum = bucket_range_size_sums.back();
    constexpr u64 max_size = bucket_range_sizes.back();
    return max_sum + (range_count - precomputed_bucket_ranges) * max_size;
}

// Given a bucket index, returns the index of the bucket range that contains that bucket
// and the offset of the bucket within the range.
static std::tuple<u64, u64> find_bucket_position(u64 bucket_index) {
    // Leftmost bucket range with end index > bucket_index. (Note: end index is exclusive).
    auto pos = std::upper_bound(bucket_range_size_sums.begin(), bucket_range_size_sums.end(),
                                bucket_index);
    if (pos != bucket_range_size_sums.end()) {
        // bucket index is in the precomputed table.
        u64 range_index = static_cast<u64>(pos - bucket_range_size_sums.begin());
        u64 range_start = *pos - bucket_range_size(range_index);
        PREQUEL_ASSERT(bucket_index >= range_start
                           && bucket_index < range_start + bucket_range_size(range_index),
                       "Bucket index is not in this bucket.");
        return std::tuple(range_index, bucket_index - range_start);
    }

    // bucket index is greater than this value.
    PREQUEL_ASSERT(bucket_range_size_sums.back() <= bucket_index, "Logic error.");
    static constexpr u64 max_sum = bucket_range_size_sums.back();
    static constexpr u64 max_size = bucket_range_sizes.back();

    u64 range_index = precomputed_bucket_ranges + (bucket_index - max_sum) / max_size;
    u64 bucket_offset = (bucket_index - max_sum) % max_size;
    return std::tuple(range_index, bucket_offset);
}

/*
 * Important terminology:
 *
 *  - bucket index
 *      Logical index of a *primary* bucket. Ranges from 0 to the number of buckets in the table (exclusive).
 *      Bucket indices are obtained by applying the hash function to a key (see bucket_for_hash()).
 *
 *  - bucket address
 *      The physical offset of a primary bucket within the file (as a block_index). The logical bucket indices
 *      are mapped to a discontiguous phyiscal space of block ranges, because the table grows dynamically
 *      in a file shared with other datastructures.
 *
 *  - bucket range
 *      An allocated array of contiguous blocks (used for bucket storage).
 *      The size of bucket ranges increases exponentially for some time (up to 2^20 blocks at the time of writing).
 *      After that, bucket ranges stay at the same size, so allocation is linear from there on.
 *
 *      Logical bucket indices are mapped to their appropriate bucket range using binary search.
 *      The address of a primary bucket is computed by first determining the bucket range it has been allocated
 *      in, followed by the computation of the bucket's offset within that range.
 *
 *      Note that old ranges are not freed, they will remain in use for the entire lifetime of the table.
 *
 *  - bucket range index
 *      The allocated bucket ranges are managed in a lookup table. Entry `i` in that table refers to the
 *      physical location of bucket range `i`.
 */
class raw_hash_table_impl : public uses_allocator {
public:
    using anchor = raw_hash_table_anchor;

public:
    raw_hash_table_impl(anchor_handle<anchor> _anchor, const raw_hash_table_options& _opts,
                        allocator& _alloc);

public:
    u32 value_size() const { return m_options.value_size; }
    u32 key_size() const { return m_options.key_size; }

    u64 size() const { return get_size(); }
    bool empty() const { return size() == 0; }

    u32 bucket_capacity() const { return m_bucket_capacity; }
    u64 primary_buckets() const { return get_primary_buckets(); }
    u64 overflow_buckets() const { return get_overflow_buckets(); }
    u64 allocated_primary_buckets() const { return total_bucket_count(m_bucket_ranges.size()); }
    u64 allocated_buckets() const { return allocated_primary_buckets() + overflow_buckets(); }

    u64 byte_size() const {
        return m_bucket_ranges.byte_size() + allocated_buckets() * get_engine().block_size();
    }

    // Returns the average fill factor of the table's primary buckets.
    double load() const;

    // TODO: cursor api

    bool insert(const byte* value, bool overwrite);
    bool contains(const byte* key) const;

    bool find(const byte* key, byte* value) const;
    bool find_compatible(const void* compatible_key,
                         const std::function<u64(const void*)>& compatible_hash,
                         const std::function<bool(const void*, const byte*)>& compatible_equals,
                         byte* value) const;

    bool erase(const byte* key);
    bool erase_compatible(const void* compatible_key,
                          const std::function<u64(const void*)>& compatible_hash,
                          const std::function<bool(const void*, const byte*)>& compatible_equal);

    void iterate(iteration_control (*iter_func)(const byte* value, void* user_data),
                 void* user_data) const;

    void
    visit(const std::function<iteration_control(const raw_hash_table::node_view&)>& visit_func) const;

    void dump(std::ostream& os) const;
    void validate() const;

    void clear();

private:
    // Find the given key using the provided hash and equality functions.
    // TODO: Generalize other operations too?
    template<typename KeyType, typename KeyHasher, typename KeyEquals>
    bool find_impl(const KeyType& key, const KeyHasher& hasher, const KeyEquals& equals,
                   byte* value) const;

    template<typename KeyType, typename KeyHasher, typename KeyEquals>
    bool erase_impl(const KeyType& key, const KeyHasher& hasher, const KeyEquals& equals);

    // Inserts the value at the appropriate position within the bucket or returns false
    // if an equal value has been found (returns that values position in that case).
    // Returns true if the insertion took place.
    bool insert_into_bucket(const bucket_node& primary_bucket, const byte* value, const byte* key,
                            u64 hash, bucket_node& found_node, u32& found_index);

    // Helper function for grow + split, should eventually be replaced.
    void insert_into_bucket(const bucket_node& primary_bucket, const byte* value);

    // Iterate through the bucket and try to find the key.
    bool find_in_bucket(const bucket_node& primary_bucket, const byte* key, u64 key_hash,
                        bucket_node& found_node, u32& found_index) const;

    template<typename KeyType, typename KeyEquals>
    bool find_in_bucket(const bucket_node& primary_bucket, const KeyType& key, u64 key_hash,
                        const KeyEquals& equals, bucket_node& found_node, u32& found_index) const;

    template<typename KeyType, typename KeyEquals>
    bool find_in_node(const bucket_node& node, const KeyType& search_key, u64 search_hash,
                      const KeyEquals& equals, u32& position) const;

    bool grow();
    bool shrink();

    void free_overflow_chain(block_index overflow_node);

private:
    // Bucket lookup management.
    // We need a lookup structure for bucket indices because we share the engine
    // with other datastructures (our own overflow lists and other structures created by the user).
    // We allocate block extents in large chunks so the bucket storage is mostly contiguous.

    // Find the primary bucket with the given index.
    block_index bucket_address(u64 index) const;

    // Allocates a new primary bucket for the given bucket index.
    bucket_node allocate_primary_bucket(u64 index);
    void free_primary_bucket(u64 index);

    // Allocates or frees a new overflow bucket.
    bucket_node allocate_overflow_bucket();
    void free_overflow_bucket(block_index bucket_ptr);

    // Reads the primary bucket with the given bucket index (must be in range).
    bucket_node read_primary_bucket(u64 index) const;

    // Reads the bucket at the given address in the file.
    bucket_node read_bucket(block_index bucket_ptr) const;

private:
    // Returns the appropriate primary bucket index for the given key (or hash).
    u64 bucket_for_value(const byte* value) const;
    u64 bucket_for_key(const byte* key) const;
    u64 bucket_for_hash(u64 hash) const;

    // Computes the hash of that value by deriving the key first, and then hashing the key.
    u64 value_hash(const byte* value) const;

    // Compute the key's hash value, using the user-provided callback function.
    u64 key_hash(const byte* key) const;

    // Returns true iff the keys are equal according to the user-provided callback function.
    bool key_equal(const byte* left_key, const byte* right_key) const;

    // Derive a key from the value, using the user-provided callback function.
    void derive_key(const byte* value, byte* key) const;

private:
    // Anchor access
    u64 get_size() const { return m_anchor.get<&anchor::size>(); }
    void set_size(u64 size) { m_anchor.set<&anchor::size>(size); }

    u64 get_primary_buckets() const { return m_anchor.get<&anchor::primary_buckets>(); }
    void set_primary_buckets(u64 buckets) { m_anchor.set<&anchor::primary_buckets>(buckets); }

    u64 get_overflow_buckets() const { return m_anchor.get<&anchor::overflow_buckets>(); }
    void set_overflow_buckets(u64 buckets) { m_anchor.set<&anchor::overflow_buckets>(buckets); }

    u64 get_step() const { return m_anchor.get<&anchor::step>(); }
    void set_step(u64 step) { m_anchor.set<&anchor::step>(step); }

    u8 get_level() const { return m_anchor.get<&anchor::level>(); }
    void set_level(u64 level) { m_anchor.set<&anchor::level>(level); }

private:
    anchor_handle<raw_hash_table_anchor> m_anchor;
    raw_hash_table_options m_options;
    array<block_index> m_bucket_ranges;

    u32 m_bucket_capacity = 0;
};

class raw_hash_table_node_view_impl {
public:
    raw_hash_table_node_view_impl() = default;
    raw_hash_table_node_view_impl(const raw_hash_table_node_view_impl&) = delete;
    raw_hash_table_node_view_impl& operator=(raw_hash_table_node_view_impl&) = delete;

    bool is_primary() const { return !m_is_overflow; }
    bool is_overflow() const { return m_is_overflow; }

    u64 bucket_index() const { return m_bucket_index; }
    block_index address() const { return m_node.index(); }
    block_index overflow_address() const { return m_node.get_next(); }

    u32 size() const { return m_node.get_size(); }
    const byte* value(u32 index) const {
        if (index >= size()) {
            PREQUEL_THROW(
                bad_argument(fmt::format("Index out of bounds: {} (size is {}).", index, size())));
        }

        return m_node.get_value(index);
    }

private:
    // Called by the hash table
    friend raw_hash_table_impl;

    void set_node(u64 bucket_index, bool is_overflow, bucket_node node) {
        m_bucket_index = bucket_index;
        m_is_overflow = is_overflow;
        m_node = std::move(node);
    }

private:
    u64 m_bucket_index = 0;
    bool m_is_overflow = false;
    bucket_node m_node;
};

raw_hash_table_impl::raw_hash_table_impl(anchor_handle<anchor> _anchor,
                                         const raw_hash_table_options& _opts, allocator& _alloc)
    : uses_allocator(_alloc)
    , m_anchor(std::move(_anchor))
    , m_options(_opts)
    , m_bucket_ranges(m_anchor.member<&anchor::bucket_ranges>(), _alloc) {
    if (m_options.value_size == 0)
        PREQUEL_THROW(bad_argument("Zero value size."));
    if (m_options.key_size == 0)
        PREQUEL_THROW(bad_argument("Zero key size."));
    if (m_options.key_size > max_key_size)
        PREQUEL_THROW(
            bad_argument(fmt::format("Key sizes larger than {} are not supported.", max_key_size)));
    if (!m_options.derive_key)
        PREQUEL_THROW(bad_argument("No derive_key function provided."));
    if (!m_options.key_hash)
        PREQUEL_THROW(bad_argument("No key_hash function provided."));
    if (!m_options.key_equal)
        PREQUEL_THROW(bad_argument("No key_equal function provided."));

    const u32 block_size = get_engine().block_size();

    m_bucket_capacity = bucket_node::compute_capacity(block_size, m_options.value_size);
    if (m_bucket_capacity == 0) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Block size {} is too small (cannot fit a single value into a bucket)",
                        get_engine().block_size())));
    }
}

bool raw_hash_table_impl::insert(const byte* value, bool overwrite) {
    if (!value)
        PREQUEL_THROW(bad_argument("Value is null."));

    if (get_primary_buckets() == 0) {
        PREQUEL_ASSERT(get_size() == 0, "Empty hash tables have no elements.");
        PREQUEL_ASSERT(get_level() == 0, "Empty hash tables have 0 level.");
        PREQUEL_ASSERT(get_step() == 0, "Empty hash table cannot have nonzero step pointers.");
        allocate_primary_bucket(0);
    }

    key_buffer key;
    derive_key(value, key.data());

    const u64 hash = key_hash(key.data());
    const u64 bucket_index = bucket_for_hash(hash);
    const bucket_node bucket = read_primary_bucket(bucket_index);

    {
        bucket_node found_node;
        u32 found_index = 0;
        if (!insert_into_bucket(bucket, value, key.data(), hash, found_node, found_index)) {
            if (overwrite) {
                found_node.set_value(found_index, value);
                return true;
            }

            return false;
        }
    }

    set_size(get_size() + 1);

    while (load() > max_fill_factor) {
        if (!grow())
            break;
    }
    return true;
}

bool raw_hash_table_impl::contains(const byte* key) const {
    if (!key)
        PREQUEL_THROW(bad_argument("Key is null."));

    if (empty())
        return false;

    bucket_node found_node;
    u32 found_index;
    const u64 hash = key_hash(key);
    const bucket_node primary_bucket = read_primary_bucket(bucket_for_hash(hash));
    return find_in_bucket(primary_bucket, key, hash, found_node, found_index);
}

bool raw_hash_table_impl::find(const byte* key, byte* value) const {
    if (!key)
        PREQUEL_THROW(bad_argument("Key is null."));
    if (!value)
        PREQUEL_THROW(bad_argument("Value is null."));

    return find_impl(key, [&](const byte* k) { return key_hash(k); },
                     [&](const byte* left, const byte* right) { return key_equal(left, right); },
                     value);
}

bool raw_hash_table_impl::find_compatible(
    const void* compatible_key, const std::function<u64(const void*)>& compatible_hash,
    const std::function<bool(const void*, const byte*)>& compatible_equals, byte* value) const {
    if (!compatible_key)
        PREQUEL_THROW(bad_argument("Key is null."));
    if (!compatible_hash)
        PREQUEL_THROW(bad_argument("Hash function is null."));
    if (!compatible_equals)
        PREQUEL_THROW(bad_argument("Equality function is null."));
    if (!value)
        PREQUEL_THROW(bad_argument("Value is null."));

    return find_impl(compatible_key_t{compatible_key},
                     [&](const compatible_key_t ckey) { return compatible_hash(ckey.data); },
                     [&](const compatible_key_t left, const byte* right) {
                         return compatible_equals(left.data, right);
                     },
                     value);
}

bool raw_hash_table_impl::erase(const byte* key) {
    if (!key)
        PREQUEL_THROW(bad_argument("Key is null."));

    return erase_impl(key, [&](const byte* k) { return key_hash(k); },
                      [&](const byte* left, const byte* right) { return key_equal(left, right); });
}

bool raw_hash_table_impl::erase_compatible(
    const void* compatible_key, const std::function<u64(const void*)>& compatible_hash,
    const std::function<bool(const void*, const byte*)>& compatible_equals) {
    if (!compatible_key)
        PREQUEL_THROW(bad_argument("Key is null."));
    if (!compatible_hash)
        PREQUEL_THROW(bad_argument("Hash function is null."));
    if (!compatible_equals)
        PREQUEL_THROW(bad_argument("Equality function is null."));

    return erase_impl(compatible_key_t{compatible_key},
                      [&](const compatible_key_t ckey) { return compatible_hash(ckey.data); },
                      [&](const compatible_key_t left, const byte* right) {
                          return compatible_equals(left.data, right);
                      });
}

void raw_hash_table_impl::iterate(iteration_control (*iter_func)(const byte* value, void* user_data),
                                  void* user_data) const {
    if (!iter_func)
        PREQUEL_THROW(bad_argument("Iteration function is null."));

    // Visit every bucket.
    const u64 primary_buckets = get_primary_buckets();
    for (u64 bucket_index = 0; bucket_index < primary_buckets; ++bucket_index) {
        bucket_node bucket = read_primary_bucket(bucket_index);

        // Iterate over the bucket and all its overflow buckets.
        while (1) {
            const u32 values = bucket.get_size();
            for (u32 value_index = 0; value_index < values; ++value_index) {
                iteration_control control = iter_func(bucket.get_value(value_index), user_data);

                switch (control) {
                case iteration_control::next: break;
                case iteration_control::stop: goto end;
                }
            }

            block_index next = bucket.get_next();
            if (!next)
                break;

            bucket = read_bucket(next);
        }
    }

end:
    return;
}

void raw_hash_table_impl::visit(
    const std::function<iteration_control(const raw_hash_table::node_view& node)>& visit_func) const {
    if (!visit_func)
        PREQUEL_THROW(bad_argument("Invalid visitation function."));

    // Visit every bucket and all nodes within them.
    const u64 primary_buckets = get_primary_buckets();

    raw_hash_table_node_view_impl view;
    for (u64 bucket_index = 0; bucket_index < primary_buckets; ++bucket_index) {
        bucket_node bucket = read_primary_bucket(bucket_index);
        bool is_overflow = false;

        // Iterate over the bucket and all its overflow buckets.
        while (1) {
            view.set_node(bucket_index, is_overflow, bucket);
            iteration_control control = visit_func(raw_hash_table::node_view(&view));
            switch (control) {
            case iteration_control::next: break;
            case iteration_control::stop: goto end;
            }

            block_index next = bucket.get_next();
            if (!next)
                break;

            bucket = read_bucket(next);
            is_overflow = true;
        }
    }

end:
    return;
}

void raw_hash_table_impl::dump(std::ostream& os) const {
    fmt::print(os,
               "Raw hash table:\n"
               "  Value size:       {}\n"
               "  Key size:         {}\n"
               "  Block size:       {}\n"
               "  Bucket capacity:  {}\n"
               "  Size:             {}\n"
               "  Primary buckets:  {}\n"
               "  Overflow buckets: {}\n"
               "  Split pointer:    {}\n"
               "  Level:            {}\n"
               "  Load:             {}\n",
               value_size(), key_size(), get_engine().block_size(), bucket_capacity(), size(),
               primary_buckets(), overflow_buckets(), get_step(), get_level(), load());

    if (!m_bucket_ranges.empty()) {
        fmt::print(os, "\n");
        fmt::print(os, "Allocated primary bucket ranges (total buckets: {}):\n",
                   allocated_primary_buckets());
        for (u64 i = 0; i < m_bucket_ranges.size(); ++i) {
            block_index block = m_bucket_ranges.get(i);
            u64 size = bucket_range_size(i);

            fmt::print(os, "  {:>3}: Start @{}, Size {}\n", i, block, size);
        }
    }

    if (!empty()) {
        fmt::print(os, "\n");
    }

    auto print_node = [&](const bucket_node& node, u64 bucket_index, int overflow) {
        if (overflow == 0) {
            fmt::print(os,
                       "  Bucket {} @{}:\n"
                       "    Next: @{}\n"
                       "    Size: {}\n",
                       bucket_index, node.index(), node.get_next(), node.get_size());
        } else {
            fmt::print(os,
                       "  Bucket {} (Overflow {}) @{}:\n"
                       "    Next: @{}\n"
                       "    Size: {}\n",
                       bucket_index, overflow, node.index(), node.get_next(), node.get_size());
        }

        const u32 size = node.get_size();
        for (u32 i = 0; i < size; ++i) {
            const byte* value = node.get_value(i);
            fmt::print(os, "    {:>4}: {} (Hash: {})\n", i, format_hex(value, value_size()),
                       value_hash(value));
        }
        fmt::print(os, "\n");
    };

    const u64 total_primary_buckets = primary_buckets();
    for (u64 bucket_index = 0; bucket_index < total_primary_buckets; ++bucket_index) {
        bucket_node node = read_primary_bucket(bucket_index);
        print_node(node, bucket_index, 0);

        block_index next = node.get_next();
        int overflow = 1;
        while (next) {
            node = read_bucket(next);
            print_node(node, bucket_index, overflow);

            overflow += 1;
            next = node.get_next();
        }
    }
}

void raw_hash_table_impl::validate() const {
#define PREQUEL_ERROR(...) PREQUEL_THROW(corruption_error(fmt::format("validate: " __VA_ARGS__)));

    if (get_step() > (u64(1) << get_level()))
        PREQUEL_ERROR("Step pointer must be lesser than or equal to scale.");
    if (get_primary_buckets() > allocated_primary_buckets())
        PREQUEL_ERROR("More primary buckets than we have allocated.");

    u64 seen_values = 0;
    u64 seen_overflow_buckets = 0;

    // Validate and count the contents of all primary buckets and their
    // overflow lists.
    const u64 primary_buckets = get_primary_buckets();
    for (u64 bucket_index = 0; bucket_index < primary_buckets; ++bucket_index) {
        bucket_node bucket = read_primary_bucket(bucket_index);
        bool is_overflow = false;

        while (1) {
            const u32 values = bucket.get_size();

            const byte* last_value = nullptr;
            for (u32 value_index = 0; value_index < values; ++value_index) {
                const byte* value = bucket.get_value(value_index);
                if (bucket_for_value(value) != bucket_index)
                    PREQUEL_ERROR("Value is in wrong bucket.");

                if (last_value) {
                    u64 last_hash = value_hash(last_value);
                    u64 hash = value_hash(value);
                    if (hash < last_hash) {
                        PREQUEL_ERROR("Values in a node must be sorted.");
                    }
                }
                last_value = value;
            }

            seen_values += values;
            if (is_overflow)
                seen_overflow_buckets += 1;

            block_index next = bucket.get_next();
            if (!next)
                break;

            bucket = read_bucket(next);
            is_overflow = true;
        }
    }

    if (seen_values != get_size())
        PREQUEL_ERROR("Inconsistent value count.");
    if (seen_overflow_buckets != get_overflow_buckets())
        PREQUEL_ERROR("Inconsistent number of overflow buckets.");

#undef PREQUEL_ERROR
}

void raw_hash_table_impl::clear() {
    const u64 primary_buckets = get_primary_buckets();
    if (primary_buckets == 0)
        return;

    for (u64 bucket_index = primary_buckets; bucket_index-- > 0;) {
        bucket_node primary = read_primary_bucket(bucket_index);
        free_overflow_chain(primary.get_next());
        free_primary_bucket(bucket_index);
    }

    PREQUEL_ASSERT(allocated_buckets() == 0, "Must have freed all buckets.");
    PREQUEL_ASSERT(byte_size() == 0, "Must occupy 0 bytes on disk.");

    set_step(0);
    set_size(0);
    set_level(0);
}

template<typename KeyType, typename KeyHasher, typename KeyEquals>
bool raw_hash_table_impl::find_impl(const KeyType& key, const KeyHasher& hasher,
                                    const KeyEquals& equals, byte* value) const {
    PREQUEL_ASSERT(value, "Value must not be null.");

    if (empty())
        return false;

    const u64 hash = hasher(key);
    const bucket_node primary_bucket = read_primary_bucket(bucket_for_hash(hash));
    bucket_node found_node;
    u32 found_index = 0;
    if (find_in_bucket(primary_bucket, key, hash, equals, found_node, found_index)) {
        std::memmove(value, found_node.get_value(found_index), value_size());
        return true;
    }
    return false;
}

template<typename KeyType, typename KeyHasher, typename KeyEquals>
bool raw_hash_table_impl::erase_impl(const KeyType& key, const KeyHasher& hasher,
                                     const KeyEquals& equals) {
    if (empty())
        return false;

    bucket_node found_node;
    u32 found_index = 0;
    const u64 hash = hasher(key);
    const bucket_node primary_bucket = read_primary_bucket(bucket_for_hash(hash));
    if (!find_in_bucket(primary_bucket, key, hash, equals, found_node, found_index)) {
        return false;
    }

    found_node.remove(found_index);
    set_size(get_size() - 1);

    if (empty()) {
        PREQUEL_ASSERT(get_size() == 0, "Empty hash tables have no elements.");
        PREQUEL_ASSERT(get_level() == 0, "Empty hash tables have 0 level.");
        PREQUEL_ASSERT(get_step() == 0, "Empty hash table cannot have nonzero step pointers.");
        free_overflow_chain(primary_bucket.get_next());
        free_primary_bucket(0);
    } else {
        while (load() < min_fill_factor) {
            if (!shrink())
                break;
        }
    }

    return true;
}

bool raw_hash_table_impl::insert_into_bucket(const bucket_node& primary_bucket, const byte* value,
                                             const byte* key, u64 hash, bucket_node& found_node,
                                             u32& found_index) {
    PREQUEL_ASSERT(primary_bucket, "Invalid primary bucket.");
    PREQUEL_ASSERT(value, "Value is null.");
    PREQUEL_ASSERT(key, "Key is null.");

    auto key_equals = [&](const byte* lhs, const byte* rhs) { return key_equal(lhs, rhs); };

    // We cache the first node that has enough space for the new value,
    // even if we did not check all other nodes yet.
    bool cached_insert_location = false;
    bucket_node insert_node;
    u32 insert_index = u32(-1);

    // Iterate over all nodes in the bucket.
    bucket_node node = primary_bucket;
    while (1) {
        u32 position = 0;
        if (find_in_node(node, key, hash, key_equals, position)) {
            // Key exists at `position`.
            found_node = node;
            found_index = position;
            return false;
        }

        // We can insert at `position` - if the node is not full.
        // We have to check all other nodes first though, in case the key exists.
        if (!node.full()) {
            insert_node = node;
            insert_index = position;
            cached_insert_location = true;
        }

        block_index next = node.get_next();
        if (!next)
            break;

        node = read_bucket(next);
    }

    // If we did not find a suitable insert location on the way,
    // allocate a new overflow node and link it into the chain.
    // `node` points to the current last node in the overflow chain.
    if (!cached_insert_location) {
        insert_node = allocate_overflow_bucket();
        insert_index = 0;
        node.set_next(insert_node.index());
    }

    insert_node.insert(insert_index, value);
    return true;
}

/*
 * FIXME ineffecient because the calling code knows that the key is unique.
 * Grow() and shink() need to be improved.
 */
void raw_hash_table_impl::insert_into_bucket(const bucket_node& primary_bucket, const byte* value) {
    PREQUEL_ASSERT(primary_bucket, "Invalid primary bucket.");
    PREQUEL_ASSERT(value, "Value is null.");

    key_buffer key;
    derive_key(value, key.data());

    u64 hash = key_hash(key.data());

    bucket_node found_node;
    u32 found_index = 0;
    bool inserted =
        insert_into_bucket(primary_bucket, value, key.data(), hash, found_node, found_index);
    PREQUEL_ASSERT(inserted, "Key must be unique.");
    unused(inserted);
}

bool raw_hash_table_impl::find_in_bucket(const bucket_node& primary_bucket, const byte* search_key,
                                         u64 search_hash, bucket_node& found_node,
                                         u32& found_index) const {
    return find_in_bucket(primary_bucket, search_key, search_hash,
                          [&](const byte* left, const byte* right) { return key_equal(left, right); },
                          found_node, found_index);
}

template<typename KeyType, typename KeyEquals>
bool raw_hash_table_impl::find_in_node(const bucket_node& node, const KeyType& search_key,
                                       u64 search_hash, const KeyEquals& equals,
                                       u32& position) const {
    PREQUEL_ASSERT(node, "Invalid node.");

    const u32 size = node.get_size();
    key_buffer other_key;

    /*
     * Binary search. Entries are sorted by hash.
     */
    auto iter = std::lower_bound(identity_iterator<u32>(0), identity_iterator<u32>(size),
                                 search_hash, [&](u32 index, u64 hash) {
                                     const byte* value = node.get_value(index);

                                     derive_key(value, other_key.data());
                                     const u64 other_hash = key_hash(other_key.data());
                                     return other_hash < hash;
                                 });

    /*
     * Iterate starting from the found position. Continue the search
     * as long as we see the search hash (to handle collisions).
     * Terminate when we reached the end (also returned on unsuccessful hash search above)
     * or if we found the value.
     */
    u32 index = *iter;
    while (index != size) {
        const byte* value = node.get_value(index);
        derive_key(value, other_key.data());

        const u64 hash = key_hash(other_key.data());
        if (hash != search_hash) {
            PREQUEL_ASSERT(hash > search_hash, "Order invariant.");
            position = index;
            return false;
        }

        if (equals(search_key, other_key.data())) {
            position = index;
            return true;
        }

        ++index;
    }

    position = index;
    return false;
}

template<typename KeyType, typename KeyEquals>
bool raw_hash_table_impl::find_in_bucket(const bucket_node& primary_bucket,
                                         const KeyType& search_key, u64 search_hash,
                                         const KeyEquals& equals, bucket_node& found_node,
                                         u32& found_index) const {
    PREQUEL_ASSERT(primary_bucket, "Invalid primary bucket.");

    bucket_node bucket = primary_bucket;
    while (1) {
        u32 index = 0;
        if (find_in_node(bucket, search_key, search_hash, equals, index)) {
            found_node = std::move(bucket);
            found_index = index;
            return true;
        }

        block_index next = bucket.get_next();
        if (!next)
            break;
        bucket = read_bucket(next);
    }

    return false;
}

bool raw_hash_table_impl::grow() {
    u64 step = get_step();
    u8 level = get_level();
    u64 scale = u64(1) << level;
    PREQUEL_ASSERT(step <= scale, "Invalid step pointer.");

    if (step == scale) {
        PREQUEL_ASSERT(get_primary_buckets() == scale * 2,
                       "Growth for this level must be complete.");
        if (level >= 63)
            return false;

        scale *= 2;
        level += 1;
        step = 0;
        set_step(step);
        set_level(level);
    }

    /*
     * Split the bucket pointed to by the step pointer and redistribute
     * its values.
     *
     * TODO: Make this better, see below.
     *
     * The current approach is neither efficient nor very sound, because
     * the values are removed first (a destructive operation) and then re-inserted.
     * If any insertion fails (e.g. because no space can be allocated for overflow buckets),
     * then all values will be lost.
     *
     * Also, all values will be distributed between the old and the new bucket, so running
     * the entire insertion algorithm again every time is wasteful.
     *
     * Hint for the future: since we allocate one more primarry bucket, that bucket and the existing
     * oveflow buckets should be enough to hold the two split bucket chains.
     */

    // Extract all values from the existing bucket and empty it.
    std::vector<byte> split_values;
    {
        bucket_node current = read_primary_bucket(step);
        allocate_primary_bucket(scale + step);

        bool current_is_overflow = false;
        while (1) {
            const u32 size = current.get_size();
            split_values.reserve(split_values.size() + size * value_size());

            for (u32 i = 0; i < size; ++i) {
                const byte* value = current.get_value(i);
                split_values.insert(split_values.end(), value, value + value_size());
            }

            // Free or reset the current bucket. Overflow buckets are freed,
            // the primary bucket stays around.
            const block_index next = current.get_next();
            if (current_is_overflow) {
                free_overflow_bucket(current.index());
            } else {
                current.set_next(block_index());
                current.set_size(0);
            }

            if (!next) {
                break;
            }

            current = read_bucket(next);
            current_is_overflow = true;
        }
    }

    set_step(++step);

    for (size_t i = 0; i < split_values.size(); i += value_size()) {
        const byte* value = split_values.data() + i;

        // Note that this will always be either the bucket we are splitting
        // or the bucket we have just allocated.
        const u64 bucket_index = bucket_for_value(value);
        insert_into_bucket(read_primary_bucket(bucket_index), value);
    }
    return true;
}

bool raw_hash_table_impl::shrink() {
    /*
     * This function will delete the bucket BEFORE the current step pointer
     * (the pointer always points to the next bucket to be split).
     */
    u64 step = get_step();
    u8 level = get_level();

    u64 scale = u64(1) << level;
    PREQUEL_ASSERT(step <= scale, "Invalid step pointer.");

    if (step == 0) {
        if (level == 0)
            return false; // Cannot shrink anymore.

        PREQUEL_ASSERT(get_primary_buckets() == scale,
                       "Shrink operation for this level must be complete.");
        scale >>= 1;
        level -= 1;
        step = scale;
        set_step(step);
        set_level(level);
    }

    // TODO: See comment in grow(), the same applies here.
    // We already know that all values in the merged bucket will
    // end up in step - 1.

    // Extract values from the bucket; then delete it.
    std::vector<byte> merge_values;
    {
        const u64 bucket_index = get_primary_buckets() - 1;
        bucket_node current = read_primary_bucket(bucket_index);
        bool current_is_overflow = false;

        while (1) {
            const u32 size = current.get_size();
            merge_values.reserve(merge_values.size() + size * value_size());

            for (u32 i = 0; i < size; ++i) {
                const byte* value = current.get_value(i);
                merge_values.insert(merge_values.end(), value, value + value_size());
            }

            // Free the bucket appropriately.
            const block_index next = current.get_next();
            if (current_is_overflow) {
                free_overflow_bucket(current.index());
            } else {
                free_primary_bucket(bucket_index);
            }

            if (!next) {
                break;
            }

            current = read_bucket(next);
            current_is_overflow = true;
        }
    }

    set_step(--step);

    for (size_t i = 0; i < merge_values.size(); i += value_size()) {
        const byte* value = merge_values.data() + i;
        const u64 bucket_index = bucket_for_value(value);
        PREQUEL_ASSERT(bucket_index == step, "Invariant");
        insert_into_bucket(read_primary_bucket(bucket_index), value);
    }
    return true;
}

void raw_hash_table_impl::free_overflow_chain(block_index overflow) {
    while (overflow) {
        bucket_node node = read_bucket(overflow);
        block_index next = node.get_next();
        free_overflow_bucket(overflow);
        overflow = next;
    }
}

block_index raw_hash_table_impl::bucket_address(u64 bucket_index) const {
    u64 bucket_range_index;
    u64 bucket_range_offset;
    std::tie(bucket_range_index, bucket_range_offset) = find_bucket_position(bucket_index);
    PREQUEL_ASSERT(bucket_range_index < m_bucket_ranges.size(),
                   "Bucket range index out of bounds.");

    return m_bucket_ranges.get(bucket_range_index) + bucket_range_offset;
}

bucket_node raw_hash_table_impl::allocate_primary_bucket(u64 index) {
    PREQUEL_ASSERT(index == get_primary_buckets(), "Primary buckets are allocated sequentially.");

    // Allocate a new bucket range if necessary.
    if (index >= allocated_primary_buckets()) {
        m_bucket_ranges.reserve_additional(1);

        u64 range_size = bucket_range_size(m_bucket_ranges.size());
        block_index range_address = get_allocator().allocate(range_size);
        m_bucket_ranges.push_back(range_address);
    }

    PREQUEL_ASSERT(index < allocated_primary_buckets(), "Not enough buckets for that index.");
    block_index bucket_ptr = bucket_address(index);
    block_handle handle = get_engine().overwrite_zero(bucket_ptr);
    bucket_node node(std::move(handle), value_size(), m_bucket_capacity);
    node.init();

    set_primary_buckets(index + 1);
    return node;
}

void raw_hash_table_impl::free_primary_bucket(u64 index) {
    PREQUEL_ASSERT(get_primary_buckets() > 0, "There are no primary buckets to free.");
    PREQUEL_ASSERT(index == get_primary_buckets() - 1,
                   "Primary buckets are freed in reverse order.");

    set_primary_buckets(index);

    PREQUEL_ASSERT(m_bucket_ranges.size() > 0, "No allocated ranges.");
    while (!m_bucket_ranges.empty() && total_bucket_count(m_bucket_ranges.size() - 1) >= index) {
        u64 range_size = bucket_range_size(m_bucket_ranges.size() - 1);
        block_index range_address = m_bucket_ranges[m_bucket_ranges.size() - 1];
        get_allocator().free(range_address, range_size);
        m_bucket_ranges.pop_back();
        m_bucket_ranges.shrink_to_fit();
    }
}

bucket_node raw_hash_table_impl::allocate_overflow_bucket() {
    block_index bucket_ptr = get_allocator().allocate(1);
    block_handle handle = get_engine().overwrite_zero(bucket_ptr);
    bucket_node node(std::move(handle), value_size(), m_bucket_capacity);
    node.init();

    set_overflow_buckets(get_overflow_buckets() + 1);
    return node;
}

void raw_hash_table_impl::free_overflow_bucket(block_index bucket_ptr) {
    PREQUEL_ASSERT(get_overflow_buckets() > 0, "Invalid state.");
    get_allocator().free(bucket_ptr, 1);
    set_overflow_buckets(get_overflow_buckets() - 1);
}

bucket_node raw_hash_table_impl::read_primary_bucket(u64 bucket_index) const {
    PREQUEL_ASSERT(bucket_index < get_primary_buckets(), "Bucket index out of range.");
    return read_bucket(bucket_address(bucket_index));
}

bucket_node raw_hash_table_impl::read_bucket(block_index bucket_ptr) const {
    return bucket_node(get_engine().read(bucket_ptr), value_size(), m_bucket_capacity);
}

double raw_hash_table_impl::load() const {
    return empty() ? 0.0 : double(size()) / double(get_primary_buckets() * m_bucket_capacity);
}

u64 raw_hash_table_impl::bucket_for_value(const byte* value) const {
    key_buffer buffer;
    derive_key(value, buffer.data());
    return bucket_for_key(buffer.data());
}

u64 raw_hash_table_impl::bucket_for_key(const byte* key) const {
    return bucket_for_hash(key_hash(key));
}

u64 raw_hash_table_impl::bucket_for_hash(u64 hash) const {
    PREQUEL_ASSERT(get_primary_buckets() > 0, "Must have at least one bucket.");

    const u8 level = get_level();
    u64 index = hash & ((u64(1) << level) - 1);
    if (index < get_step()) {
        index = hash & ((u64(1) << (level + 1)) - 1);
    }

    PREQUEL_ASSERT(index < get_primary_buckets(), "Bucket index out of range.");
    return index;
}

u64 raw_hash_table_impl::value_hash(const byte* value) const {
    PREQUEL_ASSERT(value, "Value is null.");
    key_buffer key;
    derive_key(value, key.data());
    return key_hash(key.data());
}

u64 raw_hash_table_impl::key_hash(const byte* key) const {
    PREQUEL_ASSERT(key, "Null key.");
    return m_options.key_hash(key, m_options.user_data);
}

bool raw_hash_table_impl::key_equal(const byte* left_key, const byte* right_key) const {
    PREQUEL_ASSERT(left_key, "Null left key.");
    PREQUEL_ASSERT(right_key, "Null right key.");
    return m_options.key_equal(left_key, right_key, m_options.user_data);
}

void raw_hash_table_impl::derive_key(const byte* value, byte* key) const {
    PREQUEL_ASSERT(value, "Null value.");
    PREQUEL_ASSERT(key, "Null key.");
    m_options.derive_key(value, key, m_options.user_data);
}

} // namespace detail

// --------------------------------
//
//   Hash table public interface
//
// --------------------------------

raw_hash_table::raw_hash_table(anchor_handle<anchor> anchor_,
                               const raw_hash_table_options& options_, allocator& alloc_)
    : m_impl(std::make_unique<detail::raw_hash_table_impl>(std::move(anchor_), options_, alloc_)) {}

raw_hash_table::~raw_hash_table() {}

raw_hash_table::raw_hash_table(raw_hash_table&& other) noexcept
    : m_impl(std::move(other.m_impl)) {}

raw_hash_table& raw_hash_table::operator=(raw_hash_table&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

engine& raw_hash_table::get_engine() const {
    return impl().get_engine();
}
allocator& raw_hash_table::get_allocator() const {
    return impl().get_allocator();
}

u32 raw_hash_table::value_size() const {
    return impl().value_size();
}
u32 raw_hash_table::key_size() const {
    return impl().key_size();
}
u32 raw_hash_table::bucket_capacity() const {
    return impl().bucket_capacity();
}

bool raw_hash_table::empty() const {
    return impl().empty();
}
u64 raw_hash_table::size() const {
    return impl().size();
}

u64 raw_hash_table::primary_buckets() const {
    return impl().primary_buckets();
}
u64 raw_hash_table::overflow_buckets() const {
    return impl().overflow_buckets();
}
u64 raw_hash_table::allocated_buckets() const {
    return impl().allocated_buckets();
}

double raw_hash_table::fill_factor() const {
    return impl().load();
}
u64 raw_hash_table::byte_size() const {
    return impl().byte_size();
}

double raw_hash_table::overhead() const {
    return empty() ? 1.0 : double(byte_size()) / (size() * value_size());
}

bool raw_hash_table::contains(const byte* key) const {
    return impl().contains(key);
}

bool raw_hash_table::find(const byte* key, byte* value) const {
    return impl().find(key, value);
}

bool raw_hash_table::find_compatible(
    const void* compatible_key, const std::function<u64(const void*)>& compatible_hash,
    const std::function<bool(const void*, const byte*)>& compatible_equals, byte* value) const {
    return impl().find_compatible(compatible_key, compatible_hash, compatible_equals, value);
}

bool raw_hash_table::insert(const byte* value) {
    return impl().insert(value, false);
}
bool raw_hash_table::insert_or_update(const byte* value) {
    return impl().insert(value, true);
}

bool raw_hash_table::erase(const byte* key) {
    return impl().erase(key);
}

bool raw_hash_table::erase_compatible(
    const void* compatible_key, const std::function<u64(const void*)>& compatible_hash,
    const std::function<bool(const void*, const byte*)>& compatible_equals) const {
    return impl().erase_compatible(compatible_key, compatible_hash, compatible_equals);
}

void raw_hash_table::clear() {
    impl().clear();
}
void raw_hash_table::reset() {
    impl().clear();
}

void raw_hash_table::iterate(iteration_control (*iter_func)(const byte* value, void* user_data),
                             void* user_data) const {
    impl().iterate(iter_func, user_data);
}

void raw_hash_table::dump(std::ostream& os) const {
    impl().dump(os);
}
void raw_hash_table::validate() const {
    impl().validate();
}

void raw_hash_table::visit(const std::function<iteration_control(const node_view&)>& iter_func) const {
    impl().visit(iter_func);
}

detail::raw_hash_table_impl& raw_hash_table::impl() const {
    if (!m_impl)
        PREQUEL_THROW(bad_operation("Invalid hash table instance."));
    return *m_impl;
}

// --------------------------------
//
//   Hash table node view public interface
//
// --------------------------------

raw_hash_table::node_view::node_view(detail::raw_hash_table_node_view_impl* impl)
    : m_impl(impl) {
    PREQUEL_ASSERT(m_impl, "Invalid implementation pointer.");
}

bool raw_hash_table::node_view::is_primary() const {
    return m_impl->is_primary();
}
bool raw_hash_table::node_view::is_overflow() const {
    return m_impl->is_overflow();
}

u64 raw_hash_table::node_view::bucket_index() const {
    return m_impl->bucket_index();
}
block_index raw_hash_table::node_view::address() const {
    return m_impl->address();
}
block_index raw_hash_table::node_view::overflow_address() const {
    return m_impl->overflow_address();
}

u32 raw_hash_table::node_view::size() const {
    return m_impl->size();
}
const byte* raw_hash_table::node_view::value(u32 index) const {
    return m_impl->value(index);
}

} // namespace prequel
