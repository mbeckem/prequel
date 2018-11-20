#ifndef PREQUEL_RAW_HASH_TABLE_HPP
#define PREQUEL_RAW_HASH_TABLE_HPP

#include <prequel/allocator.hpp>
#include <prequel/anchor_handle.hpp>
#include <prequel/array.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/iteration.hpp>

#include <functional>

namespace prequel {

namespace detail {

class raw_hash_table_impl;
class raw_hash_table_node_view_impl;

class raw_hash_table_anchor {
    // Number of entries.
    u64 size = 0;

    // Number of primary buckets in use.
    u64 primary_buckets = 0;

    // Number of overflow buckets in use.
    u64 overflow_buckets = 0;

    // The index of the bucket that is will be rehashed next.
    u64 step = 0;

    // The growth exponent. The table is currently scheduled to grow to
    // the size of 2^level buckets. When the step pointer has reached that
    // value, the level will be incremented.
    u8 level = 0;

    // Contains pointers to allocated chunks, which in turn are used for buckets.
    //
    // TODO: Make the anchor table more compact, e.g. by using small buffer optimization
    // for this table.
    // TODO: Store prefix sums and sizes here as well (not have them precompiled in the binary).
    array<block_index>::anchor bucket_ranges;

    static constexpr auto get_binary_format() {
        return make_binary_format(
            &raw_hash_table_anchor::step, &raw_hash_table_anchor::size,
            &raw_hash_table_anchor::primary_buckets, &raw_hash_table_anchor::overflow_buckets,
            &raw_hash_table_anchor::level, &raw_hash_table_anchor::bucket_ranges);
    }

    friend binary_format_access;
    friend raw_hash_table_impl;
};

} // namespace detail

/// A group of properties required to configure a hash table instance.
/// The parameters must be semantically equivalent whenever the
/// hash table is (re-) opened.
struct raw_hash_table_options {
    /// The size of a value, in bytes. Must be > 0.
    u32 value_size = 0;

    /// Size of a key, in bytes. Keys are derived from values.
    /// Must be > 0.
    u32 key_size = 0;

    /// Passed to all callbacks as the last argument.
    /// Can remain null.
    void* user_data = nullptr;

    /// Takes a value (`value_size` readable bytes) and derives a search
    /// key from it. The key must be stored in `key_buffer` and must
    /// be exactly `key_size` bytes long.
    void (*derive_key)(const byte* value, byte* key, void* user_data) = nullptr;

    /// Returns a hash value for the given `key` (`key_size` readable bytes).
    /// Equal keys *must* have equal hash values, the reverse need not be true.
    /// Hash values should have reasonably uniform distribution in order to prevent skewed tables.
    u64 (*key_hash)(const byte* key, void* user_data) = nullptr;

    /// Takes two keys (`key_size` readable bytes each) and returns true iff both are equal.
    /// Two equal keys *must* have the same hash value.
    bool (*key_equal)(const byte* left_key, const byte* right_key, void* user_data) = nullptr;
};

/**
 * \note the hash table will use the k lowest bits in order to determine
 * the hash bucket for a given key (with k getting larger as the table grows).
 * It is therefore crucial that those bits are somewhat uniformly distributed.
 *
 * The FNV-1A hash function has shown good results even for integer keys (it is the default
 * for hash_table<..:>).
 */
class raw_hash_table {
public:
    using anchor = detail::raw_hash_table_anchor;

public:
    raw_hash_table(anchor_handle<anchor> anc, const raw_hash_table_options& options,
                   allocator& alloc);
    ~raw_hash_table();

    raw_hash_table(raw_hash_table&& other) noexcept;
    raw_hash_table& operator=(raw_hash_table&& other) noexcept;

    engine& get_engine() const;
    allocator& get_allocator() const;

    /// Returns the size (in bytes) of every value in the table.
    u32 value_size() const;

    /// Returns the size (in bytes) of every key in the table.
    u32 key_size() const;

    /// Returns the number of values that can fit into a table bucket (primary or overflow).
    u32 bucket_capacity() const;

    /// Returns true iff the table is empty.
    bool empty() const;

    /// Returns the number of values in this table.
    u64 size() const;

    /// Returns the number of primary buckets currently in use.
    u64 primary_buckets() const;

    /// Returns the number of overflow buckets currently in use.
    u64 overflow_buckets() const;

    /// Returns the total number of allocated buckets.
    /// This is *not* the sum of `primary_buckets()` and `overflow_buckets()` because
    /// the table preallocated storage in larger chunks.
    u64 allocated_buckets() const;

    /// Returns the average fill factor of this table's primary buckets.
    double fill_factor() const;

    /// Returns the total size of this datastructure on disk, in bytes.
    u64 byte_size() const;

    /// Returns the relative overhead of this table compared to a linear file with the same values.
    /// Computed by dividing the total size of the table by the total size of its values.
    double overhead() const;

    /// Returns true if the table contains the given key (of size `key_size()`).
    bool contains(const byte* key) const;

    /// Attempts to find the value associated with the given key
    /// and stores it in the provided `value` buffer, which must have `value_size()`
    /// writable bytes.
    /// Returns true if the value was found.
    ///
    /// TODO Output parameter unnecessary once we have cursors.
    bool find(const byte* key, byte* value) const;

    /// Attempts to find the value associated with the given compatible key and stores
    /// it in the provided `value` buffer, which must have `value_size()` writable bytes.
    ///
    /// `compatible_hash` takes the `compatible_key` pointer and returns an appropriate
    /// hash value for this table.
    /// `compatible_equals` will be invoked with the compatible key (first argument)
    /// and keys from the hash table (of size `key_size()`) and shall return true iff
    /// the two keys compare equal.
    ///
    /// Returns true if the value was found.
    ///
    /// TODO Output parameter unnecessary once we have cursors.
    bool find_compatible(const void* compatible_key,
                         const std::function<u64(const void*)>& compatible_hash,
                         const std::function<bool(const void*, const byte*)>& compatible_equals,
                         byte* value) const;

    /// Attempts to insert the given value (of size `value_size()`) into the table.
    /// Does nothing if a value with the same key already exists.
    /// Returns true if the value was inserted.
    bool insert(const byte* value);

    /// Inserts the value (of size `value_size()`) into the table. Overwrites any
    /// existing value with the same key.
    /// Returns true if the value was inserted, false if an old value was overwritten.
    bool insert_or_update(const byte* value);

    /// Removes the value associated with the given key (of size `key_size()`) from the table.
    /// Returns true if a value existed.
    /// TODO: erase + retrieve?
    bool erase(const byte* key);

    /// Like erase() and find_compatible().
    /// TODO: This would eventually go away when we have cursors here,
    /// find_compatible would return a cursor and one could call erase there..
    bool
    erase_compatible(const void* compatible_key,
                     const std::function<u64(const void*)>& compatible_hash,
                     const std::function<bool(const void*, const byte*)>& compatible_equals) const;

    /// Iterates over the values of this table.
    /// The `iter_func` function will be called for every value of the table,
    /// until it returns `iteration::stop`.
    ///
    /// The `user_data` pointer will be passed to the iteration function on every invocation.
    ///
    /// \warning The table must not be modified while iteration is in progress.
    void iterate(iteration_control (*iter_func)(const byte* value, void* user_data),
                 void* user_data = nullptr) const;

    template<typename IterFunc>
    void iterate(IterFunc&& fn) const {
        auto callback = [](const byte* value, void* user_data) -> iteration_control {
            IterFunc& fn = *reinterpret_cast<IterFunc*>(user_data);
            return fn(value);
        };
        iterate(+callback, reinterpret_cast<void*>(std::addressof(fn)));
    }

    /// Removes all data from this table. After this operation completes, the table
    /// will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

    /// Erases all values from this table.
    /// \post `empty()`.
    ///
    /// TODO: if overflow buckets were linked into a global list,
    /// we would not have to iterate over ever primary bucket in order to free
    /// its overflow list.
    void clear();

    /// Prints debugging information to the output stream.
    void dump(std::ostream& os) const;

    /// Perform internal consistency checks.
    void validate() const;

public:
    /*
     * Node visitation api
     */

    class node_view {
    public:
        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

        bool is_primary() const;
        bool is_overflow() const;

        // The index of the primary bucket that this node belongs to.
        u64 bucket_index() const;

        // The index of this node's block on disk.
        block_index address() const;

        // The index of the next overflow node on disk (if any).
        block_index overflow_address() const;

        // The number of values in this node.
        u32 size() const;

        // The value at this index. Valid indices are `0 <= i < size()`.
        const byte* value(u32 index) const;

    private:
        friend detail::raw_hash_table_impl;

        node_view(detail::raw_hash_table_node_view_impl* impl);

    private:
        detail::raw_hash_table_node_view_impl* m_impl = nullptr;
    };

    /**
     * Visits all nodes in this hash table.
     * Buckets are visited in order (i.e. by their bucket_index) and primary bucket nodes
     * will be visited before the overflow nodes that belong to the same bucket.
     */
    void visit(const std::function<iteration_control(const node_view&)>& iter_func) const;

private:
    detail::raw_hash_table_impl& impl() const;

private:
    std::unique_ptr<detail::raw_hash_table_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_RAW_HASH_TABLE_HPP
