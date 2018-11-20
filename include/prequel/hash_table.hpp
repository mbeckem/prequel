#ifndef PREQUEL_HASH_TABLE_HPP
#define PREQUEL_HASH_TABLE_HPP

#include <prequel/binary_format.hpp>
#include <prequel/identity_key.hpp>
#include <prequel/hash.hpp>
#include <prequel/serialization.hpp>
#include <prequel/raw_hash_table.hpp>

#include <memory>

namespace prequel {

template<
    typename Value,
    typename DeriveKey = identity_t,
    typename KeyHash = fnv_hasher,
    typename KeyEqual = std::equal_to<>
>
class hash_table {
public:
    /// Typedef for the value type.
    using value_type = Value;

    /// Typedef for the key type, which is the result of applying the `DeriveKey`
    /// function on a value.
    using key_type = remove_cvref_t<std::result_of_t<DeriveKey(Value)>>;

public:
    class anchor {
        raw_hash_table::anchor table;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::table);
        }

        friend hash_table;
        friend binary_format_access;
    };

    // TODO Rethink node visitation API, its awkward.
    class node_view {
    public:
        bool is_primary() const { return m_inner.is_primary(); }
        bool is_overflow() const { return m_inner.is_overflow(); }

        u64 bucket_index() const { return m_inner.bucket_index(); }
        block_index address() const { return m_inner.address(); }
        block_index oveflow_address() const { return m_inner.overflow_address(); }

        u32 size() const { return m_inner.size(); }

        value_type value(u32 index) const {
            const byte* data = m_inner.value(index);
            return deserialized_value<value_type>(data);
        }

    private:
        friend hash_table;

        node_view(const raw_hash_table::node_view& inner)
            : m_inner(inner)
        {}

        node_view(const node_view&) = delete;
        node_view& operator==(const node_view&) = delete;

    private:
        const raw_hash_table::node_view& m_inner;
    };

public:
    explicit hash_table(anchor_handle<anchor> anchor_, allocator& alloc_,
                        DeriveKey derive_key = DeriveKey(),
                        KeyHash key_hash = KeyHash(),
                        KeyEqual key_equal = KeyEqual())
        : m_state(std::make_unique<state_t>(
                      std::move(derive_key),
                      std::move(key_hash),
                      std::move(key_equal)))
        , m_inner(std::move(anchor_).template member<&anchor::table>(), make_options(), alloc_)
    {}

    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    /// Returns the size (in bytes) of every value in the table.
    static constexpr u32 value_size() { return serialized_size<value_type>(); }

    /// Returns the size (in bytes) of every key in the table.
    static constexpr u32 key_size() { return serialized_size<key_type>(); }

    /// Returns the number of values that can fit into a table bucket (primary or overflow).
    u32 bucket_capacity() const { return m_inner.bucket_capacity(); }

    /// Returns true iff the table is empty.
    bool empty() const { return m_inner.empty(); }

    /// Returns the number of values in this table.
    u64 size() const { return m_inner.size(); }

    /// Returns the number of primary buckets currently in use.
    u64 primary_buckets() const { return m_inner.primary_buckets(); }

    /// Returns the number of overflow buckets currently in use.
    u64 overflow_buckets() const { return m_inner.overflow_buckets(); }

    /// Returns the total number of allocated buckets.
    /// This is *not* the sum of `primary_buckets()` and `overflow_buckets()` because
    /// the table preallocated storage in larger chunks.
    u64 allocated_buckets() const { return m_inner.allocated_buckets(); }

    /// Returns the average fill factor of this table's primary buckets.
    double fill_factor() const { return m_inner.fill_factor(); }

    /// Returns the total size of this datastructure on disk, in bytes.
    u64 byte_size() const { return m_inner.byte_size(); }

    /// Returns the relative overhead of this table compared to a linear file with the same values.
    /// Computed by dividing the total size of the table by the total size of its values.
    double overhead() const { return m_inner.overhead(); }

    /// Returns true if the table contains the given key.
    bool contains(const key_type& key) const {
        serialized_buffer<key_type> buffer = serialized_value(key);
        return m_inner.contains(buffer.data());
    }

    /// Attempts to find the value associated with the given key and writes it to `value`
    /// on success. Returns true if the value was found.
    bool find(const key_type& key, value_type& value) const {
        serialized_buffer<key_type> key_buffer = serialized_value(key);
        serialized_buffer<value_type> value_buffer;
        if (m_inner.find(key_buffer.data(), value_buffer.data())) {
            value = deserialized_value<value_type>(value_buffer.data());
            return true;
        }
        return false;
    }

    template<typename CompatibleKeyType, typename CompatibleKeyHash, typename CompatibleKeyEquals>
    bool find_compatible(const CompatibleKeyType& key,
                         const CompatibleKeyHash& hash,
                         const CompatibleKeyEquals& equals,
                         value_type& value) const
    {
        serialized_buffer<value_type> value_buffer;

        auto wrapped_hasher = wrap_hash<CompatibleKeyType>(hash);
        auto wrapped_equals = wrap_equals<CompatibleKeyType>(equals);
        if (m_inner.find_compatible(std::addressof(key), std::ref(wrapped_hasher), std::ref(wrapped_equals), value_buffer.data())) {
            value = deserialized_value<value_type>(value_buffer.data());
            return true;
        }
        return false;
    }

    /// Attempts to insert the given value (of size `value_size()`) into the table.
    /// Does nothing if a value with the same key already exists.
    /// Returns true if the value was inserted.
    bool insert(const value_type& value) {
        serialized_buffer<value_type> value_buffer = serialized_value(value);
        return m_inner.insert(value_buffer.data());
    }

    /// Inserts the value into the table. Overwrites any
    /// existing value with the same key.
    /// Returns true if the value was inserted, false if an old value was overwritten.
    bool insert_or_update(const value_type& value) {
        serialized_buffer<value_type> value_buffer = serialized_value(value);
        return m_inner.insert_or_update(value_buffer.data());
    }

    /// Removes the value associated with the given key (of size `key_size()`) from the table.
    /// Returns true if a value existed.
    /// TODO: erase + retrieve?
    bool erase(const key_type& key) {
        serialized_buffer<key_type> key_buffer = serialized_value(key);
        return m_inner.erase(key_buffer.data());
    }

    template<typename CompatibleKeyType, typename CompatibleKeyHash, typename CompatibleKeyEquals>
    bool erase_compatible(const CompatibleKeyType& key,
                          const CompatibleKeyHash& hash,
                          const CompatibleKeyEquals& equals) const
    {
        auto wrapped_hasher = wrap_hash<CompatibleKeyType>(hash);
        auto wrapped_equals = wrap_equals<CompatibleKeyType>(equals);
        return m_inner.erase_compatible(std::addressof(key), std::ref(wrapped_hasher), std::ref(wrapped_equals));
    }

    template<typename IterFunc>
    void iterate(IterFunc&& fn) const {
        m_inner.iterate([&](const byte* raw_value) {
            value_type value = deserialized_value<value_type>(raw_value);
            return fn(value);
        });
    }

    template<typename VisitFunc>
    void visit(VisitFunc&& fn) const {
        m_inner.visit([&](const raw_hash_table::node_view& raw_view) {
            return fn(node_view(raw_view));
        });
    }

    /// Removes all data from this table. After this operation completes, the table
    /// will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset() { m_inner.reset(); }

    /// Erases all values from this table.
    /// \post `empty()`.
    void clear() { m_inner.clear(); }

    /// Prints debugging information to the output stream.
    /// TODO
    void dump(std::ostream& os) const;

    /// Perform internal consistency checks.
    void validate() const { m_inner.validate(); }

    const raw_hash_table& raw() const { return m_inner; }

private:
    // Allocated on the heap for stables addresses (the user_data pointer points to this object).
    struct state_t {
        DeriveKey m_derive_key;
        KeyHash m_key_hash;
        KeyEqual m_key_equal;

        state_t(DeriveKey&& derive_key, KeyHash&& key_hash, KeyEqual&& key_equal)
            : m_derive_key(std::move(derive_key))
            , m_key_hash(std::move(key_hash))
            , m_key_equal(std::move(key_equal))
        {}

        key_type derive_key(const value_type& v) const { return m_derive_key(v); }
        u64 key_hash(const key_type& k) const { return m_key_hash(k); }
        bool key_equal(const key_type& lhs, const key_type& rhs) const { return m_key_equal(lhs, rhs); }
    };

    template<typename CompatibleKeyType, typename CompatibleKeyHash>
    struct compatible_hash_wrapper {
        const CompatibleKeyHash& hash;

        compatible_hash_wrapper(const CompatibleKeyHash& hash)
            : hash(hash)
        {}

        u64 operator()(const void* raw_key) const {
            const CompatibleKeyType& key = *static_cast<const CompatibleKeyType*>(raw_key);
            return hash(key);
        }
    };

    template<typename CompatibleKeyType, typename CompatibleKeyEquals>
    struct compatible_equals_wrapper {
        const CompatibleKeyEquals& equals;

        compatible_equals_wrapper(const CompatibleKeyEquals& equals)
            : equals(equals)
        {}

        bool operator()(const void* raw_lhs_key, const byte* raw_rhs_key) const {
            const CompatibleKeyType& lhs = *static_cast<const CompatibleKeyType*>(raw_lhs_key);
            const key_type rhs = deserialized_value<key_type>(raw_rhs_key);
            return equals(lhs, rhs);
        }
    };

    template<typename CompatibleKey, typename CompatibleKeyHash>
    auto wrap_hash(const CompatibleKeyHash& hash) const {
        return compatible_hash_wrapper<CompatibleKey, CompatibleKeyHash>(hash);
    }

    template<typename CompatibleKey, typename CompatibleKeyEquals>
    auto wrap_equals(const CompatibleKeyEquals& equals) const {
        return compatible_equals_wrapper<CompatibleKey, CompatibleKeyEquals>(equals);
    }

    raw_hash_table_options make_options() {
        raw_hash_table_options options;
        options.value_size = value_size();
        options.key_size = key_size();
        options.user_data = m_state.get();
        options.derive_key = derive_key;
        options.key_hash = key_hash;
        options.key_equal = key_equal;
        return options;
    }

    static void derive_key(const byte* value_buffer, byte* key_buffer, void* user_data) {
        const state_t* state = static_cast<state_t*>(user_data);
        value_type value = deserialized_value<value_type>(value_buffer);
        key_type key = state->derive_key(value);
        serialize(key, key_buffer);
    }

    static u64 key_hash(const byte* key_buffer, void* user_data) {
        const state_t* state = static_cast<state_t*>(user_data);
        key_type key = deserialized_value<key_type>(key_buffer);
        return state->key_hash(key);
    }

    static bool key_equal(const byte* left_key_buffer, const byte* right_key_buffer, void* user_data) {
        const state_t* state = static_cast<state_t*>(user_data);
        key_type lhs = deserialized_value<key_type>(left_key_buffer);
        key_type rhs = deserialized_value<key_type>(right_key_buffer);
        return state->key_equal(lhs, rhs);
    }

private:
    std::unique_ptr<state_t> m_state;
    raw_hash_table m_inner;
};

} // namespace prequel

#endif // PREQUEL_HASH_TABLE_HPP
