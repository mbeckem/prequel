#ifndef PREQUEL_BTREE_HPP
#define PREQUEL_BTREE_HPP

#include <prequel/raw_btree.hpp>
#include <prequel/serialization.hpp>

#include <fmt/ostream.h>

#include <memory>
#include <ostream>

namespace prequel {

// TODO: Move somewhere better.
struct identity_t {
    template<typename T>
    T operator()(const T& value) const { return value; }
};

/**
 * An ordered index for fixed sized values.
 *
 * A btree indexes instances of `Value` by deriving a *key* for each value
 * using the `DeriveKey` function. Keys must be comparable using `<` (which can be overwritten
 * by specifying the `KeyLess` parameter).
 * Two values are considered equal if their keys are equal.
 */
template<typename Value, typename DeriveKey = identity_t, typename KeyLess = std::less<>>
class btree {
public:
    /// Typedef for the value type.
    using value_type = Value;

    /// Typedef for the key type, which is the result of applying the `DeriveKey`
    /// function on a value.
    using key_type = std::decay_t<std::result_of_t<DeriveKey(Value)>>;

public:
    class anchor {
        raw_btree::anchor tree;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::tree);
        }

        friend btree;
        friend binary_format_access;
    };

public:
    /// Cursors are used to traverse the values in a btree.
    class cursor {
    public:
        cursor() = default;

        /// Returns the size of a serialized value. This is a compile-time constant.
        static constexpr u32 value_size() { return btree::value_size(); }

        /// Returns the size of a serialized key, derived from a value.
        /// This is a compile-time constant.
        static constexpr u32 key_size() { return btree::key_size(); }

        /// True iff this cursor has been positioned at the end of the tree,
        /// in which case it does not point to a valid value.
        /// This happens when the entire tree was traversed or when a search
        /// operation fails to find a value.
        bool at_end() const { return inner.at_end(); }

        /// True iff the element this cursor pointed to was erased.
        bool erased() const { return inner.erased(); }

        /// Equivalent to `!at_end()`.
        explicit operator bool() const { return static_cast<bool>(inner); }

        /// Reset the iterator. `at_end()` will return true.
        void reset() { inner.reset(); }

        /// Move this cursor to the smallest value in the tree (leftmost value).
        void move_min() { inner.move_min(); }

        /// Move this cursor to the largest value in the tree (rightmost value).
        void move_max() { inner.move_max(); }

        /// Move this cursor to the next value.
        void move_next() { inner.move_next(); }

        /// Move this cursor to the previous value.
        void move_prev() { inner.move_prev(); }

        /// Seeks to the first value for which `derive_key(value) >= key` is true.
        /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
        bool lower_bound(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.lower_bound(buffer.data());
        }

        /// Like \ref lower_bound, but seeks to the first value for which
        /// `derive_key(value) > key` returns true.
        bool upper_bound(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.upper_bound(buffer.data());
        }

        /// Seeks to to value with the given key.
        /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
        bool find(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.find(buffer.data());
        }

        /// Attempts to insert the given value into the tree. The tree will not be modified
        /// if a value with the same key already exists.
        ///
        /// Returns true if the value was inserted, false otherwise.
        /// The cursor will point to the value in question in any case.
        bool insert(const value_type& value) {
            auto buffer = serialized_value(value);
            return inner.insert(buffer.data());
        }

        /// Inserts the value into the tree. If a value with the same key already exists,
        /// it will be overwritten.
        ///
        /// Returns true if the key did not exist.
        bool insert_or_update(const value_type& value) {
            auto buffer = serialized_value(value);
            return inner.insert_or_update(buffer.data());
        }

        /// Erases the element that this cursors points at.
        /// In order for this to work, the cursor must not be at the end and must not
        /// already point at an erased element.
        void erase() { inner.erase(); }

        /// Returns the current value of this cursor.
        /// Throws an exception if the cursor does not currently point to a valid value.
        value_type get() const { return deserialized_value<value_type>(inner.get()); }

        /// Replaces the current value with the given one. The old and the new value must have the same key.
        /// Throws an exception if the cursor does not currently point to a valid value.
        void set(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.set(buffer.data());
        }

        /// Check cursor invariants. Used when testing.
        void validate() const { inner.validate(); }

        bool operator==(const cursor& other) const { return inner == other.inner; }
        bool operator!=(const cursor& other) const { return inner != other.inner; }

    private:
        friend class btree;

        cursor(raw_btree::cursor&& inner): inner(std::move(inner)) {}

    private:
        raw_btree::cursor inner;
    };

    /// Implements bulk loading for btrees.
    class loader {
        raw_btree::loader inner;

    private:
        friend class btree;

        loader(raw_btree::loader&& inner): inner(std::move(inner)) {}

    public:
        loader() = delete;
        loader(loader&&) noexcept = default;
        loader& operator=(loader&&) noexcept = default;

        /// Insert a single new value into the tree.
        /// The value must be greater than the previous values inserted
        /// into the tree.
        void insert(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.insert(buffer.data());
        }

        /// Insert a number of values into the new tree.
        /// The values must be ordered and unique and must be greater
        /// than the previous values inserted into the tree.
        template<typename InputIter>
        void insert(const InputIter& begin, const InputIter& end) {
            // TODO: Test whether batching here is worth it, i.e. maybe just
            // push one large, serialized buffer to the raw loader.
            for (auto i = begin; i != end; ++i) {
                insert(*i);
            }
        }

        /// Finalizes the loading procedure. All changes will be applied
        /// to the tree and no more values can be inserted using this loader.
        void finish() { inner.finish(); }

        /// Discard all values inserted into this loader (finish() must not have been called).
        /// Frees all allocated blocks and leaves the tree unmodified.
        void discard() { inner.discard(); }
    };

public:
    using cursor_seek_t = raw_btree::cursor_seek_t;

    static constexpr cursor_seek_t seek_none = raw_btree::seek_none;
    static constexpr cursor_seek_t seek_min = raw_btree::seek_min;
    static constexpr cursor_seek_t seek_max = raw_btree::seek_max;

    struct insert_result {
        cursor position;
        bool inserted = false;

        insert_result() = default;

        insert_result(cursor position, bool inserted)
            : position(std::move(position))
            , inserted(inserted)
        {}
    };

public:
    /// \name Construction and configuration
    ///
    /// \{

    /// Constructs the tree rooted at the existing anchor.
    /// The options must be equivalent every time the tree is opened;
    /// they are not persisted to disk.
    explicit btree(anchor_handle<anchor> anchor_, allocator& alloc_, DeriveKey derive_key = DeriveKey(), KeyLess less = KeyLess())
        : m_state(std::make_unique<state_t>(std::move(derive_key), std::move(less)))
        , m_inner(std::move(anchor_).template member<&anchor::tree>(), make_options(), alloc_)
    {}

    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    /// Derives the key from the given value by applying the DeriveKey function.
    key_type derive_key(const value_type& value) const {
        return m_state->derive(value);
    }

    /// Compares the two keys and returns true iff `lhs < rhs`, as specified
    /// by the KeyLess function.
    bool key_less(const key_type& lhs, const key_type& rhs) const {
        return m_state->less(lhs, rhs);
    }

    /// Creates a bulk loading object for this tree.
    /// The object should be used to insert values in ascending order (according to the
    /// tree's comparison function) into the tree.
    ///
    /// \note Only empty trees can be bulk-loaded.
    loader bulk_load() {
        return loader(m_inner.bulk_load());
    }

    /// \}

    /// \name Tree size
    ///
    /// \{

    /// Returns the size of a serialized value. This is a compile-time constant.
    static constexpr u32 value_size() { return serialized_size<value_type>(); }

    /// Returns the size of a serialized key, derived from a value.
    /// This is a compile-time constant.
    static constexpr u32 key_size() { return serialized_size<key_type>(); }

    /// Returns the maximum number of children in an internal node.
    u32 internal_node_capacity() const { return m_inner.internal_node_capacity(); }

    /// Returns the maximum number of values in a leaf node.
    u32 leaf_node_capacity() const { return m_inner.leaf_node_capacity(); }

    /// Returns true if the tree is empty.
    bool empty() const { return m_inner.empty(); }

    /// Returns the number of entries in this tree.
    u64 size() const { return m_inner.size(); }

    /// Returns the height of this tree. The height is length of all paths from the
    /// root to a leaf (they all have the same length).
    /// - height == 0: The tree is empty.
    /// - height == 1: The tree has a single leaf.
    /// - height >= 2: The first (height - 1) are internal nodes, then a leaf is reached.
    u32 height() const { return m_inner.height(); }

    /// Returns the number of internal nodes in this tree.
    u64 internal_nodes() const { return m_inner.internal_nodes(); }

    /// Returns the number of leaf nodes in this tree.
    u64 leaf_nodes() const { return m_inner.leaf_nodes(); }

    /// Returns the total number of nodes in this tree.
    u64 nodes() const { return m_inner.nodes(); }

    /// Returns the average fullness of this tree's leaf nodes.
    double fill_factor() const { return m_inner.fill_factor(); }

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const { return m_inner.byte_size(); }

    /// Returns the relative overhead compared to a linear file filled with the
    /// same entries as this tree. Computed by dividing the total size of this tree
    /// by the total size of its elements.
    ///
    /// \note Most leaves and internal nodes are never less than half full.
    double overhead() const { return m_inner.overhead(); }

    /// \}

    /// Create a new cursor and seek it to the specified position.
    /// The cursor is initially invalid if `seek_none` is specified;
    /// otherwise the cursor will attempt to move to the implied element.
    cursor create_cursor(cursor_seek_t seek = seek_none) const {
        return cursor(m_inner.create_cursor(seek));
    }

    /// Seek to the given key within this tree. The cursor will be invalid
    /// if the key was not found, otherwise it will point to the found value.
    cursor find(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.find(buffer.data()));
    }

    /// Seek to the smallest key `lb` with `lb >= key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor lower_bound(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.lower_bound(buffer.data()));
    }

    /// Seek to the smallest key `lb` with `lb > key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor upper_bound(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.upper_bound(buffer.data()));
    }

    /// Attempts to insert the given value into the tree. The tree will not be modified
    /// if a value with the same key already exists.
    insert_result insert(const value_type& value) {
        auto buffer = serialized_value(value);
        auto result = m_inner.insert(buffer.data());
        return insert_result(cursor(std::move(result.position)), result.inserted);
    }

    /// Inserts the value into the tree. If a value with the same key already exists,
    /// it will be overwritten.
    insert_result insert_or_update(const value_type& value) {
        auto buffer = serialized_value(value);
        auto result = m_inner.insert_or_update(buffer.data());
        return insert_result(cursor(std::move(result.position)), result.inserted);
    }

    /// Removes all data from this tree. After this operation completes,
    /// the tree will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset() { m_inner.reset(); }

    /// Erases the content of this tree.
    /// \post `empty()`.
    void clear() { m_inner.clear(); }

public:
    class node_view final {
    public:
        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        bool is_leaf() const { return m_inner.is_leaf(); }
        bool is_internal() const { return m_inner.is_internal(); }

        u32 level() const { return m_inner.level(); }
        block_index address() const { return m_inner.address(); }
        block_index parent_address() const { return m_inner.parent_address(); }

        // For internal nodes.
        u32 child_count() const { return m_inner.child_count(); }
        u32 key_count() const { return m_inner.key_count(); }
        key_type key(u32 index) const { return deserialized_value<key_type>(m_inner.key(index)); }
        block_index child(u32 index) const { return m_inner.child(index); }

        // For leaf nodes.
        u32 value_count() const { return m_inner.value_count(); }
        value_type value(u32 index) const { return deserialized_value<value_type>(m_inner.value(index)); }

    private:
        friend class btree;

        node_view(const raw_btree::node_view& nv)
            : m_inner(nv) {}

    private:
        const raw_btree::node_view& m_inner;
    };

    /// Visits every internal node and every leaf node from top to bottom. The function
    /// will be invoked for every node until it returns false, at which point the iteration
    /// through the tree will stop.
    /// The tree must not be modified during this operation.
    template<typename Func>
    void visit(Func&& fn) const {
        m_inner.visit([&](const raw_btree::node_view& raw_view) -> bool {
            node_view typed_view(raw_view);
            return fn(typed_view);
        });
    }

    void dump(std::ostream& os);

    /// Perform validation of the tree's structure. Basic invariants, such as the
    /// order and number of values (per node and in total) are checked.
    void validate() const { m_inner.validate(); }

    /// Returns a reference to the underlying raw btree.
    const raw_btree& raw() const { return m_inner; }

private:
    raw_btree_options make_options() {
        raw_btree_options options;
        options.value_size = value_size();
        options.key_size = key_size();
        options.user_data = m_state.get();
        options.derive_key = derive_key;
        options.key_less = key_less;
        return options;
    }

    static void derive_key(const byte* value_buffer, byte* key_buffer, void* user_data) {
        const state_t* state = reinterpret_cast<const state_t*>(user_data);
        value_type value = deserialized_value<value_type>(value_buffer);
        serialize(state->derive(value), key_buffer);
    }

    static bool key_less(const byte* lhs_buffer, const byte* rhs_buffer, void* user_data) {
        const state_t* state = reinterpret_cast<const state_t*>(user_data);
        key_type lhs = deserialized_value<key_type>(lhs_buffer);
        key_type rhs = deserialized_value<key_type>(rhs_buffer);
        return state->less(lhs, rhs);
    }

private:
    // Allocated on the heap for stable addresses (user data pointer in raw_btree).
    struct state_t {
        DeriveKey m_derive;
        KeyLess m_less;

        state_t(DeriveKey&& derive_key, KeyLess&& less)
            : m_derive(std::move(derive_key))
            , m_less(std::move(less))
        {}

        bool less(const key_type& a, const key_type& b) const {
            return m_less(a, b);
        }

        key_type derive(const value_type& v) const {
            return m_derive(v);
        }
    };

private:
    std::unique_ptr<state_t> m_state;
    raw_btree m_inner;
};

template<typename T, typename DeriveKey, typename KeyLess>
void btree<T, DeriveKey, KeyLess>::dump(std::ostream& os) {
    fmt::print(os,
       "Btree:\n"
       "  Value size: {}\n"
       "  Key size: {}\n"
       "  Internal node capacity: {}\n"
       "  Leaf node capacity: {}\n"
       "  Height: {}\n"
       "  Size: {}\n"
       "  Internal nodes: {}\n"
       "  Leaf nodes: {}\n",
       value_size(), key_size(),
       internal_node_capacity(), leaf_node_capacity(),
       height(), size(), internal_nodes(), leaf_nodes());

    if (!empty())
        os << "\n";

    auto visitor = [&](const node_view& node) {
        if (node.is_internal()) {
            fmt::print(os,
                       "Internal node @{}:\n"
                       "  Parent: @{}\n"
                       "  Level: {}\n"
                       "  Children: {}\n",
                       node.address(), node.parent_address(),
                       node.level(), node.child_count());

            const u32 child_count = node.child_count();
            for (u32 i = 0; i < child_count - 1; ++i) {
                fmt::print(os, "  {}: @{} (<= {})\n",
                           i, node.child(i), node.key(i));
            }
            fmt::print(os, "  {}: @{}\n", child_count - 1, node.child(child_count - 1));
        } else {
            const u32 size = node.value_count();
            fmt::print(os,
                       "Leaf node @{}:\n"
                       "  Parent: @{}\n"
                       "  Values: {}\n",
                       node.address(), node.parent_address(), size);
            for (u32 i = 0; i < size; ++i) {
                fmt::print(os, "  {}: {}\n", i, node.value(i));
            }
        }
        return true;
    };
    visit(visitor);
}

} // namespace prequel

#endif // PREQUEL_BTREE_HPP
