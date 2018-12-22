#ifndef PREQUEL_CONTAINER_BTREE_HPP
#define PREQUEL_CONTAINER_BTREE_HPP

#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/container/allocator.hpp>
#include <prequel/container/indexing.hpp>
#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/serialization.hpp>

#include <fmt/ostream.h>

#include <memory>
#include <ostream>

namespace prequel {

class raw_btree;
class raw_btree_cursor;
class raw_btree_loader;

namespace detail {

/// Serialized state of a btree instance.
/// Required for (re-) opening a tree.
struct raw_btree_anchor {
    /// The number of values in this tree.
    u64 size = 0;

    /// The number of leaf nodes in this tree.
    u64 leaf_nodes = 0;

    /// The number of internal nodes in this tree.
    u32 internal_nodes = 0;

    /// The height of the three.
    /// - 0: empty (no nodes)
    /// - 1: root is a leaf with at least one value
    /// - > 1: root is an internal node with at least one key and two children
    u32 height = 0;

    /// Points to the root node (if any).
    block_index root;

    /// Points to the leftmost leaf (if any).
    block_index leftmost;

    /// Points to the rightmost leaf (if any).
    block_index rightmost;

    static constexpr auto get_binary_format() {
        using self = raw_btree_anchor;
        return binary_format(&self::size, &self::leaf_nodes, &self::internal_nodes, &self::height,
                             &self::root, &self::leftmost, &self::rightmost);
    }
};

namespace btree_impl {

class tree;
class loader;
class cursor;

} // namespace btree_impl
} // namespace detail

/// A group of properties required to configure a btree instance.
/// The parameters must be semantically equivalent whenever the
/// tree is (re-) opened.
struct raw_btree_options {
    /// Size of a value, in bytes. Must be > 0.
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
    /// `derive_key` must return equal search keys for equal values.
    void (*derive_key)(const byte* value, byte* key_buffer, void* user_data) = nullptr;

    /// Returns true if `left_key` is less than `right_key`. Both byte buffers
    /// contain keys and have size `key_size`.
    bool (*key_less)(const byte* left_key, const byte* right_key, void* user_data) = nullptr;
};

using raw_btree_anchor = detail::raw_btree_anchor;

/// Cursors are used to traverse the values in a btree.
class raw_btree_cursor {
public:
    raw_btree_cursor();
    raw_btree_cursor(const raw_btree_cursor&);
    raw_btree_cursor(raw_btree_cursor&&) noexcept;
    ~raw_btree_cursor();

    raw_btree_cursor& operator=(const raw_btree_cursor&);
    raw_btree_cursor& operator=(raw_btree_cursor&&) noexcept;

public:
    /// Returns the size of a value.
    u32 value_size() const;

    /// Returns the size of a key derived from a value.
    u32 key_size() const;

    /// Returns a pointer to the current value. The returned pointer has exactly `value_size` readable bytes.
    /// Throws an exception if the cursor does not currently point to a valid value.
    const byte* get() const;

    /// Replaces the current value with the given one. The old and the new value must have the same key.
    /// Throws an exception if the cursor does not currently point to a valid value.
    void set(const byte* value);

    /// True iff this cursor has been positioned at the end of the tree,
    /// in which case it does not point to a valid value.
    /// This happens when the entire tree was traversed or when a search
    /// operation fails to find a value.
    bool at_end() const;

    /// True iff the element this cursor pointed to was erased.
    bool erased() const;

    /// Equivalent to `!at_end()`.
    explicit operator bool() const { return !at_end(); }

    /// Reset the iterator. `at_end()` will return true.
    void reset();

    /// Move this cursor to the smallest value in the tree (leftmost value).
    bool move_min();

    /// Move this cursor to the largest value in the tree (rightmost value).
    bool move_max();

    /// Move this cursor to the next value.
    bool move_next();

    /// Move this cursor to the previous value.
    bool move_prev();

    /// Seeks to the first value for which `derive_key(value) >= key` is true.
    /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
    bool lower_bound(const byte* key);

    /// Like \ref lower_bound, but seeks to the first value for which
    /// `derive_key(value) > key` returns true.
    bool upper_bound(const byte* key);

    /// Seeks to to value with the given key.
    /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
    bool find(const byte* key);

    /// Attempts to insert the given value into the tree. The tree will not be modified
    /// if a value with the same key already exists.
    ///
    /// Returns true if the value was inserted, false otherwise.
    /// The cursor will point to the value in question in any case.
    bool insert(const byte* value);

    /// Inserts the value into the tree. If a value with the same key already exists,
    /// it will be overwritten.
    ///
    /// Returns true if the key did not exist.
    bool insert_or_update(const byte* value);

    /// Erases the element that this cursors points at.
    /// In order for this to work, the cursor must not be at the end and must not
    /// already point at an erased element.
    void erase();

    /// Check cursor invariants. Used when testing.
    /// TODO
    void validate() const;

    bool operator==(const raw_btree_cursor& other) const;
    bool operator!=(const raw_btree_cursor& other) const { return !(*this == other); }

private:
    friend raw_btree;

    raw_btree_cursor(std::unique_ptr<detail::btree_impl::cursor> impl);

private:
    detail::btree_impl::cursor& impl() const;

private:
    std::unique_ptr<detail::btree_impl::cursor> m_impl;
};

/// Implements bulk loading for btrees.
class raw_btree_loader {
public:
    raw_btree_loader() = delete;
    ~raw_btree_loader();

    // Not copyable.
    raw_btree_loader(const raw_btree_loader&) = delete;
    raw_btree_loader& operator=(const raw_btree_loader&) = delete;

    raw_btree_loader(raw_btree_loader&&) noexcept;
    raw_btree_loader& operator=(raw_btree_loader&&) noexcept;

    /// Insert a single new value into the tree.
    /// The value must be greater than the previous values inserted
    /// into the tree.
    void insert(const byte* value);

    /// Insert a number of values into the new tree.
    /// The values must be ordered and unique and must be greater
    /// than the previous values inserted into the tree.
    ///
    /// \warning count is the number of values, *NOT* the number of bytes.
    void insert(const byte* values, size_t count);

    /// Finalizes the loading procedure. All changes will be applied
    /// to the tree and no more values can be inserted using this loader.
    void finish();

    /// Discard all values inserted into this loader (finish() must not have been called).
    /// Frees all allocated blocks and leaves the tree unmodified.
    void discard();

private:
    friend raw_btree;

    raw_btree_loader(std::unique_ptr<detail::btree_impl::loader> impl);

private:
    detail::btree_impl::loader& impl() const;

private:
    std::unique_ptr<detail::btree_impl::loader> m_impl;
};

/**
 * An ordered index for fixed sized values that allows runtime-sized values.
 * This means that it can be used for value sizes that are only known at runtime,
 * for example determined through user input. All values still have to be of the same size.
 *
 * A btree indexes instances of `Value` by deriving a *key* for each value
 * using the `DeriveKey` function. Keys must be comparable using `<` (which can be overwritten
 * by specifying the `KeyLess` parameter).
 * Two values are considered equal if their keys are equal.
 */
class raw_btree {
public:
    using anchor = raw_btree_anchor;
    using cursor = raw_btree_cursor;

    struct insert_result {
        /// Points to the position of the value.
        cursor position;

        /// Whether a new value was inserted into the tree.
        /// This will be false if an equivalent value already
        /// existed within the tree.
        bool inserted = false;

        insert_result() = default;

        insert_result(cursor position_, bool inserted_)
            : position(std::move(position_))
            , inserted(inserted_) {}
    };

    using insert_result_t = insert_result;

    using loader = raw_btree_loader;

    enum cursor_seek_t {
        /// Don't seek to anything, i.e. create an invalid cursor.
        seek_none = 0,
        /// Seek to the smallest value (if any).
        seek_min = 1,
        /// Seek to the largest value (if any).
        seek_max = 2
    };

    class node_view {
    public:
        node_view() = default;
        virtual ~node_view();

        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        virtual bool is_leaf() const = 0;
        virtual bool is_internal() const = 0;

        virtual u32 level() const = 0;
        virtual block_index address() const = 0;
        virtual block_index parent_address() const = 0;

        // For internal nodes.
        virtual u32 child_count() const = 0;
        virtual u32 key_count() const = 0;
        virtual const byte* key(u32 index) const = 0;
        virtual block_index child(u32 index) const = 0;

        // For leaf nodes.
        virtual u32 value_count() const = 0;
        virtual const byte* value(u32 index) const = 0;
    };

public:
    /// Constructs the tree rooted at the existing anchor.
    /// The options must be equivalent every time the tree is opened;
    /// they are not persisted to disk.
    raw_btree(anchor_handle<anchor> _anchor, const raw_btree_options& options, allocator& alloc);
    ~raw_btree();

    raw_btree(raw_btree&& other) noexcept;
    raw_btree& operator=(raw_btree&& other) noexcept;

    raw_btree(const raw_btree&) = delete;
    raw_btree& operator=(const raw_btree&) = delete;

    engine& get_engine() const;
    allocator& get_allocator() const;

    /// Creates a bulk loading object for this tree.
    /// The object should be used to insert values in ascending order (according to the
    /// tree's comparison function) into the tree.
    ///
    /// \note Only empty trees can be bulk-loaded.
    loader bulk_load();

    /// Returns the size (in bytes) of every value in the tree (stored in leaf nodes).
    u32 value_size() const;

    /// Returns the size (in bytes) of every key in the tree (stored in internal nodes).
    u32 key_size() const;

    /// Returns the maximum number of children in an internal node.
    u32 internal_node_capacity() const;

    /// Returns the maximum number of values in a leaf node.
    u32 leaf_node_capacity() const;

    /// Returns true if the tree is empty.
    bool empty() const;

    /// Returns the number of entries in this tree.
    u64 size() const;

    /// Returns the height of this tree. The height is length of all paths from the
    /// root to a leaf (they all have the same length).
    /// - height == 0: The tree is empty.
    /// - height == 1: The tree has a single leaf.
    /// - height >= 2: The first (height - 1) are internal nodes, then a leaf is reached.
    u32 height() const;

    /// Returns the number of internal nodes in this tree.
    u64 internal_nodes() const;

    /// Returns the number of leaf nodes in this tree.
    u64 leaf_nodes() const;

    /// Returns the total number of nodes in this tree.
    u64 nodes() const;

    /// Returns the average fullness of this tree's leaf nodes.
    double fill_factor() const;

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const;

    /// Returns the relative overhead compared to a linear file filled with the
    /// same entries as this tree. Computed by dividing the total size of this tree
    /// by the total size of its elements.
    ///
    /// \note Most leaves and internal nodes are never less than half full.
    double overhead() const;

    /// Create a new cursor and seek it to the specified position.
    /// The cursor is initially invalid if `seek_none` is specified;
    /// otherwise the cursor will attempt to move to the implied element.
    cursor create_cursor(cursor_seek_t seek = seek_none) const;

    /// Seek to the given key within this tree. The cursor will be invalid
    /// if the key was not found, otherwise it will point to the found value.
    cursor find(const byte* key) const;

    /// Seek to the smallest key `lb` with `lb >= key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor lower_bound(const byte* key) const;

    /// Seek to the smallest key `lb` with `lb > key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor upper_bound(const byte* key) const;

    /// Attempts to insert the given value into the tree. The tree will not be modified
    /// if a value with the same key already exists.
    insert_result insert(const byte* value);

    /// Inserts the value into the tree. If a value with the same key already exists,
    /// it will be overwritten.
    insert_result insert_or_update(const byte* value);

    /// Removes all data from this tree. After this operation completes,
    /// the tree will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

    /// Erases the content of this tree.
    /// \post `empty()`.
    void clear();

    /// Prints debugging information to the output stream.
    void dump(std::ostream& os) const;

    /// Perform validation of the tree's structure. Basic invariants, such as the
    /// order and number of values (per node and in total) are checked.
    void validate() const;

    /// Visits every internal node and every leaf node from top to bottom. The function
    /// will be invoked for every node until it returns false, at which point the iteration
    /// through the tree will stop.
    /// The tree must not be modified during this operation.
    void visit(bool (*visit_fn)(const node_view& node, void* user_data),
               void* user_data = nullptr) const;

    template<typename Func>
    void visit(Func&& fn) const {
        using func_t = std::remove_reference_t<Func>;

        bool (*visit_fn)(const node_view&, void*) = [](const node_view& node,
                                                       void* user_data) -> bool {
            func_t* fn_ptr = reinterpret_cast<func_t*>(user_data);
            return (*fn_ptr)(node);
        };
        visit(visit_fn, reinterpret_cast<void*>(std::addressof(fn)));
    }

    // TODO: Efficient min/max push_back/push_front with a private internal cursor.

private:
    detail::btree_impl::tree& impl() const;

private:
    std::unique_ptr<detail::btree_impl::tree> m_impl;
};

/**
 * An ordered index for fixed sized values.
 *
 * A btree indexes instances of `Value` by deriving a *key* for each value
 * using the `DeriveKey` function. Keys must be comparable using `<` (which can be overwritten
 * by specifying the `KeyLess` parameter).
 * Two values are considered equal if their keys are equal.
 */
template<typename Value, typename DeriveKey = indexed_by_identity, typename KeyLess = std::less<>>
class btree {
public:
    /// Typedef for the value type.
    using value_type = Value;

    /// Typedef for the key type, which is the result of applying the `DeriveKey`
    /// function to a value.
    using key_type = remove_cvref_t<std::result_of_t<DeriveKey(Value)>>;

public:
    class anchor {
        raw_btree::anchor tree;

        static constexpr auto get_binary_format() { return binary_format(&anchor::tree); }

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
        bool at_end() const { return m_inner.at_end(); }

        /// True iff the element this cursor pointed to was erased.
        bool erased() const { return m_inner.erased(); }

        /// Equivalent to `!at_end()`.
        explicit operator bool() const { return static_cast<bool>(m_inner); }

        /// Reset the iterator. `at_end()` will return true.
        void reset() { m_inner.reset(); }

        /// Move this cursor to the smallest value in the tree (leftmost value).
        void move_min() { m_inner.move_min(); }

        /// Move this cursor to the largest value in the tree (rightmost value).
        void move_max() { m_inner.move_max(); }

        /// Move this cursor to the next value.
        void move_next() { m_inner.move_next(); }

        /// Move this cursor to the previous value.
        void move_prev() { m_inner.move_prev(); }

        /// Seeks to the first value for which `derive_key(value) >= key` is true.
        /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
        bool lower_bound(const key_type& key) {
            auto buffer = serialize_to_buffer(key);
            return m_inner.lower_bound(buffer.data());
        }

        /// Like \ref lower_bound, but seeks to the first value for which
        /// `derive_key(value) > key` returns true.
        bool upper_bound(const key_type& key) {
            auto buffer = serialize_to_buffer(key);
            return m_inner.upper_bound(buffer.data());
        }

        /// Seeks to to value with the given key.
        /// Returns true if such a value was found. Returns false and becomes invalid otherwise.
        bool find(const key_type& key) {
            auto buffer = serialize_to_buffer(key);
            return m_inner.find(buffer.data());
        }

        /// Attempts to insert the given value into the tree. The tree will not be modified
        /// if a value with the same key already exists.
        ///
        /// Returns true if the value was inserted, false otherwise.
        /// The cursor will point to the value in question in any case.
        bool insert(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            return m_inner.insert(buffer.data());
        }

        /// Inserts the value into the tree. If a value with the same key already exists,
        /// it will be overwritten.
        ///
        /// Returns true if the key did not exist.
        bool insert_or_update(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            return m_inner.insert_or_update(buffer.data());
        }

        /// Erases the element that this cursors points at.
        /// In order for this to work, the cursor must not be at the end and must not
        /// already point at an erased element.
        void erase() { m_inner.erase(); }

        /// Returns the current value of this cursor.
        /// Throws an exception if the cursor does not currently point to a valid value.
        value_type get() const { return deserialize<value_type>(m_inner.get()); }

        /// Replaces the current value with the given one. The old and the new value must have the same key.
        /// Throws an exception if the cursor does not currently point to a valid value.
        void set(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            m_inner.set(buffer.data());
        }

        /// Check cursor invariants. Used when testing.
        void validate() const { m_inner.validate(); }

        bool operator==(const cursor& other) const { return m_inner == other.m_inner; }
        bool operator!=(const cursor& other) const { return m_inner != other.m_inner; }

    private:
        friend class btree;

        cursor(raw_btree::cursor&& inner)
            : m_inner(std::move(inner)) {}

    private:
        raw_btree::cursor m_inner;
    };

    /// Implements bulk loading for btrees.
    class loader {
    public:
        loader() = delete;
        loader(loader&&) noexcept = default;
        loader& operator=(loader&&) noexcept = default;

        /// Insert a single new value into the tree.
        /// The value must be greater than the previous values inserted
        /// into the tree.
        void insert(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            m_inner.insert(buffer.data());
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
        void finish() { m_inner.finish(); }

        /// Discard all values inserted into this loader (finish() must not have been called).
        /// Frees all allocated blocks and leaves the tree unmodified.
        void discard() { m_inner.discard(); }

    private:
        friend class btree;

        loader(raw_btree::loader&& inner)
            : m_inner(std::move(inner)) {}

    private:
        raw_btree::loader m_inner;
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

        insert_result(cursor position_, bool inserted_)
            : position(std::move(position_))
            , inserted(inserted_) {}
    };

public:
    /// \name Construction and configuration
    ///
    /// \{

    /// Constructs the tree rooted at the existing anchor.
    /// The options must be equivalent every time the tree is opened;
    /// they are not persisted to disk.
    explicit btree(anchor_handle<anchor> anchor_, allocator& alloc_,
                   DeriveKey derive_key = DeriveKey(), KeyLess less = KeyLess())
        : m_state(std::make_unique<state_t>(std::move(derive_key), std::move(less)))
        , m_inner(std::move(anchor_).template member<&anchor::tree>(), make_options(), alloc_) {}

    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    /// Derives the key from the given value by applying the DeriveKey function.
    key_type derive_key(const value_type& value) const { return m_state->derive(value); }

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
    loader bulk_load() { return loader(m_inner.bulk_load()); }

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
        auto buffer = serialize_to_buffer(key);
        return cursor(m_inner.find(buffer.data()));
    }

    /// Seek to the smallest key `lb` with `lb >= key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor lower_bound(const key_type& key) const {
        auto buffer = serialize_to_buffer(key);
        return cursor(m_inner.lower_bound(buffer.data()));
    }

    /// Seek to the smallest key `lb` with `lb > key`. The cursor will be invalid
    /// if no such key exists within this tree.
    cursor upper_bound(const key_type& key) const {
        auto buffer = serialize_to_buffer(key);
        return cursor(m_inner.upper_bound(buffer.data()));
    }

    /// Attempts to insert the given value into the tree. The tree will not be modified
    /// if a value with the same key already exists.
    insert_result insert(const value_type& value) {
        auto buffer = serialize_to_buffer(value);
        auto result = m_inner.insert(buffer.data());
        return insert_result(cursor(std::move(result.position)), result.inserted);
    }

    /// Inserts the value into the tree. If a value with the same key already exists,
    /// it will be overwritten.
    insert_result insert_or_update(const value_type& value) {
        auto buffer = serialize_to_buffer(value);
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
        key_type key(u32 index) const { return deserialize<key_type>(m_inner.key(index)); }
        block_index child(u32 index) const { return m_inner.child(index); }

        // For leaf nodes.
        u32 value_count() const { return m_inner.value_count(); }
        value_type value(u32 index) const { return deserialize<value_type>(m_inner.value(index)); }

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
        value_type value = deserialize<value_type>(value_buffer);
        serialize(state->derive(value), key_buffer);
    }

    static bool key_less(const byte* lhs_buffer, const byte* rhs_buffer, void* user_data) {
        const state_t* state = reinterpret_cast<const state_t*>(user_data);
        key_type lhs = deserialize<key_type>(lhs_buffer);
        key_type rhs = deserialize<key_type>(rhs_buffer);
        return state->less(lhs, rhs);
    }

private:
    // Allocated on the heap for stable addresses (user data pointer in raw_btree).
    struct state_t {
        DeriveKey m_derive;
        KeyLess m_less;

        state_t(DeriveKey&& derive_key, KeyLess&& less)
            : m_derive(std::move(derive_key))
            , m_less(std::move(less)) {}

        bool less(const key_type& a, const key_type& b) const { return m_less(a, b); }

        key_type derive(const value_type& v) const { return m_derive(v); }
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
               value_size(), key_size(), internal_node_capacity(), leaf_node_capacity(), height(),
               size(), internal_nodes(), leaf_nodes());

    if (!empty())
        os << "\n";

    auto visitor = [&](const node_view& node) {
        if (node.is_internal()) {
            fmt::print(os,
                       "Internal node @{}:\n"
                       "  Parent: @{}\n"
                       "  Level: {}\n"
                       "  Children: {}\n",
                       node.address(), node.parent_address(), node.level(), node.child_count());

            const u32 child_count = node.child_count();
            for (u32 i = 0; i < child_count - 1; ++i) {
                fmt::print(os, "  {}: @{} (<= {})\n", i, node.child(i), node.key(i));
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

#endif // PREQUEL_CONTAINER_BTREE_HPP
