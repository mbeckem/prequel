#ifndef EXTPP_RAW_BTREE_HPP
#define EXTPP_RAW_BTREE_HPP

#include <extpp/allocator.hpp>
#include <extpp/anchor_handle.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>

#include <memory>

namespace extpp {

class raw_btree;
class raw_btree_impl;
class raw_btree_cursor;
class raw_btree_cursor_impl;

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

/// Serialized state of a btree instance.
/// Required for (re-) opening a tree.
class raw_btree_anchor {
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
        return make_binary_format(
                    &self::size, &self::leaf_nodes, &self::internal_nodes, &self::height,
                    &self::root, &self::leftmost, &self::rightmost);
    }

    friend class raw_btree_impl;
    friend class binary_format_access;
};

/// An efficient index for fixed-size values.
class raw_btree {
public:
    using anchor = raw_btree_anchor;
    using cursor = raw_btree_cursor;

public:
    enum cursor_seek_t {
        /// Don't seek to anything, i.e. create an invalid cursor.
        seek_none = 0,
        /// Seek to the smallest value (if any).
        seek_min = 1,
        /// Seek to the largest value (if any).
        seek_max = 2
    };

    enum cursor_insert_t {
        /// Don't alter the tree if an element already exists.
        keep_existing = 0,
        /// Overwrite the existing element with the same key (if any).
        overwrite_existing = 1
    };

public:
    raw_btree(anchor_handle<anchor> _anchor, const raw_btree_options& options, allocator& alloc);
    ~raw_btree();

    raw_btree(raw_btree&& other) noexcept;
    raw_btree& operator=(raw_btree&& other) noexcept;

    raw_btree(const raw_btree&) = delete;
    raw_btree& operator=(const raw_btree&) = delete;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    /// \name Tree properties
    ///
    /// \{

    /// Returns the size (in bytes) of every value in the tree (stored in leaf nodes).
    u32 value_size() const;

    // Returns the size (in bytes) of every key in the tree (stored in internal nodes).
    u32 key_size() const;

    /// Returns the number of children an internal node can point to.
    u32 internal_node_capacity() const;

    /// Returns the number of values a leaf node can store
    u32 leaf_node_capacity() const;

    /// Returns true if the list is empty.
    bool empty() const;

    /// Returns the number of entries in this tree.
    u64 size() const;

    /// Returns the height of this tree. The height is length of all paths from the
    /// root to a leaf (they all have the same length).
    /// - height == 0: The tree is empty.
    /// - height == 1: The tree has a single leaf.
    /// - height >= 2: The first (height - 1) are internal nodes, then a leaf is reached.
    u64 height() const;

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

    /// \}
    ///

    /// \name Cursor operations

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

    /// Inserts the value into the tree. Does not alter the tree if `mode` is `keep_existing`
    /// and a value with the same key already exists. Overwrites if `mode` is `overwrite_existing`.
    /// The boolean return value is true if the new value was inserted into the tree.
    std::pair<cursor, bool> insert(const byte* value, cursor_insert_t mode = keep_existing);

    /// }

    /// Removes all data from this tree. After this operation completes,
    /// the tree will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

    /// Erases the content of this tree.
    /// \post `empty()`.
    void clear();

    // TODO: Efficient min/max push_back/puhs_front with a private internal cursor.

    void dump(std::ostream& os) const;

    /// Perform validation of the tree's structure. Basic invariants, such as the
    /// order and number of values (per node and in total) are checked.
    void validate() const;

public:
    class node_view {
    public:
        node_view() = default;
        virtual ~node_view();

        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        virtual bool is_leaf() const  = 0;
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

    /// Visits every internal node and every leaf node from top to bottom. The function
    /// will be invoked for every node until it returns false, at which point the iteration
    /// through the tree will stop.
    /// The tree must not be modified during this operation.
    void visit(bool (*visit_fn)(const node_view& node, void* user_data), void* user_data = nullptr) const;

    template<typename Func>
    void visit(Func&& fn) const {
        using func_t = std::remove_reference_t<Func>;

        bool (*visit_fn)(const node_view&, void*) = [](const node_view& node, void* user_data) -> bool {
            func_t* fn = reinterpret_cast<func_t*>(user_data);
            return (*fn)(node);
        };
        visit(visit_fn, reinterpret_cast<void*>(std::addressof(fn)));
    }

private:
    raw_btree_impl& impl() const;

private:
    std::unique_ptr<raw_btree_impl> m_impl;
};

class raw_btree_cursor {
public:
    using cursor_insert_t = raw_btree::cursor_insert_t;

    static constexpr auto keep_existing = raw_btree::keep_existing;
    static constexpr auto overwrite_existing = raw_btree::overwrite_existing;

public:
    raw_btree_cursor();
    raw_btree_cursor(const raw_btree_cursor&);
    raw_btree_cursor(raw_btree_cursor&&) noexcept;
    ~raw_btree_cursor();

    raw_btree_cursor& operator=(const raw_btree_cursor&);
    raw_btree_cursor& operator=(raw_btree_cursor&&) noexcept;

public:
    u32 value_size() const;
    u32 key_size() const;

    bool at_end() const;
    bool erased() const;
    explicit operator bool() const { return !at_end(); }

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

    /// Inserts the given value into the tree. The tree will not be modified
    /// if a value with the same key already exists and mode equals `keep_existing`.
    /// If mode is `overwrite_existing`, the old value will be overwritten instead.
    ///
    /// Returns true if the value was inserted, false otherwise.
    /// The cursor will point to the value in question in any case.
    bool insert(const byte* value, cursor_insert_t mode = keep_existing);

    /// Erases the element that this cursors points at.
    /// In order for this to work, the cursor must not be at the end and must not
    /// already point at an erased element.
    void erase();

    /// Returns a pointer to the current value. The returned pointer has exactly `value_size` readable bytes.
    /// Throws an exception if the cursor does not currently point to a valid value.
    const byte* get() const;

    /// Replaces the current value with the given one. The old and the new value must have the same key.
    /// Throws an exception if the cursor does not currently point to a valid value.
    void set(const byte* value);

    /// Check cursor invariants. Used when testing.
    /// TODO
    void validate() const;

    bool operator==(const raw_btree_cursor& other) const;
    bool operator!=(const raw_btree_cursor& other) const { return !(*this == other); }

private:
    friend class raw_btree;

    raw_btree_cursor(std::unique_ptr<raw_btree_cursor_impl> impl);

private:
    raw_btree_cursor_impl& impl() const;

private:
    std::unique_ptr<raw_btree_cursor_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_RAW_BTREE_HPP
