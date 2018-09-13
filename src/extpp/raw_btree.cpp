#include <extpp/raw_btree.hpp>

#include <extpp/exception.hpp>
#include <extpp/formatting.hpp>
#include <extpp/raw_btree_leaf_node.hpp>
#include <extpp/raw_btree_internal_node.hpp>
#include <extpp/detail/deferred.hpp>
#include <extpp/detail/fix.hpp>
#include <extpp/detail/iter_tools.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <algorithm>
#include <array>
#include <vector>

namespace extpp {

namespace {

// Key size is limited to allow for stack allocation, C++ does not have VLAs.
// Might as well use alloca() and rely on compiler extensions but this should be fine for now.
static constexpr u32 max_key_size = 256;

} // namespace

using index_iterator = detail::identity_iterator<u32>;
using key_buffer = std::array<byte, max_key_size>;
using leaf_node = raw_btree_leaf_node;
using internal_node = raw_btree_internal_node;

class raw_btree_cursor_impl {
public:
    // Represents one of the parent (internal) nodes of the current leaf.
    // The first entry (if any) is the root, then the root's child and so forth.
    // The index is the index of the next level's node (internal or leaf) in its parent.
    struct internal_entry {
        internal_node node;
        u32 index = 0;
    };

    enum flags_t {
        INVALID = 1 << 0,       ///< When the cursor is at the end or was otherwise invalidated.
        DELETED = 1 << 1,       ///< When the current element was inserted (TODO)
        INPROGRESS = 1 << 2,    ///< When an operation is not yet complete.
    };

    raw_btree_impl* tree = nullptr;

    /// Tracked cursors are linked together in a list.
    /// When elements are inserted or removed, existing cursors are updated
    /// so that they keep pointing at the same element.
    boost::intrusive::list_member_hook<> cursors;

    // Parents of the current leaf node.
    std::vector<internal_entry> parents;

    // The current leaf node.
    raw_btree_leaf_node leaf;

    // The current value's index in its leaf.
    u32 index = 0;

    // A combination of flags_t values.
    int flags = 0;

public:
    raw_btree_cursor_impl(raw_btree_impl* tree);
    ~raw_btree_cursor_impl();

    raw_btree_cursor_impl(const raw_btree_cursor_impl&) = delete;
    raw_btree_cursor_impl& operator=(const raw_btree_cursor_impl&) = delete;

    // Explicit to avoid errors.
    void copy(const raw_btree_cursor_impl&);

    void reset_to_zero() {
        flags = 0;
        parents.clear();
        leaf = raw_btree_leaf_node();
        index = 0;
    }

    void reset_to_invalid(int saved_flags = 0) {
        reset_to_zero();
        flags |= saved_flags;
        flags |= INVALID;
    }

    bool invalid() const { return flags & INVALID; }

public:
    u32 value_size() const;
    u32 key_size() const;

    bool at_end() const;
    bool erased() const;

    bool move_min();
    bool move_max();
    bool move_next();
    bool move_prev();

    bool lower_bound(const byte* key);
    bool upper_bound(const byte* key);
    bool find(const byte* key);
    bool insert(const byte* value, bool overwrite);
    void erase();

    const byte* get() const;
    void set(const byte* value);

    void validate() const;

    bool operator==(const raw_btree_cursor_impl& other) const;

private:
    // Seek to the first or last entry in the tree.
    template<bool max>
    void init_position();

private:
    void check_tree_valid() const;
    void check_element_valid() const;
};

class raw_btree_impl : public uses_allocator {
    using anchor = raw_btree_anchor;

public:
    raw_btree_impl(anchor_handle<anchor> _anchor, const raw_btree_options& opts, allocator& alloc);
    ~raw_btree_impl();

    raw_btree_impl(const raw_btree_impl&) = delete;
    raw_btree_impl& operator=(const raw_btree_impl&) = delete;

public:
    bool empty() const { return size() == 0; }

    u32 value_size() const { return m_options.value_size; }
    u32 key_size() const { return m_options.key_size; }

    u32 leaf_node_max_values() const { return m_leaf_capacity; }
    u32 internal_node_max_children() const { return m_internal_max_children; }
    u32 internal_node_min_chlidren() const { return m_internal_min_children; }

    // Returns left < right
    bool key_less(const byte* left_key, const byte* right_key) const {
        return m_options.key_less(left_key, right_key, m_options.user_data);
    }

    // Returns left == right (in a non-optimal way).
    bool key_equal(const byte* left_key, const byte* right_key) const {
        return !key_less(left_key, right_key) && !key_less(right_key, left_key);
    }

    bool key_greater(const byte* left_key, const byte* right_key) const {
        return key_less(right_key, left_key);
    }

    // Returns left < right
    bool value_less(const byte* left_value, const byte* right_value) const;

    bool value_equal_key(const byte* value, const byte* key) const {
        key_buffer k;
        derive_key(value, k.data());
        return key_equal(key, k.data());
    }

    // Returns key(value)
    void derive_key(const byte* value, byte* key_buffer) const {
        return m_options.derive_key(value, key_buffer, m_options.user_data);
    }

    // Seek the cursor to the lower bound of key.
    void lower_bound(const byte* key, raw_btree_cursor_impl& cursor) const;

    // Seek the cursor to the upper bound of key.
    void upper_bound(const byte* key, raw_btree_cursor_impl& cursor) const;

    // Find the key (or fail).
    void find(const byte* key, raw_btree_cursor_impl& cursor) const;

    // Insert the value into the tree (or do nothing if the key exists).
    // Points to the value (old or new) after the operation completed.
    bool insert(const byte* value, raw_btree_cursor_impl& cursor);

    /// Erase the element that is currently being pointed at by this cursor.
    void erase(raw_btree_cursor_impl& cursor);

    void clear();

    void clear_subtree(block_index root, u32 level);

    std::unique_ptr<raw_btree_loader_impl> bulk_load();

    std::unique_ptr<raw_btree_cursor_impl> create_cursor(raw_btree::cursor_seek_t seek);

    void dump(std::ostream& os) const;

    void visit(bool (*visit_fn)(const raw_btree::node_view& node, void* user_data), void* user_data) const;

    void validate() const;

private:
    void seek_insert_location(const byte* key, raw_btree_cursor_impl& cursor);

    // Create a new root node from two children (leaves or internal) and a split key.
    internal_node create_root(block_index left_child, block_index right_child, const byte* split_key);

    // Exactly like split(const leaf_node&, byte*) but for internal nodes.
    internal_node split(const internal_node& old_internal, byte* split_key);

    // The root was split. Make sure that all (valid) cursors include the new root in their path.
    void apply_root_split(const internal_node& new_root, const internal_node& left_leaf, const internal_node& right_leaf);

    // A node with a (non-full!) parent was split. The left node was at left_index and remains there. The right node
    // becomes the child at index left_index + 1 and all nodes further to the right have their index incremented by one.
    // 'level' is the level of the left node.
    void apply_child_split(const internal_node& parent, u32 left_level, u32 left_index, const internal_node& left, const internal_node& right);

private:
    // Handle the deletion of a leaf node in its parent node(s).
    void propagate_leaf_deletion(raw_btree_cursor_impl& cursor, block_index child_node, u32 child_node_index);

    // Steal an entry from the neighboring node and put it into the leaf.
    void steal_leaf_entry(const internal_node& parent,
                          const raw_btree_leaf_node& leaf, u32 leaf_index,
                          const raw_btree_leaf_node& neighbor, u32 neighbor_index);
    void steal_internal_entry(const internal_node& parent, u32 stack_index,
                              const internal_node& internal, u32 internal_index,
                              const internal_node& neighbor, u32 neighbor_index);

    // Merge two neighboring nodes.
    // Note: values will end up in "leaf" and will be taken from "neighbor".
    void merge_leaf(const internal_node& parent,
                    const raw_btree_leaf_node& leaf, u32 leaf_index,
                    const raw_btree_leaf_node& neighbor, u32 neighbor_index);
    void merge_internal(const internal_node& parent, u32 stack_index,
                        const internal_node& node, u32 node_index,
                        const internal_node& neighbor, u32 neighbor_index);


private:
    // Index of the first value >= key or leaf.size() if none exists.
    u32 lower_bound(const raw_btree_leaf_node& leaf, const byte* key) const;

    // Index of the first child >= key or the index of the last child.
    u32 lower_bound(const internal_node& internal, const byte* key) const;

    u32 upper_bound(const raw_btree_leaf_node& leaf, const byte* key) const;
    u32 upper_bound(const internal_node& internal, const byte* key) const;

    // Seek to the first value with key(value) > key (upper)
    // or >= key (lower).
    enum seek_bound_t {
        seek_bound_lower,
        seek_bound_upper,
        seek_bound_find
    };

    template<seek_bound_t which>
    void seek_bound(const byte* key, raw_btree_cursor_impl& cursor) const;

public:
    // TODO: Move rest of cursor navigation here too, then make this private again.
    // Also get rid of most of the code in cursor_impl.
    bool next_leaf(raw_btree_cursor_impl& cursor) const;
    bool prev_leaf(raw_btree_cursor_impl& cursor) const;

private:
    template<typename Func>
    void visit_nodes(Func&& fn) const;

public:
    raw_btree_leaf_node as_leaf(block_handle handle) const;
    internal_node as_internal(block_handle handle) const;

    raw_btree_leaf_node read_leaf(block_index index) const;
    internal_node read_internal(block_index) const;

private:
    friend class raw_btree_loader_impl;

    raw_btree_leaf_node create_leaf();
    internal_node create_internal();

    void free_leaf(block_index leaf);
    void free_internal(block_index internal);

private:
    // Cursor management

    friend class raw_btree_cursor_impl;

    using cursor_list_type = boost::intrusive::list<
        raw_btree_cursor_impl,
        boost::intrusive::member_hook<
            raw_btree_cursor_impl,
            decltype(raw_btree_cursor_impl::cursors),
            &raw_btree_cursor_impl::cursors
        >
    >;

    void link_cursor(raw_btree_cursor_impl* cursor) {
        m_cursors.push_back(*cursor);
    }

    void unlink_cursor(raw_btree_cursor_impl* cursor) {
        m_cursors.erase(m_cursors.iterator_to(*cursor));
    }

public:
    // Persistent tree state accessors

    u32 height() const { return m_anchor.get<&anchor::height>(); }
    u64 size() const { return m_anchor.get<&anchor::size>(); }
    block_index root() const { return m_anchor.get<&anchor::root>(); }
    block_index leftmost() const { return m_anchor.get<&anchor::leftmost>(); }
    block_index rightmost() const { return m_anchor.get<&anchor::rightmost>(); }
    u64 leaf_nodes() const { return m_anchor.get<&anchor::leaf_nodes>(); }
    u64 internal_nodes() const { return m_anchor.get<&anchor::internal_nodes>(); }

    void set_height(u32 height) {
        m_anchor.set<&anchor::height>(height);
    }

    void set_size(u64 size) {
        m_anchor.set<&anchor::size>(size);
    }

    void set_root(block_index root) {
        m_anchor.set<&anchor::root>(root);
    }

    void set_leftmost(block_index leftmost) {
        m_anchor.set<&anchor::leftmost>(leftmost);
    }

    void set_rightmost(block_index rightmost) {
        m_anchor.set<&anchor::rightmost>(rightmost);
    }

    void set_internal_nodes(u32 internal_nodes) {
        m_anchor.set<&anchor::internal_nodes>(internal_nodes);
    }

    void set_leaf_nodes(u64 leaf_nodes) {
        m_anchor.set<&anchor::leaf_nodes>(leaf_nodes);
    }

private:
    anchor_handle<anchor> m_anchor;
    raw_btree_options m_options;
    u32 m_internal_max_children;
    u32 m_internal_min_children;
    u32 m_leaf_capacity;
    // List of all active cursors.
    mutable cursor_list_type m_cursors;
};


// --------------------------------
//
//   BTree implementation
//
// --------------------------------

raw_btree_impl::raw_btree_impl(anchor_handle<anchor> _anchor, const raw_btree_options& opts, allocator& alloc)
    : uses_allocator(alloc)
    , m_anchor(std::move(_anchor))
    , m_options(opts)
{
    if (m_options.value_size == 0)
        EXTPP_THROW(bad_argument("invalid value size"));
    if (m_options.key_size == 0)
        EXTPP_THROW(bad_argument("invalid key size"));
    if (m_options.key_size > max_key_size)
        EXTPP_THROW(bad_argument(fmt::format("key sizes larger than {} are not supported", max_key_size)));
    if (!m_options.derive_key)
        EXTPP_THROW(bad_argument("no derive_key function provided"));
    if (!m_options.key_less)
        EXTPP_THROW(bad_argument("no key_less function provided"));

    m_leaf_capacity = raw_btree_leaf_node::capacity(get_engine().block_size(), value_size());
    m_internal_max_children = internal_node::compute_max_children(get_engine().block_size(), key_size());
    m_internal_min_children = internal_node::compute_min_children(m_internal_max_children);

    if (m_leaf_capacity < 2)
        EXTPP_THROW(bad_argument("block size too small (cannot fit 2 values into one leaf)"));
    if (m_internal_max_children < 4)
        EXTPP_THROW(bad_argument("block size too small (cannot fit 4 children into one internal node)"));
}

raw_btree_impl::~raw_btree_impl()
{
    // Invalidate all existing cursors.
    for (auto& cursor : m_cursors) {
        cursor.reset_to_invalid();
        cursor.tree = nullptr;
    }
}

bool raw_btree_impl::value_less(const byte* left_value, const byte* right_value) const {
    key_buffer left_key, right_key;
    derive_key(left_value, left_key.data());
    derive_key(right_value, right_key.data());
    return key_less(left_key.data(), right_key.data());
}

template<raw_btree_impl::seek_bound_t which>
void raw_btree_impl::seek_bound(const byte* key, raw_btree_cursor_impl& cursor) const {
    EXTPP_ASSERT(cursor.tree == this, "Cursor does not belong to this tree.");

    if (height() == 0) {
        // Tree is empty.
        cursor.reset_to_invalid();
        return;
    }

    cursor.reset_to_zero();
    cursor.flags |= cursor.INPROGRESS;

    // For every level of internal nodes
    cursor.parents.resize(height() - 1);
    block_index current = root();
    for (u32 level = height() - 1; level > 0; --level) {
        auto& entry = cursor.parents[height() - 1 - level];
        entry.node = read_internal(current);
        if constexpr (which == seek_bound_upper) {
            entry.index = upper_bound(entry.node, key);
        } else /* find and lower */ {
            entry.index = lower_bound(entry.node, key);
        }
        current = entry.node.get_child(entry.index);
    }

    // Search in the leaf node
    cursor.leaf = read_leaf(current);
    if constexpr (which == seek_bound_upper) {
        cursor.index = upper_bound(cursor.leaf, key);
    } else /* find and lower */ {
        cursor.index = lower_bound(cursor.leaf, key);
    }

    if (cursor.index == cursor.leaf.get_size()) {
        if constexpr (which == seek_bound_lower || which == seek_bound_upper) {
            // The upper/lower bound must be in the next node.
            // This is because parent keys in the upper levels might be out of date, i.e.
            // they might be larger than the max element in their subtree.
            if (!next_leaf(cursor)) {
                cursor.reset_to_invalid();
                return;
            }
            cursor.index = 0;
        } else {
            // Don't do that for normal find though; it must fail here.
            cursor.reset_to_invalid();
            return;
        }
    }

    cursor.flags &= ~cursor.INPROGRESS;
}

bool raw_btree_impl::next_leaf(raw_btree_cursor_impl& cursor) const {
    // Find a parent that is not yet at it's last index.
    auto rpos = std::find_if(cursor.parents.rbegin(), cursor.parents.rend(), [&](const auto& entry) {
        return entry.index + 1 < entry.node.get_child_count();
    });
    if (rpos == cursor.parents.rend()) {
        return false;
    }

    // Increment the index in this parent node, then walk to the leaf level and set all indices to 0.
    // Note that rpos.base() is the element *after* the parent.
    auto pos = std::prev(rpos.base());
    pos->index++;
    for (auto child_pos = pos + 1; child_pos != cursor.parents.end(); ++child_pos, ++pos) {
        child_pos->node = read_internal(pos->node.get_child(pos->index));
        child_pos->index = 0;
    }

    cursor.leaf = read_leaf(pos->node.get_child(pos->index));
    return true;
}

bool raw_btree_impl::prev_leaf(raw_btree_cursor_impl& cursor) const {
    // Find a parent that is not yet at index 0.
    auto rpos = std::find_if(cursor.parents.rbegin(), cursor.parents.rend(), [&](const auto& entry) {
        return entry.index > 0;
    });
    if (rpos == cursor.parents.rend()) {
        return false;
    }

    // Decrement the index in this parent node, then walk to the leaf level and set all indices to size - 1.
    // Note that rpos.base() is the element *after* the parent.
    auto pos = std::prev(rpos.base());
    pos->index -= 1;
    for (auto child_pos = pos + 1; child_pos != cursor.parents.end(); ++child_pos, ++pos) {
        child_pos->node = read_internal(pos->node.get_child(pos->index));
        child_pos->index = child_pos->node.get_child_count() - 1;
    }

    cursor.leaf = read_leaf(pos->node.get_child(pos->index));
    return true;
}

void raw_btree_impl::lower_bound(const byte* key, raw_btree_cursor_impl& cursor) const {
    return seek_bound<seek_bound_lower>(key, cursor);
}

void raw_btree_impl::upper_bound(const byte* key, raw_btree_cursor_impl& cursor) const {
    return seek_bound<seek_bound_upper>(key, cursor);
}

void raw_btree_impl::find(const byte* key, raw_btree_cursor_impl& cursor) const {
    seek_bound<seek_bound_find>(key, cursor);
    if (cursor.at_end())
        return;

    EXTPP_ASSERT(cursor.index < cursor.leaf.get_size(), "Invalid index.");
    if (!value_equal_key(cursor.leaf.get(cursor.index), key)) {
        cursor.reset_to_invalid();
    }
}

u32 raw_btree_impl::lower_bound(const leaf_node& leaf, const byte* search_key) const {
    const u32 size = leaf.get_size();
    index_iterator result = std::lower_bound(index_iterator(0), index_iterator(size), search_key, [&](u32 i, const byte* search_key) {
        key_buffer buffer;
        derive_key(leaf.get(i), buffer.data());
        return key_less(buffer.data(), search_key);
    });
    return *result;
}

u32 raw_btree_impl::lower_bound(const internal_node& internal, const byte* search_key) const {
    EXTPP_ASSERT(internal.get_child_count() > 1, "Not enough children in this internal node");
    // internal.get_size() is the number of children, not the number of keys.
    const u32 keys = internal.get_child_count() - 1;
    index_iterator result = std::lower_bound(index_iterator(0), index_iterator(keys), search_key, [&](u32 i, const byte* search_key) {
        return key_less(internal.get_key(i), search_key);
    });
    return *result;
}

u32 raw_btree_impl::upper_bound(const leaf_node& leaf, const byte* search_key) const {
    const u32 size = leaf.get_size();
    index_iterator result = std::upper_bound(index_iterator(0), index_iterator(size), search_key, [&](const byte* search_key, u32 i) {
        key_buffer buffer;
        derive_key(leaf.get(i), buffer.data());
        return key_less(search_key, buffer.data());
    });
    return *result;
}

u32 raw_btree_impl::upper_bound(const internal_node& internal, const byte* search_key) const {
    EXTPP_ASSERT(internal.get_child_count() > 1, "Not enough children in this internal node");
    // internal.get_size() is the number of children, not the number of keys.
    const u32 keys = internal.get_child_count() - 1;
    index_iterator result = std::upper_bound(index_iterator(0), index_iterator(keys), search_key, [&](const byte* search_key, u32 i) {
        return key_less(search_key, internal.get_key(i));
    });
    return *result;
}

// Insert a new value into the btree. Leave the cursor pointing to the position where the value
// was inserted. Do nothing if a value with the same key already existed (the cursor is still repositioned, though).
//
// 1.   Walk down the stack and split nodes that are full. This ensures that we can always insert a new entry if we need to.
//      It saves some effort to walk back up the stack in the case that a node has been split and its parent is full.
//      It might be more performant in the future than the alternative because we dont have to lock nodes
//      for an extended period of time and just modify them once. (but node locks are in the very remote future).
// 2.   Perform lower bound search at every node. When we reached the leaf we either found a place
//      where we can insert the new value or we found an existing value with the same key.
//      We can insert if we want to without any complications because of (1).
// 3.   Keep cursors updated in the meantime. Every time a record is inserted or a node is split we must
//      ensure that the existing cursors keep pointing to their old values.
//
//      Note:
//          sqlite solves this in a different way: when a value moves, the cursor(s) that point to it are invalidated
//          and remember which key they pointed to in their local storage. When the value is required again, they simply
//          traverse the tree and move to the value again. I don't like that solution right now.
bool raw_btree_impl::insert(const byte* value, raw_btree_cursor_impl& cursor) {
    if (empty()) {
        leaf_node leaf = create_leaf();
        leaf.set(0, value);
        leaf.set_size(1);

        set_height(1);
        set_size(1);
        set_root(leaf.index());
        set_leftmost(leaf.index());
        set_rightmost(leaf.index());

        cursor.reset_to_zero();
        cursor.leaf = std::move(leaf);
        cursor.index = 0;
        return true;
    }

    cursor.reset_to_zero();
    cursor.flags |= cursor.INPROGRESS;

    // Find the correct position where we could insert the new value.
    // The seek function already splits internal nodes along the way.
    // The leaf node might be full though.
    key_buffer key;
    derive_key(value, key.data());
    seek_insert_location(key.data(), cursor);

    const leaf_node leaf = cursor.leaf;
    const u32 insert_index = cursor.index;
    const u32 leaf_size = cursor.leaf.get_size();
    if (insert_index < leaf_size && value_equal_key(leaf.get(insert_index), key.data())) {
        cursor.flags &= ~cursor.INPROGRESS;
        return false; // Equivalent value exists.
    }

    if (leaf_size < leaf.max_size()) {
        // Simple case: enough space in the leaf.
        leaf.insert_nonfull(insert_index, value);
        for (auto& c : m_cursors) {
            if (c.invalid() || c.leaf.index() != leaf.index())
                continue;

            if (&c != &cursor && c.index >= insert_index)
                c.index += 1;
        }
    } else {
        // The leaf is full.
        leaf_node new_leaf = create_leaf();

        // Number of remaining entries in the existing leaf.
        const u32 left_size = [&]{
            if (leaf.index() == rightmost())
                return leaf_size;
            if (leaf.index() == leftmost())
                return u32(1);
            return (leaf_size + 2) / 2;
        }();
        leaf.insert_full(insert_index, value, left_size, new_leaf);

        // The split key and the new leaf pointer must be inserted into the parent.
        key_buffer split_key;
        derive_key(leaf.get(left_size - 1), split_key.data());

        // New leaf is to the right of the old one, leftmost can be ignored.
        if (leaf.index() == rightmost())
            set_rightmost(new_leaf.index());

        // Insert the pointer of the new leaf into the parent node (if any, else create a new root).
        // Then update all cursors so that they remain consistent.
        if (height() == 1) {
            const internal_node new_root = create_root(leaf.index(), new_leaf.index(), split_key.data());

            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;

                EXTPP_ASSERT(c.leaf.index() == leaf.index(), "Must point to the existing root.");
                EXTPP_ASSERT(c.parents.empty(), "There cannot be any internal nodes.");

                if (&c != &cursor && c.index >= insert_index)
                    c.index += 1;

                raw_btree_cursor_impl::internal_entry entry;
                entry.node = new_root;
                if (c.index >= left_size) {
                    c.leaf = new_leaf;
                    c.index -= left_size;
                    entry.index = 1;
                } else {
                    entry.index = 0;
                }
                c.parents.push_back(std::move(entry));
            }
        } else {
            // Parent is not full because of preparatory split for internal nodes.
            const internal_node& parent = cursor.parents.back().node;
            const u32 index_in_parent = cursor.parents.back().index;
            parent.insert_split_result(index_in_parent + 1, split_key.data(), new_leaf.index());

            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;

                auto& parent_entry =  c.parents.back();
                if (parent_entry.node.index() != parent.index())
                    continue;

                if (parent_entry.index == index_in_parent) {
                    EXTPP_ASSERT(c.leaf.index() == leaf.index(), "Inconsistent leaf node for parent index.");
                    EXTPP_ASSERT(parent_entry.node.get_child(index_in_parent) == leaf.index(), "Inconsistent child pointer.");

                    if (&c != &cursor && c.index >= insert_index)
                        c.index += 1;

                    if (c.index >= left_size) {
                        c.leaf = new_leaf;
                        c.index -= left_size;
                        parent_entry.index += 1;
                    }
                } else if (parent_entry.index >= index_in_parent + 1) {
                    parent_entry.index += 1;
                }
            }
        }
    }

    cursor.flags &= ~cursor.INPROGRESS;
    set_size(size() + 1);
    return true;
}

void raw_btree_impl::seek_insert_location(const byte* key, raw_btree_cursor_impl& cursor) {
    EXTPP_ASSERT(height() > 0, "Tree must not be empty at this point.");

    // For every level of internal nodes.
    block_index current = root();
    for (u32 level = height() - 1; level > 0; --level) {
        internal_node internal = read_internal(current);

        // Find the appropriate child node and push it as a stack entry. The entry might change
        // as the result of a node split in the code below.
        {
            raw_btree_cursor_impl::internal_entry entry;
            entry.node = internal;
            entry.index = lower_bound(entry.node, key);
            cursor.parents.push_back(std::move(entry));
        }

        // Split if full, then insert new internal node into the parent.
        if (internal.get_child_count() == internal.max_children()) {
            key_buffer split_key;
            internal_node new_internal = split(internal, split_key.data());

            if (cursor.parents.size() == 1) {
                // Root split
                internal_node new_root = create_root(internal.index(), new_internal.index(), split_key.data());
                apply_root_split(new_root, internal, new_internal);
            } else {
                // Split node with a parent.
                const auto& parent_entry = cursor.parents[cursor.parents.size() - 2];
                const internal_node& parent = parent_entry.node;
                const u32 index_in_parent = parent_entry.index;

                EXTPP_ASSERT(parent.get_child_count() < parent.max_children(),
                             "Parent must not be full.");
                EXTPP_ASSERT(parent.get_child(index_in_parent) == internal.index(),
                             "Parent does not point to this node at the given index");
                parent.insert_split_result(index_in_parent + 1, split_key.data(), new_internal.index());
                apply_child_split(parent, level, index_in_parent, internal, new_internal);
            }
        }

        // Update with (possibly changed) node info.
        const auto& last_entry = cursor.parents.back();
        current = last_entry.node.get_child(last_entry.index);
    }

    // Reached the leaf level. Note that the leaf node can be full at this point.
    cursor.leaf = read_leaf(current);
    cursor.index = lower_bound(cursor.leaf, key);
}

internal_node raw_btree_impl::create_root(block_index left_child, block_index right_child, const byte* split_key) {
    // Grow by one level.
    internal_node new_root = create_internal();
    new_root.set_child(0, left_child);
    new_root.set_child(1, right_child);
    new_root.set_key(0, split_key);
    new_root.set_child_count(2);

    set_height(height() + 1);
    set_root(new_root.index());

    return new_root;
}

internal_node raw_btree_impl::split(const internal_node& old_internal, byte* split_key) {
    internal_node new_internal = create_internal();
    old_internal.split(new_internal, split_key);
    return new_internal;
}

void raw_btree_impl::apply_root_split(const internal_node& new_root,
                                      const internal_node& left_internal, const internal_node& right_internal)
{
    // All children with index >= left_child_count have moved to the right node.
    const u32 left_child_count = left_internal.get_child_count();

    for (auto& cursor : m_cursors) {
        if (cursor.invalid())
            continue;

        EXTPP_ASSERT(!cursor.parents.empty(),
                     "Must have internal nodes on the stack because the root was internal.");
        EXTPP_ASSERT(cursor.parents.front().node.index() == left_internal.index(),
                     "Must point to the old root.");

        auto& root_entry = cursor.parents.front();

        raw_btree_cursor_impl::internal_entry new_entry;
        new_entry.node = new_root;

        if (root_entry.index >= left_child_count) {
            root_entry.node = right_internal;
            root_entry.index -= left_child_count;
            new_entry.index = 1;
        } else {
            new_entry.index = 0;
        }

        cursor.parents.insert(cursor.parents.begin(), std::move(new_entry));
    }
}

void raw_btree_impl::apply_child_split(const internal_node& parent,
                                       u32 left_level, u32 left_index,
                                       const internal_node& left_internal,
                                       const internal_node& right_internal)
{
    EXTPP_ASSERT(left_level > 0, "Left node must not be at leaf level.");
    EXTPP_ASSERT(left_level < height() - 1, "Left node must not be the root.");
    EXTPP_ASSERT(parent.get_child(left_index) == left_internal.index(), "Parent must point to the left node.");
    EXTPP_ASSERT(parent.get_child(left_index + 1) == right_internal.index(), "Parent must point to the right node.");

    // Examine only these indices. Lower levels might not yet be initialized for the seeking cursor.
    const u32 children_stack_index = height() - 1 - left_level;
    const u32 parent_stack_index = children_stack_index - 1;

    // All children with index >= left_child count are now in the right node.
    const u32 left_child_count = left_internal.get_child_count();
    for (auto& cursor : m_cursors) {
        if (cursor.invalid())
            continue;

        EXTPP_ASSERT(parent_stack_index < cursor.parents.size(),
                     "Parent stack index out of bounds.");
        EXTPP_ASSERT(children_stack_index >= 1 && children_stack_index < cursor.parents.size(),
                     "Children stack index out of bounds.");

        // Fixup entries of neighboring internal nodes.
        auto& parent_entry = cursor.parents[parent_stack_index];
        if (parent_entry.node.index() != parent.index())
            continue;

        if (parent_entry.index == left_index) {
            // Fixup entries of cursors in the split nodes
            auto& child_entry = cursor.parents[children_stack_index];
            EXTPP_ASSERT(parent_entry.node.get_child(parent_entry.index) == child_entry.node.index(),
                         "Parent must point to the old child.");
            EXTPP_ASSERT(child_entry.node.index() == left_internal.index(),
                         "Old child must be the left internal node.");

            if (child_entry.index >= left_child_count) {
                child_entry.node = right_internal;
                child_entry.index -= left_child_count;
                parent_entry.index += 1;
            }
        } else if (parent_entry.index > left_index) {
            parent_entry.index += 1;
        }
    }
}

// This is the 'normal' erase algorithm for btrees because the cursor already has references
// to all nodes the stack an we do not need to discover the position of the to-be-deleted value.
// We still have to respect the fact that the preparatory splits during insertion will result
// in slightly less than half full internal nodes.
void raw_btree_impl::erase(raw_btree_cursor_impl& cursor) {
    EXTPP_ASSERT(!(cursor.flags & cursor.INVALID), "Cursor must not be invalid.");
    EXTPP_ASSERT(!(cursor.flags & cursor.DELETED), "Cursor must not point to a deleted element.");
    EXTPP_ASSERT(cursor.parents.size() == this->height() - 1, "Not enough nodes on the parent stack.");
    EXTPP_ASSERT(height() > 0, "The tree cannot be empty.");

    leaf_node leaf = cursor.leaf;
    const u32 index = cursor.index;

    leaf.remove(index);
    set_size(size() - 1);

    for (auto& c : m_cursors) {
        if (c.invalid() || c.leaf.index() != leaf.index())
            continue;
        if (c.index == index)
            c.flags |= c.DELETED;
        else if (c.index > index)
            c.index -= 1;
    }

    // Handle the root leaf.
    if (cursor.parents.empty()) {
        if (leaf.get_size() == 0) {
            EXTPP_ASSERT(height() == 1, "Inconsistent tree height.");
            free_leaf(leaf.index());
            set_leftmost({});
            set_rightmost({});
            set_root({});
            set_height(0);
            for (auto& c : m_cursors) {
                if (c.invalid())
                    continue;
                c.reset_to_invalid(c.flags);
            }
        }
        return;
    }

    // Handle leftmost/rightmost leaf nodes.
    EXTPP_ASSERT(height() > 1, "We are not at the leaf level.");
    if (leaf.index() == leftmost() || leaf.index() == rightmost()) {

        // Usually empty leftmost/rightmost leaves are only deleted when they become
        // completely empty. This is an optimization for the likely case that the user
        // inserts and deletes at the end or the beginning (splitting is optimized similarily).
        if (leaf.get_size() == 0) {
            // Other nodes remain. Move cursors from this node (they are "deleted")
            // to the left/right neighbor and propagate the node erasure to the parents.
            const internal_node& parent = cursor.parents.back().node;
            u32 index_in_parent = cursor.parents.back().index;

            u32 neighbor_index;
            leaf_node neighbor;
            u32 index_in_neighbor;
            if (leaf.index() == leftmost())  {
                neighbor_index = index_in_parent + 1;
                neighbor = read_leaf(parent.get_child(neighbor_index));
                index_in_neighbor = 0;
            } else {
                neighbor_index = index_in_parent - 1;
                neighbor = read_leaf(parent.get_child(neighbor_index));
                index_in_neighbor = neighbor.get_size();
            }

            // I'm not a fan of loading the neighbor here because it means an additional I/O
            // just to load the other leaf node in order to move the cursor there.
            for (auto& c : m_cursors) {
                if (c.invalid() || c.leaf.index() != leaf.index())
                    continue;
                c.leaf = neighbor;
                c.index = index_in_neighbor;
                c.parents.back().index = neighbor_index;
            }

            if (leaf.index() == leftmost())
                set_leftmost(neighbor.index());
            else
                set_rightmost(neighbor.index());
            free_leaf(leaf.index());
            propagate_leaf_deletion(cursor, leaf.index(), index_in_parent);

        // If there are only two leaves remaining we will merge them back together if both are
        // somewhat empty. Two leaf nodes with a single element each just look too stupid.
        } else if (leaf_nodes() == 2 && size() <= leaf.max_size()) {
            const internal_node& parent = cursor.parents.back().node;
            if (leaf.index() == leftmost()) {
                leaf_node right = read_leaf(parent.get_child(1));
                merge_leaf(parent, leaf, 0, right, 1);
                free_leaf(right.index());
                propagate_leaf_deletion(cursor, right.index(), 1);
            } else {
                leaf_node left = read_leaf(parent.get_child(0));
                merge_leaf(parent, leaf, 1, left, 0);
                free_leaf(left.index());
                propagate_leaf_deletion(cursor, left.index(), 0);
            }
        }

        return;
    }

    // Handle all other leaf nodes. Leaf is not the root and not leftmost/rightmost.
    if (leaf.get_size() >= leaf.min_size())
        return;
    EXTPP_ASSERT(cursor.parents.size() > 0, "Must have parents.");
    const internal_node& parent = cursor.parents.back().node;
    const u32 index_in_parent = cursor.parents.back().index;

    // Attempt to steal entries from the right node.
    leaf_node right;
    if (index_in_parent + 1 < parent.get_child_count()) {
        right = read_leaf(parent.get_child(index_in_parent + 1));
        if (right.get_size() > right.min_size() || (right.index() == rightmost() && right.get_size() > 1))
        {
            steal_leaf_entry(parent, leaf, index_in_parent, right, index_in_parent + 1);
            return;
        }
    }

    // Attempt to steal entries from the left node.
    leaf_node left;
    if (index_in_parent > 0) {
        left = read_leaf(parent.get_child(index_in_parent - 1));
        if ((left.get_size() > left.min_size() || (left.index() == leftmost() && left.get_size() > 1)))
        {
            steal_leaf_entry(parent, leaf, index_in_parent, left, index_in_parent - 1);
            return;
        }
    }

    // Merge with one of the leaves.
    block_index child_node;
    u32 child_node_index;
    if (right.valid()) {
        merge_leaf(parent, leaf, index_in_parent, right, index_in_parent + 1);
        child_node = right.index();
        child_node_index = index_in_parent + 1;
    } else if (left.valid()) {
        merge_leaf(parent, leaf, index_in_parent, left, index_in_parent - 1);
        child_node = left.index();
        child_node_index = index_in_parent - 1;
    } else {
        EXTPP_UNREACHABLE("Must have a left or right neighbor.");
    }

    free_leaf(child_node);
    propagate_leaf_deletion(cursor, child_node, child_node_index);
}

// Called when the leaf node `child_node` has been merged with a neighbor and now has to be removed from its parent.
// The internal nodes up the stack might have to be merged as well.
void raw_btree_impl::propagate_leaf_deletion(raw_btree_cursor_impl& cursor, block_index child_node, u32 child_node_index) {
    EXTPP_ASSERT(!cursor.parents.empty(), "There must be internal node parents.");

    // Walk up the stack and merge nodes if necessary.
    u32 stack_index = cursor.parents.size() - 1;
    internal_node node = cursor.parents[stack_index].node;
    while (1) {
        EXTPP_ASSERT(cursor.parents.at(stack_index).node.index() == node.index(),
                     "Internal node must be at that level.");
        EXTPP_ASSERT(node.get_child(child_node_index) == child_node,
                     "Parent must point to the child.");
        EXTPP_ASSERT(node.get_child_count() >= 2, "Node is too empty.");

        // Remove the child and shift cursors to the left.
        node.remove_child(child_node_index);
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& entry = c.parents[stack_index];
            if (entry.node.index() == node.index()) {
                EXTPP_ASSERT(entry.index != child_node_index,
                             "Nobody must point to the deleted child.");
                if (entry.index > child_node_index)
                    entry.index -= 1;
            }
        }

        // We're at the root. If it becomes too empty, shrink the tree by one level.
        if (stack_index == 0) {
            EXTPP_ASSERT(node.index() == root(), "Must be the root.");
            if (node.get_child_count() == 1) {
                set_root(node.get_child(0));
                set_height(height() - 1);
                free_internal(node.index());

                for (auto& c : m_cursors) {
                    if (c.invalid())
                        continue;
                    EXTPP_ASSERT(!c.parents.empty() && c.parents[0].node.index() == node.index(),
                            "All cursors must point to the root.");
                    c.parents.erase(c.parents.begin());
                }
            }
            break;
        }

        // Not the root. We must have a minimum child count (or merge)
        // and we have a parent node.
        if (node.get_child_count() >= node.min_children())
            break;

        internal_node parent = cursor.parents[stack_index - 1].node;
        u32 index_in_parent = cursor.parents[stack_index - 1].index;

        // Attempt to steal a (key, child) pair from the right node.
        internal_node right;
        if (index_in_parent + 1 < parent.get_child_count()) {
            right = read_internal(parent.get_child(index_in_parent + 1));
            if (right.get_child_count() > right.min_children()) {
                steal_internal_entry(parent, stack_index, node, index_in_parent, right, index_in_parent + 1);
                break;
            }
        }

        // Attempt to steal a (key, child) pair from the left node.
        internal_node left;
        if (index_in_parent > 0) {
            left = read_internal(parent.get_child(index_in_parent - 1));
            if (left.get_child_count() > left.min_children()) {
                steal_internal_entry(parent, stack_index, node, index_in_parent, left, index_in_parent - 1);
                break;
            }
        }

        // Merge with one of the neighbors.
        if (left.valid()) {
            merge_internal(parent, stack_index, node, index_in_parent, left, index_in_parent - 1);
            child_node = left.index();
            child_node_index = index_in_parent - 1;
        } else if (right.valid()) {
            merge_internal(parent, stack_index, node, index_in_parent, right, index_in_parent + 1);
            child_node = right.index();
            child_node_index = index_in_parent + 1;
        } else {
            EXTPP_UNREACHABLE("Must have a left or a right neighbor.");
        }

        free_internal(child_node);
        node = std::move(parent);
        stack_index -= 1;
    }
}

void raw_btree_impl::steal_leaf_entry(const internal_node& parent,
                                      const leaf_node& leaf, u32 leaf_index,
                                      const leaf_node& neighbor, u32 neighbor_index)
{
    const u32 parent_children = parent.get_child_count();
    const u32 leaf_size = leaf.get_size();
    const u32 neighbor_size = neighbor.get_size();

    EXTPP_ASSERT(neighbor_index < parent.get_child_count(),
                 "Neighbor index out of bounds.");
    EXTPP_ASSERT(leaf_index != 0 || neighbor_index == 1,
                 "Invalid neighbor index for first child.");
    EXTPP_ASSERT(leaf_index != parent_children - 1 || neighbor_index == parent_children - 2,
                 "Invalid neighbor index for last child.");
    EXTPP_ASSERT(parent.get_child(leaf_index) == leaf.index(), "Leaf index wrong.");
    EXTPP_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");
    EXTPP_ASSERT(neighbor_size > 1, "At least one value must remain after stealing one.");

    if (leaf_index < parent_children - 1 && neighbor_index == leaf_index + 1) {
        // Move elements
        leaf.insert_nonfull(leaf_size, neighbor.get(0));
        neighbor.remove(0);

        // Update max key of this node in parent.
        key_buffer key;
        derive_key(leaf.get(leaf_size), key.data());
        parent.set_key(leaf_index, key.data());

        // Rewrite all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid() || c.leaf.index() != neighbor.index())
                continue;
            if (c.index == 0) {
                c.leaf = leaf;
                c.index = leaf_size;
                c.parents.back().index -= 1;
            } else {
                c.index -= 1;
            }
        }
    } else if (leaf_index > 0 && neighbor_index == leaf_index - 1) {
        // Move elements
        leaf.insert_nonfull(0, neighbor.get(neighbor_size - 1));
        neighbor.remove(neighbor_size - 1);

        // Update max key of the neighbor node in parent.
        key_buffer key;
        derive_key(neighbor.get(neighbor_size - 2), key.data());
        parent.set_key(leaf_index - 1, key.data());

        // Rewrite all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            if (c.leaf.index() == leaf.index()) {
                c.index += 1;
            } else if (c.leaf.index() == neighbor.index()) {
                if (c.index >= neighbor_size - 1) {
                    c.leaf = leaf;
                    c.index -= neighbor_size - 1;
                    c.parents.back().index += 1;
                }
            }
        }
    } else {
        EXTPP_UNREACHABLE("Not a neighbor node.");
    }
}

// Note: stack_index is the index of the children (node and neighbor).
void raw_btree_impl::steal_internal_entry(const internal_node& parent, u32 stack_index,
                                          const internal_node& node, u32 node_index,
                                          const internal_node& neighbor, u32 neighbor_index)
{
    const u32 parent_children = parent.get_child_count();
    const u32 node_children = node.get_child_count();
    const u32 neighbor_children = neighbor.get_child_count();

    EXTPP_ASSERT(stack_index > 0, "Cannot steal from the root.");
    EXTPP_ASSERT(neighbor_index < parent_children,
                 "Neighbor index out of bounds.");
    EXTPP_ASSERT(node_index != 0 || neighbor_index == 1,
                 "Invalid neighbor index for first child.");
    EXTPP_ASSERT(node_index != parent_children - 1 || neighbor_index == parent_children - 2,
                 "Invalid neighbor index for last child.");
    EXTPP_ASSERT(parent.get_child(node_index) == node.index(), "Node index wrong.");
    EXTPP_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");
    EXTPP_ASSERT(neighbor_children > neighbor.min_children(), "Enough children must remain after stealing one.");

    if (node_index < parent_children - 1 && neighbor_index == node_index + 1) {
        // Taking from the right neighbor. We have to update our own key after this op.
        node.append_entry(parent.get_key(node_index), neighbor.get_child(0));
        parent.set_key(node_index, neighbor.get_key(0));
        neighbor.remove_child(0);

        // Update cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.parents[stack_index - 1];
            auto& entry = c.parents[stack_index];
            if (entry.node.index() != neighbor.index())
                continue;

            if (entry.index == 0) {
                entry.node = node;
                entry.index = node_children;
                parent_entry.index -= 1;
            } else {
                entry.index -= 1;
            }
        }
    } else if (node_index > 0 && neighbor_index == node_index - 1) {
        // Taking from the left neighbor. The appropriate key (the max) is stored
        // in the parent node and needs to be taken + replaced.
        node.prepend_entry(parent.get_key(neighbor_index), neighbor.get_child(neighbor_children - 1));
        parent.set_key(neighbor_index, neighbor.get_key(neighbor_children - 2));
        neighbor.remove_child(neighbor_children - 1);

        // Update cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.parents[stack_index - 1];
            auto& entry = c.parents[stack_index];
            if (entry.node.index() == node.index()) {
                entry.index += 1;
            } else if (entry.node.index() == neighbor.index()) {
                if (entry.index >= neighbor_children - 1) {
                    entry.node = node;
                    entry.index -= neighbor_children - 1;
                    parent_entry.index += 1;
                }
            }
        }
    } else {
        EXTPP_UNREACHABLE("Not a neighbor node.");
    }
}

void raw_btree_impl::merge_leaf(const internal_node& parent,
                                const leaf_node& leaf, u32 leaf_index,
                                const leaf_node& neighbor, u32 neighbor_index) {
    const u32 parent_children = parent.get_child_count();
    const u32 leaf_size = leaf.get_size();
    const u32 neighbor_size = neighbor.get_size();

    EXTPP_ASSERT(neighbor_index < parent.get_child_count(),
                 "Neighbor index out of bounds.");
    EXTPP_ASSERT(leaf_index != 0 || neighbor_index == 1,
                 "Invalid neighbor index for first child.");
    EXTPP_ASSERT(leaf_index != parent_children - 1 || neighbor_index == parent_children - 2,
                 "Invalid neighbor index for last child.");
    EXTPP_ASSERT(parent.get_child(leaf_index) == leaf.index(), "Leaf index wrong.");
    EXTPP_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");

    if (leaf_index < parent_children - 1 && neighbor_index == leaf_index + 1) {
        // Merge with the node to the right.
        leaf.append_from_right(neighbor);
        if (rightmost() == neighbor.index())
            set_rightmost(leaf.index());

        // Update the key since the leaf's max value changed.
        if (neighbor_index != parent_children - 1) {
            key_buffer key;
            derive_key(leaf.get(leaf_size + neighbor_size - 1), key.data());
            parent.set_key(leaf_index, key.data());
        }

        for (auto& c : m_cursors) {
            if (c.invalid() || c.leaf.index() != neighbor.index())
                continue;

            c.leaf = leaf;
            c.index += leaf_size;
            c.parents.back().index -= 1;
        }
    } else if (leaf_index > 0 && neighbor_index == leaf_index - 1) {
        // Merge with the node to the left.
        leaf.prepend_from_left(neighbor);
        if (leftmost() == neighbor.index())
            set_leftmost(leaf.index());

        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            if (c.leaf.index() == leaf.index()) {
                c.index += neighbor_size;
            } else if (c.leaf.index() == neighbor.index()) {
                c.leaf = leaf;
                c.parents.back().index += 1;
            }
        }
    } else {
        EXTPP_UNREACHABLE("Not a neighbor node.");
    }
}

// Note: stack_index is the index of the children (node and neighbor).
void raw_btree_impl::merge_internal(const internal_node& parent, u32 stack_index,
                                    const internal_node& node, u32 node_index,
                                    const internal_node& neighbor, u32 neighbor_index)
{
    const u32 parent_children = parent.get_child_count();
    const u32 node_children = node.get_child_count();
    const u32 neighbor_children = neighbor.get_child_count();

    EXTPP_ASSERT(stack_index > 0, "Cannot steal from the root.");
    EXTPP_ASSERT(neighbor_index < parent_children, "Neighbor index out of bounds.");
    EXTPP_ASSERT(node_index != 0 || neighbor_index == 1, "Invalid neighbor index for first child.");
    EXTPP_ASSERT(node_index != parent_children - 1 || neighbor_index == parent_children - 2,
                 "Invalid neighbor index for last child.");
    EXTPP_ASSERT(parent.get_child(node_index) == node.index(), "Node index wrong.");
    EXTPP_ASSERT(parent.get_child(neighbor_index) == neighbor.index(), "Neighbor index wrong.");

    if (node_index < parent_children - 1 && neighbor_index == node_index + 1) {
        // Merge with the right neighbor.
        node.append_from_right(parent.get_key(node_index), neighbor);

        if (neighbor_index != parent_children - 1) {
            parent.set_key(node_index, parent.get_key(neighbor_index));
        }

        // Update all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.parents[stack_index - 1];
            auto& entry = c.parents[stack_index];
            if (entry.node.index() == neighbor.index()) {
                entry.node = node;
                entry.index += node_children;
                parent_entry.index -= 1;
            }
        }

    } else if (node_index > 0 && neighbor_index == node_index - 1) {
        // Merge with the left neighbor.
        node.prepend_from_left(parent.get_key(neighbor_index), neighbor);

        // Update all cursors.
        for (auto& c : m_cursors) {
            if (c.invalid())
                continue;

            auto& parent_entry = c.parents[stack_index - 1];
            auto& entry = c.parents[stack_index];
            if (entry.node.index() == neighbor.index()) {
                entry.node = node;
                parent_entry.index += 1;
            } else if (entry.node.index() == node.index()) {
                entry.index += neighbor_children;
            }
        }
    } else {
        EXTPP_UNREACHABLE("Not a neighbor node.");
    }
}

void raw_btree_impl::clear() {
    if (empty())
        return;

    // Invalidate all cursors first.
    for (auto& c : m_cursors) {
        if (c.invalid())
            continue;

        c.reset_to_invalid(c.DELETED);
    }

    const block_index old_root = root();
    const u32 old_height = height();

    set_root({});
    set_leftmost({});
    set_rightmost({});
    set_height(0);
    set_size(0);
    set_internal_nodes(0);
    set_leaf_nodes(0);

    detail::fix visit = [&](auto& self, block_index index, u32 level) -> void {
        if (level > 0) {
            internal_node node = read_internal(index);

            level -= 1;
            const u32 child_count = node.get_child_count();
            for (u32 i = 0; i < child_count; ++i)
                self(node.get_child(i), level);
        }

        get_allocator().free(index, 1);
    };

    if (old_height >= 1) {
        visit(old_root, old_height - 1);
    } else {
        EXTPP_UNREACHABLE("Invalid height for nonempty tree.");
    }
}

void raw_btree_impl::clear_subtree(block_index index, u32 level) {
    if (level > 0) {
        internal_node node = read_internal(index);

        level -= 1;
        const u32 child_count = node.get_child_count();
        for (u32 i = 0; i < child_count; ++i)
            clear_subtree(node.get_child(i), level);
        free_internal(index);
    } else {
        free_leaf(index);
    }
}

std::unique_ptr<raw_btree_loader_impl> raw_btree_impl::bulk_load()
{
    if (!empty())
        EXTPP_THROW(bad_operation("Tree must be empty."));

    return std::make_unique<raw_btree_loader_impl>(*this);
}

std::unique_ptr<raw_btree_cursor_impl> raw_btree_impl::create_cursor(raw_btree::cursor_seek_t seek) {
    auto c = std::make_unique<raw_btree_cursor_impl>(this);

    switch (seek) {
    case raw_btree::seek_none:
        break;
    case raw_btree::seek_min:
        c->move_min();
        break;
    case raw_btree::seek_max:
        c->move_max();
        break;
    default:
        EXTPP_THROW(bad_argument("Invalid seek value"));
    }

    return c;
}

template<typename Func>
void raw_btree_impl::visit_nodes(Func&& fn) const {
    if (height() == 0)
        return;

    detail::fix recurse = [&](auto& self, u32 level, block_index parent, block_index current) -> bool {
        if (level == 0)
            return fn(parent, level, read_leaf(current));

        internal_node node = read_internal(current);
        const u32 children = node.get_child_count();
        if (!fn(parent, level, node))
            return false;

        for (u32 i = 0; i < children; ++i) {
            if (!self(level - 1, node.index(), node.get_child(i)))
                return false;
        }
        return true;
    };
    recurse(height() - 1, block_index(), root());
}

void raw_btree_impl::dump(std::ostream& os) const {
    fmt::print(os,
       "Raw btree:\n"
       "  Value size: {}\n"
       "  Key size: {}\n"
       "  Internal node capacity: {}\n"
       "  Leaf node capacity: {}\n"
       "  Height: {}\n"
       "  Size: {}\n"
       "  Internal nodes: {}\n"
       "  Leaf nodes: {}\n",
       value_size(), key_size(),
       internal_node_max_children(), leaf_node_max_values(),
       height(), size(), internal_nodes(), leaf_nodes());

    if (!empty())
        os << "\n";

    struct visitor {
        std::ostream& os;
        visitor(std::ostream& os): os(os) {}

        bool operator()(block_index parent, u32 level, const internal_node& node) const {
            const u32 child_count = node.get_child_count();
            EXTPP_ASSERT(child_count > 0, "Invalid child count.");

            fmt::print(os,
                       "Internal node @{}:\n"
                       "  Parent: @{}\n"
                       "  Level: {}\n"
                       "  Children: {}\n",
                       node.index(), parent, level, child_count);

            for (u32 i = 0; i < child_count - 1; ++i) {
                fmt::print(os, "  {}: @{} (<= {})\n",
                           i, node.get_child(i),
                           format_hex(node.get_key(i), node.key_size()));
            }
            fmt::print(os, "  {}: @{}\n", child_count - 1, node.get_child(child_count - 1));
            return true;
        }

        bool operator()(block_index parent, u32 level, const leaf_node& leaf) const {
            unused(level);

            const u32 size = leaf.get_size();
            fmt::print(os,
                       "Leaf node @{}:\n"
                       "  Parent: @{}\n"
                       "  Values: {}\n",
                       leaf.index(), parent, size);
            for (u32 i = 0; i < size; ++i) {
                fmt::print(os, "  {}: {}\n", i, format_hex(leaf.get(i), leaf.value_size()));
            }
            return true;
        }
    };
    visit_nodes(visitor(os));
}

void raw_btree_impl::visit(bool (*visit_fn)(const raw_btree::node_view& node, void* user_data),
                           void* user_data) const
{
    if (!visit_fn)
        EXTPP_THROW(bad_argument("Invalid visitation function."));

    struct node_view_impl : raw_btree::node_view {
        virtual bool is_leaf() const { return m_level == 0; }
        virtual bool is_internal() const { return !is_leaf(); }

        virtual u32 level() const { return m_level; }
        virtual block_index address() const { return m_address; }
        virtual block_index parent_address() const { return m_parent; }

        virtual u32 child_count() const { return check_internal().get_child_count(); }
        virtual u32 key_count() const  { return check_internal().get_child_count() - 1; }

        virtual const byte* key(u32 index) const {
            const internal_node& node = check_internal();
            if (index >= node.get_child_count() - 1)
                EXTPP_THROW(bad_argument("Key index out of bounds."));
            return node.get_key(index);
        }

        virtual block_index child(u32 index) const {
            const internal_node& node = check_internal();
            if (index >= node.get_child_count())
                EXTPP_THROW(bad_argument("Child index out of bounds."));
            return node.get_child(index);
        }

        // For leaf nodes.
        virtual u32 value_count() const { return check_leaf().get_size(); }

        virtual const byte* value(u32 index) const {
            const leaf_node& node = check_leaf();
            if (index >= node.get_size())
                EXTPP_THROW(bad_argument("Value index out of bounds."));
            return node.get(index);
        }

        const internal_node& check_internal() const {
            auto ptr = std::get_if<internal_node>(&m_node);
            if (!ptr)
                EXTPP_THROW(bad_argument("Not an internal node."));
            return *ptr;
        }

        const leaf_node& check_leaf() const {
            auto ptr = std::get_if<leaf_node>(&m_node);
            if (!ptr)
                EXTPP_THROW(bad_argument("Not a leaf node."));
            return *ptr;
        }


        u32 m_level = 0;
        block_index m_parent;
        block_index m_address;
        std::variant<leaf_node, internal_node> m_node;
    };

    struct visitor_t {
        node_view_impl view;
        bool (*visit_fn)(const raw_btree::node_view& node, void* user_data) = nullptr;
        void* user_data = nullptr;

        bool operator()(block_index parent, u32 level, const internal_node& internal) {
            view.m_level = level;
            view.m_parent = parent;
            view.m_address = internal.index();
            view.m_node = internal;
            return visit_fn(view, user_data);
        }

        bool operator()(block_index parent, u32 level, const leaf_node& leaf) {
            view.m_level = level;
            view.m_parent = parent;
            view.m_address = leaf.index();
            view.m_node = leaf;
            return visit_fn(view, user_data);
        }
    } visitor;
    visitor.visit_fn = visit_fn;
    visitor.user_data = user_data;
    visit_nodes(visitor);
}

void raw_btree_impl::validate() const {
#define ERROR(...) EXTPP_THROW( \
    corruption_error(fmt::format("validate: " __VA_ARGS__)));

    struct context {
        u32 level = 0;
        block_index parent;
        const byte* lower_key = nullptr; // If present: Values or keys must be greater
        const byte* upper_key = nullptr; // If present: Value or keys must be greater or equal
    };

    struct checker {
        const raw_btree_impl* tree;

        const u32 min_values = tree->m_leaf_capacity / 2;
        const u32 max_values = tree->m_leaf_capacity;
        const u32 min_children = tree->m_internal_max_children / 2;
        const u32 max_children = tree->m_internal_max_children;

        u64 seen_values = 0;
        u64 seen_leaf_nodes = 0;
        u64 seen_internal_nodes = 0;

        checker(const raw_btree_impl* tree): tree(tree) {}

        void check_key(const context& ctx, const byte* key) {
            if (ctx.lower_key && !tree->key_greater(key, ctx.lower_key))
                ERROR("Key is not greater than the lower bound.");
            if (ctx.upper_key && tree->key_less(ctx.upper_key, key))
                ERROR("Key is greater than the upper bound.");
        };

        void check_leaf(const context& ctx, const leaf_node& leaf) {
            if (!ctx.lower_key && tree->leftmost() != leaf.index()) {
                ERROR("Only the leftmost leaf can have an unbounded lower key.");
            }
            if (tree->leftmost() == leaf.index() && seen_leaf_nodes != 0) {
                ERROR("The leftmost leaf must be visited first.");
            }
            if (seen_leaf_nodes == tree->leaf_nodes() - 1 && tree->rightmost() != leaf.index()) {
                ERROR("Expected the rightmost leaf at this index.");
            }

            const u32 size = leaf.get_size();
            if (size == 0) {
                ERROR("Empty leaf.");
            }
            if (size < min_values
                    && leaf.index() != tree->root()
                    && leaf.index() != tree->leftmost()
                    && leaf.index() != tree->rightmost()) {
                ERROR("Leaf is underflowing.");
            }
            if (size > max_values) {
                ERROR("Leaf is overflowing.");
            }

            for (u32 i = 0; i < size; ++i) {
                key_buffer key;
                tree->derive_key(leaf.get(i), key.data());
                check_key(ctx, key.data());

                if (i > 0) {
                    key_buffer prev;
                    tree->derive_key(leaf.get(i - 1), prev.data());
                    if (!tree->key_less(prev.data(), key.data())) {
                        ERROR("Leaf entries are not sorted.");
                    }
                }
            }

            seen_leaf_nodes += 1;
            seen_values += size;
        };

        void check_internal(const context& ctx, const internal_node& node) {
            const u32 child_count = node.get_child_count();
            if (child_count < min_children && node.index() != tree->root()) {
                ERROR("Internal node is underflowing.");
            }
            if (child_count < 2 && node.index() != tree->root()) {
                ERROR("Root is too empty.");
            }
            if (child_count > max_children) {
                ERROR("Internal node is overflowing.");
            }

            check_key(ctx, node.get_key(0));

            context child_ctx;
            child_ctx.parent = node.index();
            child_ctx.level = ctx.level - 1;
            child_ctx.lower_key = ctx.lower_key;
            child_ctx.upper_key = node.get_key(0);
            check_key(child_ctx, node.get_key(0));
            check(child_ctx, node.get_child(0));

            for (u32 i = 1; i < child_count - 1; ++i) {
                check_key(ctx, node.get_key(i));
                if (!tree->key_less(node.get_key(i - 1), node.get_key(i))) {
                    ERROR("Internal node entries are not sorted.");
                }

                child_ctx.lower_key = node.get_key(i - 1);
                child_ctx.upper_key = node.get_key(i);
                check(child_ctx, node.get_child(i));
            }

            child_ctx.lower_key = node.get_key(child_count - 2);
            child_ctx.upper_key = ctx.upper_key;
            check(child_ctx, node.get_child(child_count - 1));

            seen_internal_nodes += 1;
        };

        void check(const context& ctx, block_index node_index) {
            if (ctx.level == 0) {
                check_leaf(ctx, tree->read_leaf(node_index));
            } else {
                check_internal(ctx, tree->read_internal(node_index));
            }
        }

        void run() {
            if (tree->height() != 0) {
                if (!tree->root()) {
                    ERROR("Non-empty tree does not have a root.");
                }

                context ctx;
                ctx.level = tree->height() - 1;
                check(ctx, tree->root());
            }

            if (seen_values != tree->size()) {
                ERROR("Value count does not match the tree's size.");
            }
            if (seen_leaf_nodes != tree->leaf_nodes()) {
                ERROR("Leaf node count does not match the tree's state.");
            }
            if (seen_internal_nodes != tree->internal_nodes()) {
                ERROR("internal node count does not match the tree's state.");
            }
        }
    };

    checker(this).run();

#undef ERROR
}

void raw_btree_impl::free_leaf(block_index leaf) {
    EXTPP_ASSERT(leaf_nodes() > 0, "Invalid state");
    get_allocator().free(leaf, 1);
    set_leaf_nodes(leaf_nodes() - 1);
}

void raw_btree_impl::free_internal(block_index internal) {
    EXTPP_ASSERT(internal_nodes() > 0, "Invalid state");
    get_allocator().free(internal, 1);
    set_internal_nodes(internal_nodes() - 1);
}

leaf_node raw_btree_impl::create_leaf() {
    auto index = get_allocator().allocate(1);
    set_leaf_nodes(leaf_nodes() + 1);

    auto block = get_engine().zeroed(index);
    auto node = leaf_node(std::move(block), value_size(), m_leaf_capacity);
    node.init();
    return node;
}

internal_node raw_btree_impl::create_internal() {
    auto index = get_allocator().allocate(1);
    set_internal_nodes(internal_nodes() + 1);

    auto block = get_engine().zeroed(index);
    auto node = internal_node(std::move(block), key_size(), m_internal_max_children);
    node.init();
    return node;
}

leaf_node raw_btree_impl::as_leaf(block_handle handle) const {
    return leaf_node(std::move(handle), value_size(), m_leaf_capacity);
}

internal_node raw_btree_impl::as_internal(block_handle handle) const {
    return internal_node(std::move(handle), key_size(), m_internal_max_children);
}

leaf_node raw_btree_impl::read_leaf(block_index index) const {
    return as_leaf(get_engine().read(index));
}

internal_node raw_btree_impl::read_internal(block_index index) const {
    return as_internal(get_engine().read(index));
}

// --------------------------------
//
//   Cursor implementation
//
// --------------------------------

raw_btree_cursor_impl::raw_btree_cursor_impl(raw_btree_impl* tree)
    : tree(tree)
{
    if (tree)
        tree->link_cursor(this);
    reset_to_invalid();
}

raw_btree_cursor_impl::~raw_btree_cursor_impl() {
    if (tree && cursors.is_linked())
        tree->unlink_cursor(this);
}

void raw_btree_cursor_impl::copy(const raw_btree_cursor_impl& other) {
    if (this == &other)
        return;

    EXTPP_ASSERT(tree == other.tree, "Cursors must belong to the same tree.");
    parents = other.parents;
    leaf = other.leaf;
    index = other.index;
    flags = other.flags;
}

void raw_btree_cursor_impl::check_tree_valid() const {
    if (!tree)
        EXTPP_THROW(bad_cursor("the cursor's tree instance has been destroyed"));
}

void raw_btree_cursor_impl::check_element_valid() const {
    check_tree_valid();
    if (flags & raw_btree_cursor_impl::INPROGRESS)
        EXTPP_THROW(bad_cursor("leak of in-progress cursor."));
    if (flags & raw_btree_cursor_impl::DELETED)
        EXTPP_THROW(bad_cursor("cursor points to deleted element"));
    if (flags & raw_btree_cursor_impl::INVALID)
        EXTPP_THROW(bad_cursor("bad cursor"));

#ifdef EXTPP_DEBUG
    {
        const u32 height = tree->height();
        EXTPP_ASSERT(parents.size() + 1 == height,
                     "Cursor does not have enough nodes on the stack.");
        for (u32 i = 0; i < parents.size(); ++i) {
            auto& entry = parents[i];
            EXTPP_ASSERT(entry.node.valid(),
                         "Invalid node on the cursor's stack.");
            EXTPP_ASSERT(entry.index < entry.node.get_child_count(),
                         "Child index out of bounds.");

            if (i > 0) {
                // Must be at the specified index in its parent.
                auto& parent_entry = parents[i - 1];
                EXTPP_ASSERT(parent_entry.node.get_child(parent_entry.index) == entry.node.index(),
                             "Must be at that position in the parent.");
            }
        }


        if (height > 1) {
            // Must be at the specified index in its parent.
            auto& parent_entry = parents.back();
            EXTPP_ASSERT(parent_entry.node.get_child(parent_entry.index) == leaf.index(),
                         "Must be at that position in the parent.");
        }

        if (height == 1) {
            EXTPP_ASSERT(leaf.index() == tree->root(), "Leaf must be the root");
        } else if (height > 1) {
            EXTPP_ASSERT(parents.front().node.index() == tree->root(), "First parent must be the root.");
        } else {
            EXTPP_ASSERT(false, "The tree is empty...");
        }

        EXTPP_ASSERT(leaf.valid(), "Leaf must be valid.");
        EXTPP_ASSERT(index < leaf.get_size(), "Invalid index in leaf.");
    }
#endif
}

u32 raw_btree_cursor_impl::value_size() const {
    check_tree_valid();
    return tree->value_size();
}

u32 raw_btree_cursor_impl::key_size() const {
    check_tree_valid();
    return tree->key_size();
}

bool raw_btree_cursor_impl::at_end() const {
    return !erased() && (flags & INVALID);
}

bool raw_btree_cursor_impl::erased() const {
    return flags & DELETED;
}

template<bool max>
void raw_btree_cursor_impl::init_position() {
    check_tree_valid();
    reset_to_zero();

    const u32 height = tree->height();
    if (height == 0) {
        flags |= INVALID;
        return;
    }

    parents.reserve(height - 1);
    flags |= INPROGRESS;

    block_index current = tree->root();
    for (u32 level = height - 1; level > 0; --level) {
        internal_node node = tree->read_internal(current);

        u32 child_index;
        if constexpr (max) {
            child_index = node.get_child_count() - 1;
        } else {
            child_index = 0;
        }

        block_index child = node.get_child(child_index);
        parents.push_back({std::move(node), child_index});
        current = child;
    }

    leaf = tree->read_leaf(current);
    EXTPP_ASSERT(leaf.get_size() > 0, "Leaf cannot be empty.");

    if constexpr (max) {
        index = leaf.get_size() - 1;
    } else {
        index = 0;
    }

    flags &= ~INPROGRESS;
}

bool raw_btree_cursor_impl::move_min() {
    init_position<false>();
    return !at_end();
}

bool raw_btree_cursor_impl::move_max() {
    init_position<true>();
    return !at_end();
}

bool raw_btree_cursor_impl::move_prev() {
    check_tree_valid();

    if (flags & DELETED) {
        flags &= ~DELETED;
        if (flags & INVALID)
            return false;
    } else if (flags & INVALID) {
        EXTPP_THROW(bad_cursor("bad cursor"));
    }


    if (index > 0) {
        --index;
        return true;
    }

    // Find a parent that is not yet at index 0.
    auto rpos = std::find_if(parents.rbegin(), parents.rend(), [&](const internal_entry& entry) {
        return entry.index > 0;
    });
    if (rpos == parents.rend()) {
        reset_to_invalid();
        return false;
    }

    // Decrement the index in this parent node, then walk to the leaf level and set all indices to size - 1.
    // Note that rpos.base() is the element *after* the parent.
    flags |= INPROGRESS;
    auto pos = std::prev(rpos.base());
    pos->index -= 1;
    for (auto child_pos = pos + 1; child_pos != parents.end(); ++child_pos, ++pos) {
        child_pos->node = tree->read_internal(pos->node.get_child(pos->index));
        child_pos->index = child_pos->node.get_child_count() - 1;
    }

    leaf = tree->read_leaf(pos->node.get_child(pos->index));
    EXTPP_ASSERT(leaf.get_size() > 0, "Leaf cannot be empty.");
    index = leaf.get_size() - 1;
    flags &= ~INPROGRESS;
    return true;
}

bool raw_btree_cursor_impl::move_next() {
    check_tree_valid();

    if (flags & DELETED) {
        flags &= ~DELETED;
        if (flags & INVALID)
            return false;
    } else if (flags & INVALID) {
        EXTPP_THROW(bad_cursor("bad cursor"));
    } else {
        ++index;
    }

    if (index < leaf.get_size()) {
        return true;
    }

    // Find a parent that is not yet at it's last index.
    auto rpos = std::find_if(parents.rbegin(), parents.rend(), [&](const internal_entry& entry) {
        return entry.index + 1 < entry.node.get_child_count();
    });
    if (rpos == parents.rend()) {
        reset_to_invalid();
        return false;
    }

    // Increment the index in this parent node, then walk to the leaf level and set all indices to 0.
    // Note that rpos.base() is the element *after* the parent.
    flags |= INPROGRESS;
    auto pos = std::prev(rpos.base());
    pos->index++;
    for (auto child_pos = pos + 1; child_pos != parents.end(); ++child_pos, ++pos) {
        child_pos->node = tree->read_internal(pos->node.get_child(pos->index));
        child_pos->index = 0;
    }

    leaf = tree->read_leaf(pos->node.get_child(pos->index));
    EXTPP_ASSERT(leaf.get_size() > 0, "Leaf cannot be empty.");
    index = 0;
    flags &= ~INPROGRESS;
    return true;
}

bool raw_btree_cursor_impl::lower_bound(const byte* key) {
    check_tree_valid();
    tree->lower_bound(key, *this);
    return !at_end();
}

bool raw_btree_cursor_impl::upper_bound(const byte* key) {
    check_tree_valid();
    tree->upper_bound(key, *this);
    return !at_end();
}

bool raw_btree_cursor_impl::find(const byte* key) {
    check_tree_valid();
    tree->find(key, *this);
    return !at_end();
}

bool raw_btree_cursor_impl::insert(const byte* value, bool overwrite) {
    check_tree_valid();
    bool inserted = tree->insert(value, *this);
    if (!inserted && overwrite) {
        leaf.set(index, value);
    }
    return inserted;
}

void raw_btree_cursor_impl::erase() {
    check_element_valid();
    tree->erase(*this);
}

const byte* raw_btree_cursor_impl::get() const {
    check_element_valid();
    return leaf.get(index);
}

void raw_btree_cursor_impl::set(const byte* value) {
    EXTPP_ASSERT(value, "Nullpointer instead of a value.");
    check_element_valid();

    key_buffer k1, k2;
    tree->derive_key(get(), k1.data());
    tree->derive_key(value, k2.data());
    if (std::memcmp(k1.data(), k2.data(), tree->key_size()) != 0) {
        EXTPP_THROW(bad_argument("The key derived from the new value differs from the old key."));
    }

    leaf.set(index, value);
}

void raw_btree_cursor_impl::validate() const {
#define BAD(...) EXTPP_THROW(bad_cursor(__VA_ARGS__))

// TODO

#undef BAD
}

bool raw_btree_cursor_impl::operator==(const raw_btree_cursor_impl& other) const {
    if (tree != other.tree)
        return false;
    if (at_end() != other.at_end())
        return false;
    if (erased() != other.erased())
        return false;

    if (at_end())
        return true;
    return leaf.index() == other.leaf.index() && index == other.index;
}


// --------------------------------
//
//   BTree Loader
//
// --------------------------------

// Future: Implement bulk loading for non-emtpy trees (i.e. all keys must be > max).
// TODO: Exception safety, discard()
class raw_btree_loader_impl {
public:
    raw_btree_loader_impl(raw_btree_impl& tree);

    raw_btree_loader_impl(const raw_btree_loader_impl&) = delete;
    raw_btree_loader_impl& operator=(const raw_btree_loader_impl&) = delete;

    void insert(const byte* values, size_t count);

    void finish();

    void discard();

private:
    // Every node is represented by its max key and its pointer.
    // Enough memory for keys and indices for (children + min_children) children.
    // This scheme ensures that we never emit internal nodes that are too empty.
    // Note that we can emit *leaf*-nodes that are too empty because the tree
    // already has a special case for them.
    struct proto_internal_node {
        std::vector<byte> keys;
        std::vector<block_index> children;
        u32 size = 0;
        u32 capacity = 0;
    };

    proto_internal_node make_internal_node() {
        proto_internal_node node;
        node.capacity = m_internal_max_children + m_internal_min_children;
        node.keys.resize(node.capacity * m_key_size);
        node.children.resize(node.capacity);
        return node;
    }

    void insert_child(size_t index, const byte* key, block_index child);
    void flush_internal(size_t index, proto_internal_node& node, u32 count);

    void insert_child_nonfull(proto_internal_node& node, const byte* key, block_index child);
    void flush_leaf();

    enum state_t {
        STATE_OK,
        STATE_ERROR,
        STATE_FINALIZED
    };

private:
    // Constants
    raw_btree_impl& m_tree;
    const u32 m_internal_min_children;
    const u32 m_internal_max_children;
    const u32 m_leaf_max_values;
    const u32 m_value_size;
    const u32 m_key_size;
    state_t m_state = STATE_OK;

    // Track these values while we build the tree.
    block_index m_leftmost_leaf;    // First leaf created.
    block_index m_rightmost_leaf;   // Last leaf created.
    u64 m_size = 0;                 // Total number of values inserted.

    // One proto node for every level of internal nodes.
    // The last entry is the root. Unique pointer for stable addresses.
    std::vector<std::unique_ptr<proto_internal_node>> m_parents;
    raw_btree_leaf_node m_leaf;
};

raw_btree_loader_impl::raw_btree_loader_impl(raw_btree_impl& tree)
    : m_tree(tree)
    , m_internal_min_children(m_tree.internal_node_min_chlidren())
    , m_internal_max_children(m_tree.internal_node_max_children())
    , m_leaf_max_values(m_tree.leaf_node_max_values())
    , m_value_size(m_tree.value_size())
    , m_key_size(m_tree.key_size())
{}

void raw_btree_loader_impl::insert(const byte* values, size_t count) {
    if (count == 0)
        return;
    if (!values)
        EXTPP_THROW(bad_argument("Values are null."));
    if (m_state == STATE_ERROR)
        EXTPP_THROW(bad_operation("A previous operation on this loader failed."));
    if (m_state == STATE_FINALIZED)
        EXTPP_THROW(bad_operation("This loader was already finalized."));

    detail::deferred guard = [&]{
        m_state = STATE_ERROR;
    };

    while (count > 0) {
        if (!m_leaf.valid()) {
            m_leaf = m_tree.create_leaf();
        }

        u32 leaf_size = m_leaf.get_size();
        if (leaf_size == m_leaf_max_values) {
            flush_leaf();
            m_leaf = m_tree.create_leaf();
            leaf_size = 0;
        }

        u32 take = m_leaf_max_values - leaf_size;
        if (take > count)
            take = count;
        EXTPP_ASSERT(take > 0, "Leaf must not be full.");

        m_leaf.append_nonfull(values, take);
        m_size += take;
        count -= take;
    }

    guard.disable();
}

// TODO: Exception safety.
void raw_btree_loader_impl::finish() {
    if (!m_tree.empty())
        EXTPP_THROW(bad_operation("The tree must be empty."));

    if (m_size == 0)
        return; // Nothing to do, the tree remains empty.

    if (m_leaf.valid()) {
        EXTPP_ASSERT(m_leaf.get_size() > 0, "Leaves must not be empty.");
        flush_leaf();
        m_leaf = raw_btree_leaf_node();
    }

    // Loop over all internal nodes and flush them to the next level.
    // The parents vector will grow as needed because of the insert_child(index + 1) calls.
    // After this loop terminates, the highest level of the tree contains exactly one child, the root.
    for (size_t index = 0; index < m_parents.size(); ++index) {
        proto_internal_node& node = *m_parents[index];

        if (index == m_parents.size() - 1) {
            if (node.size == 1) {
                break; // Done, this child is our root.
            }
            // Continue with flush, make the tree grow in height.
        } else {
            // Not the last entry, this means that there must be enough entries for one node.
            EXTPP_ASSERT(node.size >= m_internal_min_children, "Not enough entries for one internal node.");
        }

        if (node.size > m_internal_max_children) {
            flush_internal(index, node, (node.size + 1) / 2);
        }
        flush_internal(index, node, node.size);
    }

    EXTPP_ASSERT(m_parents.size() > 0 && m_parents.back()->size == 1,
                 "The highest level must contain one child.");

    m_tree.set_height(m_parents.size());
    m_tree.set_root(m_parents.back()->children[0]);
    m_tree.set_size(m_size);
    m_tree.set_leftmost(m_leftmost_leaf);
    m_tree.set_rightmost(m_rightmost_leaf);
    m_state = STATE_FINALIZED;
}

void raw_btree_loader_impl::discard() {
    if (m_state == STATE_OK)
        m_state = STATE_FINALIZED;

    if (m_leaf.valid()) {
        m_tree.clear_subtree(m_leaf.index(), 0);
        m_leaf = raw_btree_leaf_node();
    }

    // The level of the child entries in the following nodes.
    u32 level = 0;
    for (auto& nodeptr : m_parents) {
        proto_internal_node& node = *nodeptr;
        for (size_t child = 0; child < node.size; ++child) {
            m_tree.clear_subtree(node.children[child], level);
        }
        node.size = 0;
        ++level;
    }
}

// Note: Invalidates references to nodes on the parent stack.
void raw_btree_loader_impl::flush_leaf() {
    EXTPP_ASSERT(m_leaf.valid(), "Leaf must be valid.");
    EXTPP_ASSERT(m_leaf.get_size() > 0, "Leaf must not be empty.");

    // Insert the leaf into its (proto-) parent. If that parent becomes full,
    // emit a new node and register that one it's parent etc.
    key_buffer child_key;
    m_tree.derive_key(m_leaf.get(m_leaf.get_size() - 1), child_key.data());
    insert_child(0, child_key.data(), m_leaf.index());

    if (!m_leftmost_leaf) {
        m_leftmost_leaf = m_leaf.index();
    }
    m_rightmost_leaf = m_leaf.index();
    m_leaf = raw_btree_leaf_node();
}

// Note: Invalidates references to nodes on the parent stack.
void raw_btree_loader_impl::insert_child(size_t index, const byte* key, block_index child)
{
    EXTPP_ASSERT(index <= m_parents.size(), "Invalid parent index.");

    if (index == m_parents.size()) {
        m_parents.push_back(std::make_unique<proto_internal_node>(make_internal_node()));
    }

    proto_internal_node& node = *m_parents[index];
    if (node.size == node.capacity) {
        flush_internal(index, node, m_internal_max_children);
    }
    insert_child_nonfull(node, key, child);
}

// Flush count entries from the node to the next level.
// Note: Invalidates references to nodes on the parent stack.
void raw_btree_loader_impl::flush_internal(size_t index, proto_internal_node& node, u32 count) {
    EXTPP_ASSERT(index < m_parents.size(), "Invalid node index.");
    EXTPP_ASSERT(m_parents[index].get() == &node, "Node address mismatch.");
    EXTPP_ASSERT(count <= node.size, "Cannot flush that many elements.");
    EXTPP_ASSERT(count <= m_internal_max_children, "Too many elements for a tree node.");

    internal_node tree_node = m_tree.create_internal();
    detail::deferred cleanup = [&]{
        m_tree.free_internal(tree_node.index());
    };

    // Copy first `count` entries into the real node, then forward max key and node index
    // to the next level.
    tree_node.set_entries(node.keys.data(), node.children.data(), count);
    insert_child(index + 1, node.keys.data() + m_key_size * (count - 1), tree_node.index());
    cleanup.disable();

    // Shift `count` values to the left.
    std::copy_n(node.children.begin() + count, node.size - count, node.children.begin());
    std::memmove(node.keys.data(), node.keys.data() + count * m_key_size, (node.size - count) * m_key_size);
    node.size = node.size - count;
}

void raw_btree_loader_impl::insert_child_nonfull(proto_internal_node& node, const byte* key, block_index child)
{
    EXTPP_ASSERT(node.size < node.capacity, "Node is full.");

    std::memcpy(node.keys.data() + node.size * m_key_size, key, m_key_size);
    node.children[node.size] = child;
    node.size += 1;
}

// --------------------------------
//
//   BTree public interface
//
// --------------------------------

raw_btree::raw_btree(anchor_handle<anchor> _anchor, const raw_btree_options& options, allocator& alloc)
    : m_impl(std::make_unique<raw_btree_impl>(std::move(_anchor), options, alloc))
{}

raw_btree::~raw_btree() {}

raw_btree::raw_btree(raw_btree&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree& raw_btree::operator=(raw_btree&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

engine& raw_btree::get_engine() const { return impl().get_engine(); }
allocator& raw_btree::get_allocator() const { return impl().get_allocator(); }

u32 raw_btree::value_size() const { return impl().value_size(); }
u32 raw_btree::key_size() const { return impl().key_size(); }
u32 raw_btree::internal_node_capacity() const { return impl().internal_node_max_children(); }
u32 raw_btree::leaf_node_capacity() const { return impl().leaf_node_max_values(); }
bool raw_btree::empty() const { return impl().empty(); }
u64 raw_btree::size() const { return impl().size(); }
u32 raw_btree::height() const { return impl().height(); }
u64 raw_btree::internal_nodes() const { return impl().internal_nodes(); }
u64 raw_btree::leaf_nodes() const { return impl().leaf_nodes(); }
u64 raw_btree::nodes() const { return internal_nodes() + leaf_nodes(); }

double raw_btree::fill_factor() const {
    return empty() ? 0 : double(size()) / (leaf_nodes() * leaf_node_capacity());
}

u64 raw_btree::byte_size() const { return nodes() * get_engine().block_size(); }

double raw_btree::overhead() const {
    return empty() ? 0 : double(byte_size()) / (size() * value_size());
}

raw_btree_cursor raw_btree::create_cursor(raw_btree::cursor_seek_t seek) const {
    return raw_btree_cursor(impl().create_cursor(seek));
}

raw_btree_cursor raw_btree::find(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.find(key);
    return c;
}

raw_btree_cursor raw_btree::lower_bound(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.lower_bound(key);
    return c;
}

raw_btree_cursor raw_btree::upper_bound(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.upper_bound(key);
    return c;
}

raw_btree::insert_result raw_btree::insert(const byte* value) {
    auto c = create_cursor(raw_btree::seek_none);
    bool inserted = c.insert(value);
    return insert_result(std::move(c), inserted);
}

raw_btree::insert_result raw_btree::insert_or_update(const byte* value) {
    auto c = create_cursor(raw_btree::seek_none);
    bool inserted = c.insert_or_update(value);
    return insert_result(std::move(c), inserted);
}

void raw_btree::reset() { impl().clear(); }
void raw_btree::clear() { impl().clear(); }

raw_btree_loader raw_btree::bulk_load() {
    return raw_btree_loader(impl().bulk_load());
}

void raw_btree::dump(std::ostream& os) const { return impl().dump(os); }

raw_btree::node_view::~node_view() {}

void raw_btree::visit(bool (*visit_fn)(const node_view& node, void* user_data), void* user_data) const {
    return impl().visit(visit_fn, user_data);
}

void raw_btree::validate() const { return impl().validate(); }

raw_btree_impl& raw_btree::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_operation("invalid tree instance"));
    return *m_impl;
}

// --------------------------------
//
//   Cursor public interface
//
// --------------------------------

raw_btree_cursor::raw_btree_cursor()
{}

raw_btree_cursor::raw_btree_cursor(std::unique_ptr<raw_btree_cursor_impl> impl)
    : m_impl(std::move(impl))
{}

raw_btree_cursor::raw_btree_cursor(const raw_btree_cursor& other)
{
    if (other.m_impl) {
        if (!m_impl || m_impl->tree != other.m_impl->tree) {
            m_impl = std::make_unique<raw_btree_cursor_impl>(other.m_impl->tree);
        }
        m_impl->copy(*other.m_impl);
    }
}

raw_btree_cursor::raw_btree_cursor(raw_btree_cursor&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree_cursor::~raw_btree_cursor() {}

raw_btree_cursor& raw_btree_cursor::operator=(const raw_btree_cursor& other) {
    if (this != &other) {
        if (!other.m_impl) {
            m_impl.reset();
        } else {
            if (!m_impl || m_impl->tree != other.m_impl->tree) {
                m_impl = std::make_unique<raw_btree_cursor_impl>(other.m_impl->tree);
            }
            m_impl->copy(*other.m_impl);
        }
    }
    return *this;
}

raw_btree_cursor& raw_btree_cursor::operator=(raw_btree_cursor&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_btree_cursor_impl& raw_btree_cursor::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_cursor("invalid cursor"));
    return *m_impl;
}

u32 raw_btree_cursor::value_size() const { return impl().value_size(); }
u32 raw_btree_cursor::key_size() const { return impl().key_size(); }

bool raw_btree_cursor::at_end() const { return !m_impl || impl().at_end(); }
bool raw_btree_cursor::erased() const { return m_impl && impl().erased(); }

void raw_btree_cursor::reset() { return impl().reset_to_invalid(); }
bool raw_btree_cursor::move_min() { return impl().move_min(); }
bool raw_btree_cursor::move_max() { return impl().move_max(); }
bool raw_btree_cursor::move_next() { return impl().move_next(); }
bool raw_btree_cursor::move_prev() { return impl().move_prev(); }

bool raw_btree_cursor::lower_bound(const byte* key) { return impl().lower_bound(key); }
bool raw_btree_cursor::upper_bound(const byte* key) { return impl().upper_bound(key); }
bool raw_btree_cursor::find(const byte* key) { return impl().find(key); }
bool raw_btree_cursor::insert(const byte* value) { return impl().insert(value, false); }
bool raw_btree_cursor::insert_or_update(const byte* value) { return impl().insert(value, true); }
void raw_btree_cursor::erase() { impl().erase(); }

void raw_btree_cursor::set(const byte* data) { return impl().set(data); }
const byte* raw_btree_cursor::get() const { return impl().get(); }

void raw_btree_cursor::validate() const { return impl().validate(); }

bool raw_btree_cursor::operator==(const raw_btree_cursor& other) const {
    if (!m_impl) {
        return !other.m_impl || other.impl().at_end();
    }
    if (!other.m_impl) {
        return impl().at_end();
    }
    return impl() == other.impl();
}


// --------------------------------
//
//   Loader public interface
//
// --------------------------------

raw_btree_loader::raw_btree_loader(std::unique_ptr<raw_btree_loader_impl> impl)
    : m_impl(std::move(impl))
{
    EXTPP_ASSERT(m_impl, "Invalid impl pointer.");
}

raw_btree_loader::~raw_btree_loader() {}

raw_btree_loader::raw_btree_loader(raw_btree_loader&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree_loader& raw_btree_loader::operator=(raw_btree_loader&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_btree_loader_impl& raw_btree_loader::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_operation("bad loader"));
    return *m_impl;
}

void raw_btree_loader::insert(const byte* value) {
    insert(value, 1);
}

void raw_btree_loader::insert(const byte* values, size_t count) {
    return impl().insert(values, count);
}

void raw_btree_loader::finish() {
    impl().finish();
}

void raw_btree_loader::discard() {
    impl().discard();
}

} // namespace extpp
