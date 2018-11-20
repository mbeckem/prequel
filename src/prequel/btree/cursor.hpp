#ifndef PREQUEL_BTREE_CURSOR_HPP
#define PREQUEL_BTREE_CURSOR_HPP

#include <prequel/btree/base.hpp>
#include <prequel/btree/internal_node.hpp>
#include <prequel/btree/leaf_node.hpp>
#include <prequel/defs.hpp>

#include <boost/intrusive/list_hook.hpp>

#include <vector>

namespace prequel::detail::btree_impl {

class cursor {
    // TODO: Put state used by the tree into functions and mark them as such.
    friend btree_impl::tree;

private:
    // Represents one of the parent (internal) nodes of the current leaf.
    // The first entry (if any) is the root, then the root's child and so forth.
    // The index is the index of the next level's node (internal or leaf) in its parent.
    struct internal_entry {
        internal_node node;
        u32 index = 0;
    };

    enum flags_t {
        INVALID = 1 << 0,    ///< When the cursor is at the end or was otherwise invalidated.
        DELETED = 1 << 1,    ///< When the current element was deleted.
        INPROGRESS = 1 << 2, ///< When an operation is not yet complete.
    };

    btree_impl::tree* m_tree = nullptr;

    /// Tracked cursors are linked together in a list.
    /// When elements are inserted or removed, existing cursors are updated
    /// so that they keep pointing at the same element.
    boost::intrusive::list_member_hook<> m_cursors;

    // Parents of the current leaf node.
    std::vector<internal_entry> m_parents;

    // The current leaf node.
    leaf_node m_leaf;

    // The current value's index in its leaf.
    u32 m_index = 0;

    // A combination of flags_t values.
    int m_flags = 0;

public:
    explicit cursor(btree_impl::tree* parent);
    ~cursor();

    cursor(const cursor&) = delete;
    cursor& operator=(const cursor&) = delete;

    // Explicit to avoid errors.
    void copy(const cursor&);

    btree_impl::tree* tree() const { return m_tree; }

    void reset_to_zero() {
        m_flags = 0;
        m_parents.clear();
        m_leaf = leaf_node();
        m_index = 0;
    }

    // Called by the tree parent to invalidate a cursor.
    // The old flags can get saved.
    void reset_to_invalid(int saved_flags = 0) {
        reset_to_zero();
        m_flags |= saved_flags;
        m_flags |= INVALID;
    }

    bool invalid() const { return m_flags & INVALID; }

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

    bool operator==(const cursor& other) const;

private:
    // Seek to the first or last entry in the tree.
    template<bool max>
    void init_position();

private:
    void check_tree_valid() const;
    void check_element_valid() const;
};

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_CURSOR_HPP
