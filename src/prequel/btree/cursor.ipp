#ifndef PREQUEL_BTREE_CURSOR_IPP
#define PREQUEL_BTREE_CURSOR_IPP

#include <prequel/btree/cursor.hpp>

#include <prequel/btree/tree.hpp>
#include <prequel/exception.hpp>

namespace prequel::detail::btree_impl {

inline cursor::cursor(btree_impl::tree* parent)
    : m_tree(parent) {
    if (m_tree)
        m_tree->link_cursor(this);
    reset_to_invalid();
}

inline cursor::~cursor() {
    if (m_tree && m_cursors.is_linked())
        m_tree->unlink_cursor(this);
}

inline void cursor::copy(const cursor& other) {
    if (this == &other)
        return;

    PREQUEL_ASSERT(m_tree == other.m_tree, "Cursors must belong to the same tree.");
    m_parents = other.m_parents;
    m_leaf = other.m_leaf;
    m_index = other.m_index;
    m_flags = other.m_flags;
}

inline void cursor::check_tree_valid() const {
    if (!m_tree)
        PREQUEL_THROW(bad_cursor("The cursor's tree instance has been destroyed."));
}

inline void cursor::check_element_valid() const {
    check_tree_valid();
    if (m_flags & cursor::INPROGRESS)
        PREQUEL_THROW(bad_cursor("Leak of in-progress cursor."));
    if (m_flags & cursor::DELETED)
        PREQUEL_THROW(bad_cursor("Cursor points to deleted element."));
    if (m_flags & cursor::INVALID)
        PREQUEL_THROW(bad_cursor("Bad cursor."));

#ifdef PREQUEL_DEBUG
    {
        const u32 height = m_tree->height();
        PREQUEL_ASSERT(m_parents.size() + 1 == height,
                       "Cursor does not have enough nodes on the stack.");
        for (u32 i = 0; i < m_parents.size(); ++i) {
            auto& entry = m_parents[i];
            PREQUEL_ASSERT(entry.node.valid(), "Invalid node on the cursor's stack.");
            PREQUEL_ASSERT(entry.index < entry.node.get_child_count(),
                           "Child index out of bounds.");

            if (i > 0) {
                // Must be at the specified index in its parent.
                auto& parent_entry = m_parents[i - 1];
                PREQUEL_ASSERT(parent_entry.node.get_child(parent_entry.index) == entry.node.index(),
                               "Must be at that position in the parent.");
            }
        }

        if (height > 1) {
            // Must be at the specified index in its parent.
            auto& parent_entry = m_parents.back();
            PREQUEL_ASSERT(parent_entry.node.get_child(parent_entry.index) == m_leaf.index(),
                           "Must be at that position in the parent.");
        }

        if (height == 1) {
            PREQUEL_ASSERT(m_leaf.index() == m_tree->root(), "Leaf must be the root");
        } else if (height > 1) {
            PREQUEL_ASSERT(m_parents.front().node.index() == m_tree->root(),
                           "First parent must be the root.");
        } else {
            PREQUEL_ASSERT(false, "The tree is empty...");
        }

        PREQUEL_ASSERT(m_leaf.valid(), "Leaf must be valid.");
        PREQUEL_ASSERT(m_index < m_leaf.get_size(), "Invalid index in leaf.");
    }
#endif
}

inline u32 cursor::value_size() const {
    check_tree_valid();
    return m_tree->value_size();
}

inline u32 cursor::key_size() const {
    check_tree_valid();
    return m_tree->key_size();
}

inline bool cursor::at_end() const {
    return !erased() && (m_flags & INVALID);
}

inline bool cursor::erased() const {
    return m_flags & DELETED;
}

template<bool max>
inline void cursor::init_position() {
    check_tree_valid();
    reset_to_zero();

    const u32 height = m_tree->height();
    if (height == 0) {
        m_flags |= INVALID;
        return;
    }

    m_parents.reserve(height - 1);
    m_flags |= INPROGRESS;

    block_index current = m_tree->root();
    for (u32 level = height - 1; level > 0; --level) {
        internal_node node = m_tree->read_internal(current);

        u32 child_index;
        if constexpr (max) {
            child_index = node.get_child_count() - 1;
        } else {
            child_index = 0;
        }

        block_index child = node.get_child(child_index);
        m_parents.push_back({std::move(node), child_index});
        current = child;
    }

    m_leaf = m_tree->read_leaf(current);
    PREQUEL_ASSERT(m_leaf.get_size() > 0, "Leaf cannot be empty.");

    if constexpr (max) {
        m_index = m_leaf.get_size() - 1;
    } else {
        m_index = 0;
    }

    m_flags &= ~INPROGRESS;
}

inline bool cursor::move_min() {
    init_position<false>();
    return !at_end();
}

inline bool cursor::move_max() {
    init_position<true>();
    return !at_end();
}

inline bool cursor::move_prev() {
    check_tree_valid();

    if (m_flags & DELETED) {
        m_flags &= ~DELETED;
        if (m_flags & INVALID)
            return false;
    } else if (m_flags & INVALID) {
        PREQUEL_THROW(bad_cursor("Bad cursor."));
    }

    if (m_index > 0) {
        --m_index;
        return true;
    }

    // Find a parent that is not yet at index 0.
    auto rpos = std::find_if(m_parents.rbegin(), m_parents.rend(),
                             [&](const internal_entry& entry) { return entry.index > 0; });
    if (rpos == m_parents.rend()) {
        reset_to_invalid();
        return false;
    }

    // Decrement the index in this parent node, then walk to the leaf level and set all indices to size - 1.
    // Note that rpos.base() is the element *after* the parent.
    m_flags |= INPROGRESS;
    auto pos = std::prev(rpos.base());
    pos->index -= 1;
    for (auto child_pos = pos + 1; child_pos != m_parents.end(); ++child_pos, ++pos) {
        child_pos->node = m_tree->read_internal(pos->node.get_child(pos->index));
        child_pos->index = child_pos->node.get_child_count() - 1;
    }

    m_leaf = m_tree->read_leaf(pos->node.get_child(pos->index));
    PREQUEL_ASSERT(m_leaf.get_size() > 0, "Leaf cannot be empty.");
    m_index = m_leaf.get_size() - 1;
    m_flags &= ~INPROGRESS;
    return true;
}

inline bool cursor::move_next() {
    check_tree_valid();

    if (m_flags & DELETED) {
        m_flags &= ~DELETED;
        if (m_flags & INVALID)
            return false;
    } else if (m_flags & INVALID) {
        PREQUEL_THROW(bad_cursor("Bad cursor."));
    } else {
        ++m_index;
    }

    if (m_index < m_leaf.get_size()) {
        return true;
    }

    // Find a parent that is not yet at it's last index.
    auto rpos = std::find_if(m_parents.rbegin(), m_parents.rend(), [&](const internal_entry& entry) {
        return entry.index + 1 < entry.node.get_child_count();
    });
    if (rpos == m_parents.rend()) {
        reset_to_invalid();
        return false;
    }

    // Increment the index in this parent node, then walk to the leaf level and set all indices to 0.
    // Note that rpos.base() is the element *after* the parent.
    m_flags |= INPROGRESS;
    auto pos = std::prev(rpos.base());
    pos->index++;
    for (auto child_pos = pos + 1; child_pos != m_parents.end(); ++child_pos, ++pos) {
        child_pos->node = m_tree->read_internal(pos->node.get_child(pos->index));
        child_pos->index = 0;
    }

    m_leaf = m_tree->read_leaf(pos->node.get_child(pos->index));
    PREQUEL_ASSERT(m_leaf.get_size() > 0, "Leaf cannot be empty.");
    m_index = 0;
    m_flags &= ~INPROGRESS;
    return true;
}

inline bool cursor::lower_bound(const byte* key) {
    check_tree_valid();
    m_tree->lower_bound(key, *this);
    return !at_end();
}

inline bool cursor::upper_bound(const byte* key) {
    check_tree_valid();
    m_tree->upper_bound(key, *this);
    return !at_end();
}

inline bool cursor::find(const byte* key) {
    check_tree_valid();
    m_tree->find(key, *this);
    return !at_end();
}

inline bool cursor::insert(const byte* value, bool overwrite) {
    check_tree_valid();
    bool inserted = m_tree->insert(value, *this);
    if (!inserted && overwrite) {
        m_leaf.set(m_index, value);
    }
    return inserted;
}

inline void cursor::erase() {
    check_element_valid();
    m_tree->erase(*this);
}

inline const byte* cursor::get() const {
    check_element_valid();
    return m_leaf.get(m_index);
}

inline void cursor::set(const byte* value) {
    PREQUEL_ASSERT(value, "Nullpointer instead of a value.");
    check_element_valid();

    key_buffer k1, k2;
    m_tree->derive_key(get(), k1.data());
    m_tree->derive_key(value, k2.data());
    if (std::memcmp(k1.data(), k2.data(), m_tree->key_size()) != 0) {
        PREQUEL_THROW(bad_argument("The key derived from the new value differs from the old key."));
    }

    m_leaf.set(m_index, value);
}

inline void cursor::validate() const {
#define BAD(...) PREQUEL_THROW(bad_cursor(__VA_ARGS__))

    // TODO

#undef BAD
}

inline bool cursor::operator==(const cursor& other) const {
    if (m_tree != other.m_tree)
        return false;
    if (at_end() != other.at_end())
        return false;
    if (erased() != other.erased())
        return false;

    if (at_end())
        return true;
    return m_leaf.index() == other.m_leaf.index() && m_index == other.m_index;
}

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_CURSOR_IPP
