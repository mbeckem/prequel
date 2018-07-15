#include <extpp/raw_list.hpp>

#include <extpp/exception.hpp>
#include <extpp/formatting.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <fmt/ostream.h>

namespace extpp {

class raw_list_node {
private:
    struct header {
        block_index prev;   // Previous node in list
        block_index next;   // Next node
        u32 size = 0;       // Number of values in this node <= capacity

        static constexpr auto get_binary_format() {
            return make_binary_format(
                        &header::prev,
                        &header::next,
                        &header::size);
        }
    };

public:
    raw_list_node() = default;

    raw_list_node(block_handle block, u32 value_size, u32 capacity)
        : m_handle(std::move(block), 0)
        , m_value_size(value_size)
        , m_capacity(capacity)
    {}

    const block_handle& block() const { return m_handle.block(); }
    block_index index() const { return block().index(); }

    void init() { m_handle.set(header()); }

    u32 get_size() const { return m_handle.get<&header::size>(); }
    void set_size(u32 new_size) const {
        EXTPP_ASSERT(new_size <= m_capacity, "Invalid size");
        m_handle.set<&header::size>(new_size);
    }

    block_index get_prev() const { return m_handle.get<&header::prev>(); }
    void set_prev(block_index index) const { m_handle.set<&header::prev>(index); }

    block_index get_next() const { return m_handle.get<&header::next>(); }
    void set_next(block_index index) const { m_handle.set<&header::next>(index); }

    void set(u32 index, const byte* value) const {
        EXTPP_ASSERT(index < m_capacity, "Index out of bounds.");
        m_handle.block().write(offset_of_index(index), value, m_value_size);
    }

    const byte* get(u32 index) const {
        EXTPP_ASSERT(index < m_capacity, "Index out of bounds.");
        return m_handle.block().data() + offset_of_index(index);
    }

    bool valid() const { return m_handle.valid(); }

    static u32 capacity(u32 block_size, u32 value_size) {
        u32 hdr = serialized_size<header>();
        if (block_size < hdr)
            return 0;
        return (block_size - hdr) / value_size;
    }

    static void move(const raw_list_node& src, u32 first_index, u32 last_index,
                      const raw_list_node& dest, u32 dest_index)
    {
        EXTPP_ASSERT(src.valid() && dest.valid(), "Nodes must both be valid.");
        EXTPP_ASSERT(src.m_value_size == dest.m_value_size, "Different value sizes.");
        EXTPP_ASSERT(src.m_capacity == dest.m_capacity, "Different capacities.");
        EXTPP_ASSERT(first_index <= src.m_capacity
                     && last_index <= src.m_capacity
                     && last_index >= first_index,
                     "Source range within bounds.");
        EXTPP_ASSERT(dest_index <= dest.m_capacity
                     && (last_index - first_index) <= dest.m_capacity - dest_index,
                     "Dest range within bounds.");

        byte* b1 = dest.block().writable_data() + dest.offset_of_index(dest_index);
        const byte* b2 = src.block().data() + src.offset_of_index(first_index);
        std::memmove(b1, b2, (last_index - first_index) * src.m_value_size);
    }

private:
    u32 offset_of_index(u32 index) const { return serialized_size<header>() + index * m_value_size; }

private:
    handle<header> m_handle;
    u32 m_value_size = 0;
    u32 m_capacity = 0;
};

class raw_list_visitor_impl {
public:
    raw_list_visitor_impl(const raw_list_impl* list);

    bool valid() const { return m_node.valid(); }

    block_index prev_address() const {
        return node().get_prev();
    }

    block_index next_address() const {
        return node().get_next();
    }

    block_index address() const {
        return node().block().index();
    }

    u32 size() const { return node().get_size(); }

    const byte* value(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return node().get(index);
    }

    void move_next();
    void move_prev();
    void move_first();
    void move_last();

    u32 value_size() const;

private:
    void move_to(block_index node_index);

    const raw_list_node& node() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return m_node;
    }

    u32 block_size() const;

private:
    const raw_list_impl* m_list;
    raw_list_node m_node;
};

class raw_list_cursor_impl {
public:
    /// The list this cursor belongs to.
    raw_list_impl* list;

    /// Tracked cursors are linked together in a list.
    /// When elements are inserted or removed, existing cursors are updated
    /// so that they keep pointing at the same element.
    /// This is currently a linked list and can be improved in the future to make it a tree.
    boost::intrusive::list_member_hook<> cursors;

    enum flags_t {
        /// The cursors points to nothing, only seek to begin/end are supported.
        /// If a cursor is not invalid, then it points to some usable location within the list,
        /// unless it is in DELETED state.
        INVALID = 1 << 0,

        /// The cursor's current element was deleted, but it still points to its last position.
        /// These cursors have to be special-cased when they are incremented or decremented.
        /// Deleted cursors can point one past the end of a node (i.e. index == size).
        DELETED = 1 << 1,

        /// Seek to the first value of the list (if possible), otherwise become invalid now.
        /// Only possible in combinatoin with DELETED.
        SEEK_FIRST_NEXT = 1 << 2,

        /// Same as SEEK_FIRST_NEXT, but seek to the last value.
        SEEK_LAST_NEXT = 1 << 3,
    };

    int flags = INVALID;

    /// The current list node (may be invalid for invalid or end cursors).
    raw_list_node node;

    /// The current list index. May be past the end of the node for deleted entries.
    u32 index = 0;

public:
    raw_list_cursor_impl(raw_list_impl* list);
    ~raw_list_cursor_impl();

    raw_list_cursor_impl(const raw_list_cursor_impl&);
    raw_list_cursor_impl& operator=(const raw_list_cursor_impl&);

    u32 value_size() const;

    void move_first();
    void move_last();
    void move_next();
    void move_prev();
    void move_to_node(block_index node_index, int direction);
    void erase();

    bool invalid() const { return flags & INVALID; }
    bool at_end() const { return !erased() && invalid(); }
    bool erased() const { return flags & DELETED; }

    const byte* get() const;
    void set(const byte* data);

    void insert_before(const byte* data);
    void insert_after(const byte* data);
};

class raw_list_impl : public uses_allocator {
public:
    using anchor = raw_list_anchor;

    raw_list_impl(anchor_handle<anchor> anchor_, u32 value_size, allocator& alloc_)
        : uses_allocator(alloc_)
        , m_anchor(std::move(anchor_))
        , m_value_size(value_size)
        , m_node_capacity(raw_list_node::capacity(get_engine().block_size(), m_value_size))
    {
        if (m_node_capacity == 0)
            EXTPP_THROW(bad_argument("block size too small to fit a single value"));
    }

    ~raw_list_impl();

    raw_list_impl(const raw_list_impl&) = delete;
    raw_list_impl& operator=(const raw_list_impl&) = delete;

    u32 block_size() const { return get_allocator().block_size(); }

    std::unique_ptr<raw_list_cursor_impl> create_cursor(raw_list::cursor_seek_t seek) {
        auto c = std::make_unique<raw_list_cursor_impl>(this);

        switch (seek) {
        case raw_list::seek_none:
            break;
        case raw_list::seek_first:
            c->move_first();
            break;
        case raw_list::seek_last:
            c->move_last();
            break;
        default:
            EXTPP_THROW(bad_argument("Invalid seek value"));
        }

        return c;
    }

    std::unique_ptr<raw_list_visitor_impl> create_visitor() const {
        return std::make_unique<raw_list_visitor_impl>(this);
    }

    bool empty() const { return size() == 0; }
    u64 size() const { return m_anchor.get<&anchor::size>(); }
    u64 nodes() const { return m_anchor.get<&anchor::nodes>(); }

    u32 value_size() const { return m_value_size; }
    u32 node_capacity() const { return m_node_capacity; }

    block_index first() const { return m_anchor.get<&anchor::first>(); }
    block_index last() const { return m_anchor.get<&anchor::last>(); }

    void clear();
    void push_back(const byte* data);
    void push_front(const byte* data);
    void pop_back();
    void pop_front();

    void visit(bool (*visit_fn)(const raw_list::node_view& node, void* user_data), void* user_data) const;

    void dump(std::ostream& os) const;

    raw_list_node read_node(block_index index) const;

private:
    block_index allocate_node();
    void free_node(block_index index);

    raw_list_node create_node();
    void destroy_node(const raw_list_node& node);

    void insert_first(const byte* value);
    void insert_at(raw_list_node node, u32 index, const byte* value);
    void erase_at(raw_list_node node, u32 index);

    template<typename Func>
    void visit_nodes(Func&& fn) const;

private:
    friend class raw_list_cursor_impl;

    using cursor_list_type = boost::intrusive::list<
        raw_list_cursor_impl,
        boost::intrusive::member_hook<
            raw_list_cursor_impl,
            decltype(raw_list_cursor_impl::cursors),
            &raw_list_cursor_impl::cursors
        >
    >;

    void link_cursor(raw_list_cursor_impl* cursor) {
        m_cursors.push_back(*cursor);
    }

    void unlink_cursor(raw_list_cursor_impl* cursor) {
        m_cursors.erase(m_cursors.iterator_to(*cursor));
    }

    template<typename Func>
    void cursors_in_node(block_index node_index, Func&& fn) {
        for (auto& cursor : m_cursors) {
            if (cursor.invalid())
                continue;
            if (cursor.node.index() != node_index)
                continue;

            fn(cursor);
        }
    }

private:
    anchor_handle<anchor> m_anchor;
    u32 m_value_size;
    u32 m_node_capacity;

    // Contains all existing cursors.
    mutable cursor_list_type m_cursors;
};

// --------------------------------
//
//   List implementation
//
// --------------------------------

raw_list_impl::~raw_list_impl() {
    m_cursors.clear_and_dispose([](raw_list_cursor_impl* cursor) {
        cursor->flags |= raw_list_cursor_impl::INVALID;
        cursor->node = raw_list_node();
        cursor->index = 0;
        cursor->list = nullptr;
    });
}

block_index raw_list_impl::allocate_node() {
    block_index index = get_allocator().allocate(1);
    m_anchor.set<&anchor::nodes>(nodes() + 1);
    return index;
}

void raw_list_impl::free_node(block_index index) {
    get_allocator().free(index);
    m_anchor.set<&anchor::nodes>(nodes() - 1);
}

raw_list_node raw_list_impl::create_node() {
    auto index = allocate_node();
    auto block = get_engine().zeroed(index);
    auto node = raw_list_node(std::move(block), m_value_size, m_node_capacity);
    node.init();
    return node;
}

raw_list_node raw_list_impl::read_node(block_index index) const {
    return raw_list_node(get_engine().read(index), m_value_size, m_node_capacity);
}

void raw_list_impl::destroy_node(const raw_list_node& node) {
    if (auto prev = node.get_prev()) {
        read_node(prev).set_next(node.get_next());
    } else {
        EXTPP_ASSERT(node.index() == m_anchor.get<&anchor::first>(),
                    "node must be the first one");
        m_anchor.set<&anchor::first>(node.get_next());
    }

    if (auto next = node.get_next()) {
        read_node(next).set_prev(node.get_prev());
    } else {
        EXTPP_ASSERT(node.index() == m_anchor.get<&anchor::last>(),
                    "node must be the last one");
        m_anchor.set<&anchor::last>(node.get_prev());
    }

    free_node(node.index());
}

void raw_list_impl::clear() {
    m_cursors.clear_and_dispose([](raw_list_cursor_impl* cursor) {
        cursor->flags |= raw_list_cursor_impl::INVALID | raw_list_cursor_impl::DELETED;
        cursor->node = raw_list_node();
        cursor->index = 0;
    });

    block_index index = m_anchor.get<&anchor::first>();
    while (index) {
        block_index next = read_node(index).get_next();
        free_node(index);
        index = next;
    }

    m_anchor.set<&anchor::first>(block_index());
    m_anchor.set<&anchor::last>(block_index());
    m_anchor.set<&anchor::size>(0);
}

void raw_list_impl::push_back(const byte* value) {
    if (empty()) {
        insert_first(value);
        return;
    }

    raw_list_node node = read_node(last());
    u32 index = node.get_size();
    insert_at(std::move(node), index, value);
}

void raw_list_impl::push_front(const byte* value) {
    if (empty()) {
        insert_first(value);
        return;
    }

    raw_list_node node = read_node(first());
    insert_at(std::move(node), 0, value);
}

void raw_list_impl::pop_back() {
    EXTPP_ASSERT(!empty(), "cannot remove from an empty list");

    auto node = read_node(last());
    u32 index = node.get_size() - 1;
    erase_at(node, index);
}

void raw_list_impl::pop_front() {
    EXTPP_ASSERT(!empty(), "cannot remove from an empty list");

    auto node = read_node(first());
    erase_at(node, 0);
}

void raw_list_impl::insert_first(const byte* value) {
    EXTPP_ASSERT(empty(), "list must be empty");

    raw_list_node node = create_node();
    node.set_size(1);
    node.set(0, value);

    m_anchor.set<&anchor::first>(node.index());
    m_anchor.set<&anchor::last>(node.index());
    m_anchor.set<&anchor::size>(1);
}

void raw_list_impl::insert_at(raw_list_node node, u32 index, const byte* value)
{
    EXTPP_ASSERT(index <= node.get_size(), "index is out of bounds");

    m_anchor.set<&anchor::size>(size() + 1);

    const u32 max = node_capacity();
    const u32 size = node.get_size();

    if (size < max) {
        raw_list_node::move(node, index, size, node, index + 1);
        node.set(index, value);
        node.set_size(size + 1);

        cursors_in_node(node.index(), [&](raw_list_cursor_impl& cursor) {
            if (cursor.index >= index)
                ++cursor.index;
        });
        return;
    }

    EXTPP_ASSERT(size == max,
                "Node must be exactly full.");

    // Split the node. The new node is to the right of the old one.
    raw_list_node new_node = create_node();
    {
        new_node.set_prev(node.index());
        new_node.set_next(node.get_next());

        if (new_node.get_next()) {
            // Current node has a successor.
            raw_list_node next = read_node(new_node.get_next());
            next.set_prev(new_node.index());
        } else {
            // Current node is the last node.
            m_anchor.set<&anchor::last>(new_node.index());
        }
        node.set_next(new_node.index());
    }

    // The number of elements that will remain in the old node.
    const u32 mid = [&]() -> u32 {
        if (new_node.index() == m_anchor.get<&anchor::last>())
            return size;    // Start a new node with a single element.
        if (node.index() == m_anchor.get<&anchor::first>())
            return 1;       // Leave only one element in the old node.

        // Else: move half of the values to the new node.
        // +1 because of the insertion, another +1 to round up.
        return (size + 2) / 2;
    }();

    node.set_size(mid);
    new_node.set_size(size + 1 - mid);
    if (index < mid) {
        raw_list_node::move(node, mid - 1, size, new_node, 0);
        raw_list_node::move(node, index, mid - 1, node, index + 1);
        node.set(index, value);

        cursors_in_node(node.index(), [&](raw_list_cursor_impl& cursor) {
            if (cursor.index >= mid - 1) {
                cursor.node = new_node;
                cursor.index -= (mid - 1);
            } else if (cursor.index >= index) {
                cursor.index++;
            }
        });
        return;
    } else {
        index -= mid;
        raw_list_node::move(node, mid, mid + index, new_node, 0);
        new_node.set(index, value);
        raw_list_node::move(node, mid + index, size, new_node, index + 1);

        cursors_in_node(node.index(), [&](raw_list_cursor_impl& cursor) {
            if (cursor.index >= mid) {
                cursor.node = new_node;
                cursor.index -= mid;
                if (cursor.index >= index)
                    cursor.index++;
            }
        });
        return;
    }
}

void raw_list_impl::erase_at(raw_list_node node, u32 index) {
    EXTPP_ASSERT(node.valid(), "Invalid node.");
    EXTPP_ASSERT(index < node.get_size(), "Index out of bounds.");

    m_anchor.set<&anchor::size>(m_anchor.get<&anchor::size>() - 1);

    const u32 min_size = node_capacity() / 2;
    u32 node_size = node.get_size();

    raw_list_node::move(node, index + 1, node_size, node, index);
    node_size -= 1;
    node.set_size(node_size);

    // Move or invalidate cursors.
    cursors_in_node(node.index(), [&](raw_list_cursor_impl& cursor) {
        if (cursor.index == index)
            cursor.flags |= raw_list_cursor_impl::DELETED;
        else if (cursor.index > index)
            cursor.index--;
    });

    // Can return early if the node is still full enough.
    if (node_size >= min_size) {
        return;
    }

    // The first and the last node can become completely empty.
    if (node.index() == m_anchor.get<&anchor::first>() || node.index() == m_anchor.get<&anchor::last>()) {
        if (node_size == 0) {
            int flags = 0;
            flags |= raw_list_cursor_impl::DELETED;
            flags |= node.index() == m_anchor.get<&anchor::first>()
                    ? raw_list_cursor_impl::SEEK_FIRST_NEXT
                    : raw_list_cursor_impl::SEEK_LAST_NEXT;

            cursors_in_node(node.index(), [&](raw_list_cursor_impl& cursor) {
                cursor.flags = flags;
                cursor.node = raw_list_node();
                cursor.index = 0;
            });
            destroy_node(node);
        }
        return;
    }

    // Neither the first nor the last node. Either steal a single element, or,
    // if that would leave the successor too empty, merge both nodes.
    // Note: stealing from the last node is fine as long as it does not become empty,
    // even if it has fewer than `min` values.
    raw_list_node next = read_node(node.get_next());
    u32 next_size = next.get_size();
    if (next_size > min_size || (next.index() == m_anchor.get<&anchor::last>() && next_size > 1)) {
        raw_list_node::move(next, 0, 1, node, node_size);
        raw_list_node::move(next, 1, next_size, next, 0);

        cursors_in_node(next.index(), [&](raw_list_cursor_impl& cursor) {
            if (cursor.index == 0) {
                cursor.node = node;
                cursor.index = node_size;
            } else {
                cursor.index--;
            }
        });

        node_size++;
        next_size--;
        node.set_size(node_size);
        next.set_size(next_size);
    } else {
        raw_list_node::move(next, 0, next_size, node, node_size);
        cursors_in_node(next.index(), [&](raw_list_cursor_impl& cursor) {
            cursor.node = node;
            cursor.index += node_size;
        });

        node_size += next_size;
        node.set_size(node_size);
        destroy_node(next);
    }
}

template<typename Func>
void raw_list_impl::visit_nodes(Func&& fn) const {
    block_index address = first();
    while (address) {
        raw_list_node node = read_node(address);
        if (!fn(node))
            return;

        address = node.get_next();
    }
}

void raw_list_impl::visit(bool (*visit_fn)(const raw_list::node_view& node, void* user_data),
                          void* user_data) const
{
    if (!visit_fn)
        EXTPP_THROW(bad_argument("Invalid visitation function."));

    struct node_view_impl : raw_list::node_view {
        raw_list_node node;

        block_index address() const override { return node.index(); }
        block_index next_address() const override { return node.get_next(); }
        block_index prev_address() const override { return node.get_prev(); }

        u32 value_count() const override { return node.get_size(); }

        const byte* value(u32 index) const override {
            if (index >= value_count())
                EXTPP_THROW(bad_argument("Value index out of bounds."));
            return node.get(index);
        }
    };

    node_view_impl view;
    visit_nodes([&](const raw_list_node& node) {
         view.node = node;
         return visit_fn(view, user_data);
    });
}

void raw_list_impl::dump(std::ostream& os) const {
    fmt::print(
        "Raw list:\n"
        "  Value size: {}\n"
        "  Block size: {}\n"
        "  Node Capacity: {}\n"
        "  Size: {}\n"
        "  Nodes: {}\n"
        "\n",
        value_size(),
        get_engine().block_size(),
        node_capacity(),
        size(),
        nodes());

    if (!empty())
        os << "\n";

    visit_nodes([&](const raw_list_node& node) -> bool{
        fmt::print(
            "  Node @{}:\n"
            "    Previous: @{}\n"
            "    Next: @{}\n"
            "    Size: {}\n",
            node.index(),
            node.get_prev(),
            node.get_next(),
            node.get_size());

        u32 size = node.get_size();
        for (u32 i = 0; i < size; ++i) {
            const byte* data = static_cast<const byte*>(node.get(i));
            fmt::print("    {:>4}: {}\n", i, format_hex(data, value_size()));
        }
        fmt::print("\n");
        return true;
    });
}

// --------------------------------
//
//   Cursor implementation
//
// --------------------------------

static void check_cursor_valid(const raw_list_cursor_impl& c) {
    if (!c.list)
        EXTPP_THROW(bad_cursor("the cursor's list instance has been destroyed"));
}

static void check_cursor_valid_element(const raw_list_cursor_impl& c) {
    check_cursor_valid(c);
    if (c.flags & raw_list_cursor_impl::DELETED)
        EXTPP_THROW(bad_cursor("cursor points to deleted element"));
    if (c.flags & raw_list_cursor_impl::INVALID)
        EXTPP_THROW(bad_cursor("invalid cursor"));

    EXTPP_ASSERT(c.node.valid(), "Invalid node.");
    EXTPP_ASSERT(c.index < c.node.get_size(), "Invalid index.");
}

raw_list_cursor_impl::raw_list_cursor_impl(raw_list_impl* list)
    : list(list)
{
    list->link_cursor(this);
}

raw_list_cursor_impl::~raw_list_cursor_impl() {
    if (list && cursors.is_linked())
        list->unlink_cursor(this);
}

raw_list_cursor_impl::raw_list_cursor_impl(const raw_list_cursor_impl& other)
    : list(other.list)
    , flags(other.flags)
    , node(other.node)
    , index(other.index)
{
    if (list)
        list->link_cursor(this);
}

raw_list_cursor_impl& raw_list_cursor_impl::operator=(const raw_list_cursor_impl& other) {
    if (this != &other) {
        const bool diff = list != other.list;
        if (list && diff)
            list->unlink_cursor(this);

        list = other.list;
        flags = other.flags;
        node = other.node;
        index = other.index;

        if (list && diff)
            list->link_cursor(this);
    }
    return *this;
}

u32 raw_list_cursor_impl::value_size() const {
    check_cursor_valid(*this);
    return list->value_size();
}

void raw_list_cursor_impl::move_first() {
    check_cursor_valid(*this);

    flags = 0;
    move_to_node(list->first(), 1);
}

void raw_list_cursor_impl::move_last() {
    check_cursor_valid(*this);

    flags = 0;
    move_to_node(list->last(), -1);
}

void raw_list_cursor_impl::move_next() {
    check_cursor_valid(*this);

    if (flags & DELETED) {
        flags &= ~DELETED;
        if (flags & INVALID)
            return;

        if (flags & SEEK_FIRST_NEXT) {
            flags &= ~SEEK_FIRST_NEXT;
            move_first();
            return;
        }

        if (flags & SEEK_LAST_NEXT) {
            flags &= ~SEEK_LAST_NEXT;
            flags |= INVALID;
            return;
        }
    } else if (flags & INVALID) {
        EXTPP_THROW(bad_cursor("bad cursor"));
    } else {
        ++index;
    }

    EXTPP_ASSERT(node.valid(), "Invalid node.");
    EXTPP_ASSERT(index <= node.get_size(), "Index out of bounds.");
    if (index == node.get_size())
        move_to_node(node.get_next(), 1);
}

void raw_list_cursor_impl::move_prev() {
    check_cursor_valid(*this);

    if (flags & DELETED) {
        flags &= ~DELETED;
        if (flags & INVALID)
            return;

        if (flags & SEEK_LAST_NEXT) {
            flags &= ~SEEK_LAST_NEXT;
            move_last();
            return;
        }

        if (flags & SEEK_FIRST_NEXT) {
            flags &= ~SEEK_FIRST_NEXT;
            flags |= INVALID;
            return;
        }
    } else if (flags & INVALID) {
        EXTPP_THROW(bad_cursor("bad cursor"));
    }

    EXTPP_ASSERT(node.valid(), "Invalid node.");
    EXTPP_ASSERT(index <= node.get_size(), "Index out of bounds.");
    if (index == 0) {
        move_to_node(node.get_prev(), -1);
    } else {
        --index;
    }
}

void raw_list_cursor_impl::move_to_node(block_index node_index, int direction) {
    EXTPP_ASSERT(!(flags & DELETED), "Cursor in invalid state for moving, clear deletion flag first.");
    EXTPP_ASSERT(direction == -1 || direction == 1, "Invalid direction value.");

    if (node_index) {
        node = list->read_node(node_index);
        index = direction == 1 ? 0 : node.get_size() - 1;
        flags &= ~INVALID;
    } else {
        flags |= INVALID;
        node = raw_list_node();
        index = 0;
    }
}

void raw_list_cursor_impl::erase() {
    check_cursor_valid_element(*this);
    flags |= DELETED;
    list->erase_at(node, index);
}

const byte* raw_list_cursor_impl::get() const {
    check_cursor_valid_element(*this);
    return node.get(index);
}

void raw_list_cursor_impl::set(const byte* data) {
    check_cursor_valid_element(*this);
    node.set(index, data);
}

void raw_list_cursor_impl::insert_before(const byte* data) {
    check_cursor_valid_element(*this);
    list->insert_at(node, index, data);
}

void raw_list_cursor_impl::insert_after(const byte* data) {
    check_cursor_valid_element(*this);
    list->insert_at(node, index + 1, data);
}

// --------------------------------
//
//   List public interface
//
// --------------------------------

raw_list::raw_list(anchor_handle<anchor> anchor_, u32 value_size, allocator& alloc_)
    : m_impl(std::make_unique<raw_list_impl>(std::move(anchor_), value_size, alloc_))
{}

raw_list::~raw_list() {}

raw_list::raw_list(raw_list&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_list& raw_list::operator=(raw_list&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

engine& raw_list::get_engine() const { return impl().get_engine(); }
allocator& raw_list::get_allocator() const { return impl().get_allocator(); }
u32 raw_list::value_size() const { return impl().value_size(); }
u32 raw_list::node_capacity() const { return impl().node_capacity(); }
bool raw_list::empty() const { return impl().empty(); }
u64 raw_list::size() const { return impl().size(); }
u64 raw_list::nodes() const { return impl().nodes(); }

double raw_list::fill_factor() const {
    return empty() ? 0 : double(size()) / (nodes() * node_capacity());
}

u64 raw_list::byte_size() const {
    return nodes() * get_engine().block_size();
}

double raw_list::overhead() const {
    return empty() ? 0 : double(byte_size()) / (size() * value_size());
}

raw_list_cursor raw_list::create_cursor(raw_list::cursor_seek_t seek) const {
    return raw_list_cursor(impl().create_cursor(seek));
}

void raw_list::reset() { impl().clear(); }
void raw_list::clear() { impl().clear(); }
void raw_list::push_front(const byte *value) { impl().push_front(value); }
void raw_list::push_back(const byte* value) { impl().push_back(value); }
void raw_list::pop_front() { impl().pop_front(); }
void raw_list::pop_back() { impl().pop_back(); }

raw_list::node_view::~node_view() {}

void raw_list::visit(bool (*visit_fn)(const node_view& node, void* user_data), void* user_data) const {
    return impl().visit(visit_fn, user_data);
}

void raw_list::dump(std::ostream& os) const { impl().dump(os); }

raw_list_impl& raw_list::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid list.");
    return *m_impl;
}

// --------------------------------
//
//   Cursor public interface
//
// --------------------------------

raw_list_cursor::raw_list_cursor()
{}

raw_list_cursor::raw_list_cursor(std::unique_ptr<raw_list_cursor_impl> impl)
    : m_impl(std::move(impl))
{}

raw_list_cursor::raw_list_cursor(const raw_list_cursor& other)
    : m_impl(other.m_impl ? std::make_unique<raw_list_cursor_impl>(*other.m_impl) : nullptr)
{}

raw_list_cursor::raw_list_cursor(raw_list_cursor&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_list_cursor::~raw_list_cursor() {}

raw_list_cursor& raw_list_cursor::operator=(const raw_list_cursor& other) {
    if (this != &other) {
        if (m_impl && other.m_impl) {
            *m_impl = *other.m_impl;
        } else {
            m_impl = other.m_impl ? std::make_unique<raw_list_cursor_impl>(*other.m_impl) : nullptr;
        }
    }
    return *this;
}

raw_list_cursor& raw_list_cursor::operator=(raw_list_cursor&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_list_cursor_impl& raw_list_cursor::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_cursor("bad cursor"));
    return *m_impl;
}

void raw_list_cursor::move_first() { impl().move_first(); }
void raw_list_cursor::move_last() { impl().move_last(); }
void raw_list_cursor::move_next() { impl().move_next(); }
void raw_list_cursor::move_prev() { impl().move_prev(); }

void raw_list_cursor::erase() { impl().erase(); }

void raw_list_cursor::insert_before(const byte* data) { impl().insert_before(data); }
void raw_list_cursor::insert_after(const byte* data) { impl().insert_after(data); }

const byte* raw_list_cursor::get() const { return impl().get(); }
void raw_list_cursor::set(const byte* data) { impl().set(data); }
u32 raw_list_cursor::value_size() const { return impl().value_size(); }

bool raw_list_cursor::at_end() const { return !m_impl || impl().at_end(); }
bool raw_list_cursor::erased() const { return m_impl && impl().erased(); }

} // namespace extpp
