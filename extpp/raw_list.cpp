#include <extpp/raw_list.hpp>

#include <boost/intrusive/set.hpp>
#include <boost/intrusive/set_hook.hpp>
#include <boost/iterator/iterator_facade.hpp>

namespace extpp {

namespace {

// Node, Index in node.
// Lexicographical order.
using list_position = std::tuple<block_index, u32>;

} // namespace

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
    void set_size(u32 new_size) const { m_handle.set<&header::size>(new_size); }

    block_index get_prev() const { return m_handle.get<&header::prev>(); }
    void set_prev(block_index index) const { m_handle.set<&header::prev>(index); }

    block_index get_next() const { return m_handle.get<&header::next>(); }
    void set_next(block_index index) const { m_handle.set<&header::next>(index); }

    void set(u32 index, const void* value) const {
        EXTPP_ASSERT(index < m_capacity, "Index out of bounds.");
        m_handle.block().write(offset_of_index(index), value, m_value_size);
    }

    const void* get(u32 index) const {
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

class raw_list_iterator_impl {
public:
    raw_list_iterator_impl(const raw_list_impl* list)
        : m_list(list) {} // End Iterator

    raw_list_iterator_impl(const raw_list_impl* list, raw_list_node node, u32 index);

    ~raw_list_iterator_impl();

    // TODO
    raw_list_iterator_impl(const raw_list_iterator_impl& other) = default;
    raw_list_iterator_impl& operator=(const raw_list_iterator_impl& other) = default;

    raw_list_iterator_impl(raw_list_iterator_impl&&) = delete;
    raw_list_iterator_impl& operator=(raw_list_iterator_impl&&) = delete;

    const raw_list_impl* get_list() const { return m_list; }

    bool valid() const { return m_list != nullptr && m_node.valid(); }
    bool at_end() const { return !m_node.valid(); }

    const raw_list_node& node() const { return m_node; }
    u32 index() const { return m_index; }

    list_position get_position() const {
        return { m_node.index(), m_index };
    }

    void increment();
    void decrement();

    const void* get() const {
        check_valid();
        EXTPP_ASSERT(m_index < m_node.get_size(), "Index out of bounds.");
        return m_node.get(m_index);
    }

    bool operator==(const raw_list_iterator_impl& other) const {
        return m_list == other.m_list && get_position() == other.get_position();
    }

private:
    void check_valid() const {
        EXTPP_ASSERT(valid(), "Invalid iterator.");
    }

private:
    friend class raw_list_impl;

    using iterator_map_hook = boost::intrusive::set_member_hook<>;

    bool is_linked() const { return m_map_hook.is_linked(); }

    // Called when the value moved.
    void position_changed(raw_list_node node, u32 index) {
        m_node = std::move(node);
        m_index = index;
    }

    void position_changed(u32 index) {
        m_index = index;
    }

    void position_invalidated() {
        m_node = raw_list_node();
        m_index = 0;
        // TODO: Store a flag to indicate deletion?
    }

private:
    const raw_list_impl* m_list = nullptr;
    raw_list_node m_node;
    u32 m_index = 0;
    iterator_map_hook m_map_hook;
};

class raw_list_impl : public uses_allocator {
public:
    using anchor = raw_list_anchor;

    raw_list_impl(handle<anchor> anchor_, u32 value_size, allocator& alloc_)
        : uses_allocator(alloc_)
        , m_anchor(std::move(anchor_))
        , m_value_size(value_size)
        , m_node_capacity(raw_list_node::capacity(get_engine().block_size(), m_value_size))
    {
    }

    u32 block_size() const { return get_allocator().block_size(); }

    bool empty() const { return size() == 0; }

    std::unique_ptr<raw_list_iterator_impl> begin() const;
    std::unique_ptr<raw_list_iterator_impl> end() const;
    std::unique_ptr<raw_list_visitor_impl> visit() const;

    u64 size() const { return m_anchor.get<&anchor::size>(); }
    u64 nodes() const { return m_anchor.get<&anchor::nodes>(); }

    u32 value_size() const { return m_value_size; }
    u32 node_capacity() const { return m_node_capacity; }

    block_index first() const { return m_anchor.get<&anchor::first>(); }
    block_index last() const { return m_anchor.get<&anchor::last>(); }

    void clear();
    void insert(const raw_list_iterator_impl& pos, const void* data);
    void erase(const raw_list_iterator_impl& pos);
    void push_back(const void* data);
    void push_front(const void* data);
    void pop_back();
    void pop_front();

    raw_list_node read_node(block_index index) const;

private:
    block_index allocate_node();
    void free_node(block_index index);

    raw_list_node create_node();
    void destroy_node(const raw_list_node& node);

    void insert_first(const void* value);
    void insert_at(raw_list_node node, u32 index, const void* value);
    void erase_at(const raw_list_node& node, u32 index);

private:
    friend class raw_list_iterator_impl;

    struct iterator_position_key {
        using type = list_position;

        type operator()(const raw_list_iterator_impl& iter) const {
            return iter.get_position();
        }
    };

    // Every iterator that points to a valid position within the list
    // is kept in this ordered collection. Whenever the iterator's position
    // changes, the change must be reflected in this collection by
    // reinserting the iterator.
    // The keys of of the individual elements change, so great
    // care must be taken that the order in the collection is maintained.
    using iterator_map_type = boost::intrusive::multiset<
        raw_list_iterator_impl,
        boost::intrusive::member_hook<
            raw_list_iterator_impl,
            raw_list_iterator_impl::iterator_map_hook,
            &raw_list_iterator_impl::m_map_hook
        >,
        boost::intrusive::key_of_value<iterator_position_key>
    >;

    // Insert a new iterator.
    void insert_position(raw_list_iterator_impl& iter) const {
        EXTPP_CHECK(!iter.is_linked(), "Iterator is linked.");
        m_iterators.insert(iter);
    }

    // Remove a tracked iterator.
    void erase_position(raw_list_iterator_impl& iter) const {
        EXTPP_CHECK(iter.is_linked(), "Iterator is not linked.");
        m_iterators.erase(m_iterators.iterator_to(iter));
    }

    // Reinsert the tracked iterator.
    void update_position(raw_list_iterator_impl& iter) const {
        EXTPP_CHECK(iter.is_linked(), "Iterator is not linked.");
        m_iterators.erase(m_iterators.iterator_to(iter));
        m_iterators.insert(iter);
    }

    // Reestablish the order when an iterator was incremented.
    void increment_position(raw_list_iterator_impl& iter) const {
        EXTPP_CHECK(iter.is_linked(), "Iterator is not linked.");
        auto self = m_iterators.iterator_to(iter);
        auto end = m_iterators.end();
        auto i = std::next(self);
        if (i != end && iter.get_position() > i->get_position()) {
            m_iterators.erase(self);
            do {
                ++i;
            } while (i != end && iter.get_position() > i->get_position());
            m_iterators.insert_before(i, iter);
        }
    }

    // Reestablish the order when an iterator was decremented.
    void decrement_position(raw_list_iterator_impl& iter) const {
        EXTPP_CHECK(iter.is_linked(), "Iterator is not linked.");

        auto self = m_iterators.iterator_to(iter);
        auto begin = m_iterators.begin();
        if (self == begin)
            return;

        auto i = std::prev(self);
        if (iter.get_position() < i->get_position()) {
            m_iterators.erase(self);
            while (i != begin) {
                auto prev = std::prev(i);
                if (iter.get_position() >= prev->get_position())
                    break;
                i = prev;
            }
            m_iterators.insert_before(i, iter);
        }
    }

private:
    // Returns a range over all iterators in node, starting with start_element (inclusive)
    // and ending with end_element (exclusive).
    auto iterator_range(block_index node_index, u32 start_element = 0, u32 end_element = -1) const {
        auto lower_pos = list_position(node_index, start_element);
        auto upper_pos = list_position(node_index, end_element);
        return std::pair(m_iterators.lower_bound(lower_pos),
                         m_iterators.upper_bound(upper_pos));
    }

    // Move iterators without changing the current node.
    // This function must be used in a way that preserves the relative ordering
    // of all iterators for that node.
    void move_iterators_in_node(block_index node_index, u32 start_index, u32 end_index, i32 delta) const
    {
        auto [pos, end] = iterator_range(node_index, start_index, end_index);
        for (; pos != end; ++pos) {
            pos->position_changed(pos->index() + delta);
        }
    }

    // Move iterators to a new node.
    void move_iterators_to_node(block_index old_node, u32 start_index, u32 end_index,
                                const raw_list_node& new_node, u32 dest_index) const
    {
        u32 diff = start_index - dest_index;
        auto [pos, end] = iterator_range(old_node, start_index, end_index);
        while (pos != end) {
            auto next = std::next(pos);
            pos->position_changed(new_node, pos->index() - diff);
            update_position(*pos);
            pos = next;
        }
    }

    void invalidate_iterators(block_index node, u32 index) const {
        auto [pos, end] = m_iterators.equal_range(list_position(node, index));
        m_iterators.erase_and_dispose(pos, end, [](raw_list_iterator_impl* iter) {
            iter->position_invalidated();
        });
    }

private:
    handle<anchor> m_anchor;
    u32 m_value_size;
    u32 m_node_capacity;

    // Contains every iterator that currently points to a valid list element.
    mutable iterator_map_type m_iterators;
};

class raw_list_visitor_impl {
public:
    raw_list_visitor_impl(const raw_list_impl& list)
        : m_list(&list)
    {
        move_to(m_list->first());
    }

    bool valid() const { return m_node.valid(); }

    raw_address prev_address() const {
        return raw_address::block_address(node().get_prev(), block_size());
    }

    raw_address next_address() const {
        return raw_address::block_address(node().get_next(), block_size());
    }

    raw_address address() const {
        return raw_address::block_address(node().block().index(), block_size());
    }

    u32 size() const { return node().get_size(); }

    const void* value(u32 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");
        return node().get(index);
    }

    void move_next() {
        move_to(node().get_next());
    }

    void move_prev() {
        move_to(node().get_prev());
    }

    void move_first() {
        move_to(m_list->first());
    }

    void move_last() {
        move_to(m_list->last());
    }

    void move_end() {
        move_to({});
    }

    u32 value_size() const { return m_list->value_size(); }

private:
    void move_to(block_index node_index);

    const raw_list_node& node() const {
        EXTPP_ASSERT(valid(), "Invalid node.");
        return m_node;
    }

    u32 block_size() const { return m_list->block_size(); }

private:
    const raw_list_impl* m_list;
    raw_list_node m_node;
};

void raw_list_visitor_impl::move_to(block_index node_index) {
    if (!node_index) {
        m_node = raw_list_node();
    } else {
        m_node = m_list->read_node(node_index);
    }
}

// --------------------------------
//
//   List implementation
//
// --------------------------------

block_index raw_list_impl::allocate_node() {
    block_index index = get_allocator().allocate(1).get_block_index(get_engine().block_size());
    m_anchor.set<&anchor::nodes>(nodes() + 1);
    return index;
}

void raw_list_impl::free_node(block_index index) {
    get_allocator().free(raw_address::block_address(index, get_engine().block_size()));
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

std::unique_ptr<raw_list_iterator_impl> raw_list_impl::begin() const {
    return !empty() ? std::make_unique<raw_list_iterator_impl>(this, read_node(first()), 0)
                    : end();
}

std::unique_ptr<raw_list_iterator_impl> raw_list_impl::end() const {
    return std::make_unique<raw_list_iterator_impl>(this);
}

std::unique_ptr<raw_list_visitor_impl> raw_list_impl::visit() const {
    return std::unique_ptr<raw_list_visitor_impl>(new raw_list_visitor_impl(*this));
}

void raw_list_impl::clear() {
    m_iterators.clear_and_dispose([](raw_list_iterator_impl* iter) {
        iter->position_invalidated();
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

void raw_list_impl::insert(const raw_list_iterator_impl& pos, const void* value) {
    EXTPP_ASSERT(pos.get_list() == this || !pos.valid(), "Iterator does not belong to this list.");

    if (empty())
        return insert_first(value);

    raw_list_node node;
    u32 index;
    if (!pos.valid()) {
        node = read_node(last());
        index = node.get_size();
    } else {
        node = pos.node();
        index = pos.index();
    }
    insert_at(std::move(node), index, value);
    // TODO Return position
}

void raw_list_impl::push_back(const void* value) {
    if (empty()) {
        insert_first(value);
        return;
    }

    raw_list_node node = read_node(last());
    u32 index = node.get_size();
    insert_at(std::move(node), index, value);
}

void raw_list_impl::push_front(const void* value) {
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

void raw_list_impl::insert_first(const void* value) {
    EXTPP_ASSERT(empty(), "list must be empty");

    raw_list_node node = create_node();
    node.set_size(1);
    node.set(0, value);

    m_anchor.set<&anchor::first>(node.index());
    m_anchor.set<&anchor::last>(node.index());
    m_anchor.set<&anchor::size>(1);
    // TODO
    // return iterator(std::move(node), 0);
}

void raw_list_impl::insert_at(raw_list_node node, u32 index, const void* value)
{
    EXTPP_ASSERT(index <= node.get_size(), "index is out of bounds");

    m_anchor.set<&anchor::size>(size() + 1);

    const u32 max = node_capacity();
    const u32 size = node.get_size();

    if (size < max) {
        raw_list_node::move(node, index, size, node, index + 1);
        node.set(index, value);
        node.set_size(size + 1);

        move_iterators_in_node(node.index(), index, -1, 1);

        // TODO
        // return iterator(std::move(node), index);
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

        move_iterators_to_node(node.index(), mid - 1, -1, new_node, 0);
        move_iterators_in_node(node.index(), index, -1, 1);
        // return iterator(std::move(node), index);
        return;
    } else {
        index -= mid;
        raw_list_node::move(node, mid, mid + index, new_node, 0);
        new_node.set(index, value);
        raw_list_node::move(node, mid + index, size, new_node, index + 1);

        move_iterators_to_node(node.index(), mid, mid + index, new_node, 0);
        move_iterators_to_node(node.index(), mid + index, size, new_node, index + 1);

        // return iterator(std::move(new_node), index);
        return;
    }
}

void raw_list_impl::erase(const raw_list_iterator_impl& pos) {
    EXTPP_ASSERT(pos.valid(), "Iterator is invalid.");
    EXTPP_ASSERT(pos.get_list() == this, "Iterator does not belong to this list.");

    // Must make a copy because pos will be invalidated.
    raw_list_node node = pos.node();
    u32 index = pos.index();
    erase_at(node, index);
}

void raw_list_impl::erase_at(const raw_list_node& node, u32 index) {
    u32 node_size = node.get_size();

    const u32 min_size = node_capacity() / 2;
    {
        raw_list_node::move(node, index + 1, node_size, node, index);

        invalidate_iterators(node.index(), index);
        move_iterators_in_node(node.index(), index + 1, -1, -1);

        node_size -= 1;
        node.set_size(node_size);
    }

    if (node_size >= min_size)
        return;

    // The first and the last node can become completely empty.
    if (node.index() == m_anchor.get<&anchor::first>()
            || node.index() == m_anchor.get<&anchor::last>())
    {
        if (node_size == 0)
            destroy_node(node);
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

        move_iterators_to_node(next.index(), 0, 1, node, node_size);
        move_iterators_in_node(next.index(), 1, -1, -1);

        node_size += 1;
        next_size -= 1;
        node.set_size(node_size);
        next.set_size(next_size);
    } else {
        raw_list_node::move(next, 0, next_size, node, node_size);
        move_iterators_to_node(next.index(), 0, -1, node, node_size);

        node_size += next_size;
        node.set_size(node_size);

        destroy_node(next);
    }
}

// --------------------------------
//
//   Iterator functions
//
// --------------------------------

raw_list_iterator_impl::raw_list_iterator_impl(const raw_list_impl* list, raw_list_node node, u32 index)
    : m_list(list)
    , m_node(std::move(node))
    , m_index(index)
{
    EXTPP_ASSERT(valid() && !at_end(), "Invalid iterator position.");
    m_list->insert_position(*this);
}

raw_list_iterator_impl::~raw_list_iterator_impl() {
    if (is_linked()) {
        m_list->erase_position(*this);
    }
}

void raw_list_iterator_impl::increment() {
    check_valid();

    if (at_end()) {
        if (auto first = m_list->first()) {
            m_node = m_list->read_node(first);
            m_index = 0;
            m_list->insert_position(*this);
        }
    } else {
        if (++m_index == m_node.get_size()) {
            if (auto next = m_node.get_next()) {
                m_node = m_list->read_node(next);
                m_index = 0;
                m_list->update_position(*this);
            } else {
                m_node = raw_list_node();
                m_index = 0;
                m_list->erase_position(*this);
            }
        } else {
            m_list->increment_position(*this);
        }
    }

    EXTPP_ASSERT(at_end() || m_index < m_node.get_size(), "Iterator invariants.");
}

void raw_list_iterator_impl::decrement() {
    check_valid();

    if (at_end()) {
        if (auto last = m_list->last()) {
            m_node = m_list->read_node(last);
            m_index = m_node.get_size() - 1;
            m_list->insert_position(*this);
        }
    } else {
        if (m_index-- == 0) {
            if (auto prev = m_node.get_prev()) {
                m_node = m_list->read_node(prev);
                m_index = m_node.get_size() - 1;
                m_list->update_position(*this);
            } else {
                m_node = raw_list_node();
                m_index = 0;
                m_list->erase_position(*this);
            }
        } else {
            m_list->decrement_position(*this);
        }
    }

    EXTPP_ASSERT(at_end() || m_index < m_node.get_size(), "Iterator invariants.");
}

// --------------------------------
//
//   List public interface
//
// --------------------------------

raw_list::raw_list(handle<anchor> anchor_, u32 value_size, allocator& alloc_)
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

raw_list_iterator raw_list::begin() const { return raw_list_iterator(impl().begin()); }

raw_list_iterator raw_list::end() const { return raw_list_iterator(impl().end()); }

raw_list_visitor raw_list::visit() const { return raw_list_visitor(impl().visit()); }

void raw_list::clear() { impl().clear(); }

void raw_list::push_front(const void *value) { impl().push_front(value); }

void raw_list::push_back(const void* value) { impl().push_back(value); }

void raw_list::pop_front() { impl().pop_front(); }

void raw_list::pop_back() { impl().pop_back(); }

raw_list_impl& raw_list::impl() {
    EXTPP_ASSERT(m_impl, "Invalid list.");
    return *m_impl;
}

const raw_list_impl& raw_list::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid list.");
    return *m_impl;
}

// --------------------------------
//
//   Visitor public interface
//
// --------------------------------

raw_list_visitor::raw_list_visitor(std::unique_ptr<raw_list_visitor_impl> impl)
    : m_impl(std::move(impl))
{}

raw_list_visitor::~raw_list_visitor() {}

raw_list_visitor::raw_list_visitor(raw_list_visitor&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_list_visitor& raw_list_visitor::operator=(raw_list_visitor&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_list_visitor_impl& raw_list_visitor::impl() {
    EXTPP_ASSERT(m_impl, "Visitor has been moved.");
    return *m_impl;
}

const raw_list_visitor_impl& raw_list_visitor::impl() const {
    EXTPP_ASSERT(m_impl, "Visitor has been moved.");
    return *m_impl;
}

bool raw_list_visitor::valid() const { return m_impl && impl().valid(); }

raw_address raw_list_visitor::prev_address() const { return impl().prev_address(); }

raw_address raw_list_visitor::next_address() const { return impl().next_address(); }

raw_address raw_list_visitor::address() const { return impl().address(); }

u32 raw_list_visitor::size() const { return impl().size(); }

u32 raw_list_visitor::value_size() const { return impl().value_size(); }

const void* raw_list_visitor::value(u32 index) const { return impl().value(index); }

void raw_list_visitor::move_next() { return impl().move_next(); }

void raw_list_visitor::move_prev() { return impl().move_prev(); }

void raw_list_visitor::move_first() { return impl().move_first(); }

void raw_list_visitor::move_last() { return impl().move_last(); }

// --------------------------------
//
//   Iterator public interface
//
// --------------------------------

raw_list_iterator::raw_list_iterator()
{}

raw_list_iterator::raw_list_iterator(const raw_list_iterator& other)
    : m_impl(other.m_impl ? std::make_unique<raw_list_iterator_impl>(*other.m_impl) : nullptr)
{}

raw_list_iterator::raw_list_iterator(raw_list_iterator&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_list_iterator::raw_list_iterator(std::unique_ptr<raw_list_iterator_impl> impl)
    : m_impl(std::move(impl))
{}

raw_list_iterator::~raw_list_iterator() {}

raw_list_iterator& raw_list_iterator::operator=(const raw_list_iterator& other) {
    if (this != &other) {
        m_impl = other.m_impl ? std::make_unique<raw_list_iterator_impl>(*other.m_impl) : nullptr;
    }
    return *this;
}

raw_list_iterator& raw_list_iterator::operator=(raw_list_iterator&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_list_iterator_impl& raw_list_iterator::impl() {
    EXTPP_ASSERT(m_impl, "Invalid iterator");
    return *m_impl;
}

const raw_list_iterator_impl& raw_list_iterator::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid iterator");
    return *m_impl;
}

bool raw_list_iterator::valid() const { return m_impl && impl().valid(); }

void raw_list_iterator::increment() { impl().increment(); }

void raw_list_iterator::decrement() { impl().decrement(); }

const void* raw_list_iterator::get() const { return impl().get(); }

bool raw_list_iterator::operator==(const raw_list_iterator& other) const {
    if (valid() != other.valid())
        return false;
    if (valid())
        return impl() == other.impl();
    return true;
}

} // namespace extpp
