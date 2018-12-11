#include <prequel/container/stack.hpp>

#include <prequel/formatting.hpp>

#include <fmt/ostream.h>

#include <deque>

namespace prequel {

namespace detail {

/**
 * Layout:
 * - Header
 * - Array of values (header.size entries).
 */
class raw_stack_node {
private:
    struct header {
        // Previous node on the stack (they form a linked list).
        // Invalid if this is the bottom-most node.
        block_index prev;

        u32 size = 0;

        static constexpr auto get_binary_format() {
            return binary_format(&header::prev, &header::size);
        }
    };

public:
    raw_stack_node() = default;

    raw_stack_node(block_handle block, u32 value_size, u32 capacity)
        : m_handle(std::move(block), 0)
        , m_value_size(value_size)
        , m_capacity(capacity) {
        PREQUEL_ASSERT(m_capacity == compute_capacity(this->block().block_size(), m_value_size),
                       "Inconsistent precomputed capacity.");
    }

    const block_handle& block() const { return m_handle.block(); }
    block_index index() const { return block().index(); }
    explicit operator bool() const { return static_cast<bool>(block()); }

    void init() { m_handle.set(header()); }

    u32 get_size() const { return m_handle.get<&header::size>(); }
    void set_size(u32 new_size) const {
        PREQUEL_ASSERT(new_size <= m_capacity, "Invalid size.");
        m_handle.set<&header::size>(new_size);
    }

    block_index get_prev() const { return m_handle.get<&header::prev>(); }
    void set_prev(block_index new_prev) const { m_handle.set<&header::prev>(new_prev); }

    bool full() const { return get_size() == m_capacity; }
    bool empty() const { return get_size() == 0; }

    void push(const byte* value) const {
        PREQUEL_ASSERT(!full(), "Node is full.");

        u32 size = get_size();
        byte* data = block().writable_data();
        std::memmove(data + offset_of_index(size), value, m_value_size);
        set_size(size + 1);
    }

    void pop() const {
        PREQUEL_ASSERT(!empty(), "Node is empty.");
        set_size(get_size() - 1);
    }

    const byte* get(u32 index) const {
        PREQUEL_ASSERT(index < get_size(), "Index out of bounds.");

        const byte* data = block().data();
        return data + offset_of_index(index);
    }

    const byte* top() const {
        PREQUEL_ASSERT(!empty(), "Node is empty.");
        return get(get_size() - 1);
    }

public:
    static u32 compute_capacity(u32 block_size, u32 value_size) {
        u32 hdr_size = serialized_size<header>();
        if (block_size <= hdr_size)
            return 0;

        return (block_size - hdr_size) / value_size;
    }

private:
    u32 offset_of_index(u32 value_index) const {
        return serialized_size<header>() + m_value_size * value_index;
    }

private:
    handle<header> m_handle;
    u32 m_value_size = 0;
    u32 m_capacity = 0;
};

class raw_stack_impl : public uses_allocator {
public:
    using anchor = detail::raw_stack_anchor;

    raw_stack_impl(anchor_handle<anchor> anchor_, u32 value_size, allocator& alloc_);

    ~raw_stack_impl();

    raw_stack_impl(const raw_stack_impl&) = delete;
    raw_stack_impl& operator=(const raw_stack_impl&) = delete;

    bool empty() const { return size() == 0; }
    u64 size() const { return m_anchor.get<&anchor::size>(); }
    u64 nodes() const { return m_anchor.get<&anchor::nodes>(); }

    u32 value_size() const { return m_value_size; }
    u32 node_capacity() const { return m_node_capacity; }
    block_index top_node() const { return m_anchor.get<&anchor::top>(); }

    void top(byte* value) const;
    void push(const byte* value);
    void pop();

    void clear();
    void reset();

    void validate() const;
    void dump(std::ostream& os) const;

private:
    raw_stack_node read_node(block_index index) const;

    raw_stack_node create_node();
    void free_node(block_index index);

    void check_buffer_invariants() const;

private:
    void set_size(u64 new_size) { m_anchor.set<&anchor::size>(new_size); }
    void set_nodes(u64 new_nodes) { m_anchor.set<&anchor::nodes>(new_nodes); }
    void set_top_node(block_index new_top) { m_anchor.set<&anchor::top>(new_top); }

private:
    anchor_handle<anchor> m_anchor;
    u32 m_value_size = 0;
    u32 m_node_capacity = 0;

    // Buffer topmost two blocks for efficiency.
    // (Ab-) using deque as a trivial ringbuffer of capacity 2.
    std::deque<raw_stack_node> m_buf;
};

raw_stack_impl::raw_stack_impl(anchor_handle<anchor> anchor_, u32 value_size, allocator& alloc_)
    : uses_allocator(alloc_)
    , m_anchor(std::move(anchor_))
    , m_value_size(value_size)
    , m_node_capacity(raw_stack_node::compute_capacity(get_engine().block_size(), m_value_size)) {
    if (m_node_capacity == 0)
        PREQUEL_THROW(bad_argument("Block size is too small to fit a single value."));

    // Populate the node buffer.
    if (block_index top = top_node()) {
        m_buf.push_front(read_node(top));
        if (block_index prev = m_buf.front().get_prev())
            m_buf.push_front(read_node(prev));
    }
}

raw_stack_impl::~raw_stack_impl() {}

void raw_stack_impl::top(byte* value) const {
    if (!value)
        PREQUEL_THROW(bad_argument("Invalid value buffer."));
    if (empty())
        PREQUEL_THROW(bad_operation("The stack is empty."));

    check_buffer_invariants();
    const raw_stack_node& node = m_buf.size() > 1 && !m_buf.back().empty() ? m_buf.back()
                                                                           : m_buf.front();
    std::memmove(value, node.top(), m_value_size);
}

void raw_stack_impl::push(const byte* value) {
    if (!value)
        PREQUEL_THROW(bad_argument("Invalid value buffer."));

    check_buffer_invariants();

    auto append_node = [&](block_index prev) {
        raw_stack_node node = create_node();
        node.set_prev(prev);
        set_top_node(node.index());
        return node;
    };

    // Make a new node if the last node in the buffer is full and make sure
    // that the capacity is limited at 2. Note that because of the pop() behaviour,
    // the last AND the second to last nodes may be non-full.
    if (m_buf.size() == 0) {
        m_buf.push_back(append_node(block_index()));
    } else {
        const raw_stack_node& back = m_buf.back();
        if (back.full()) {
            m_buf.push_back(append_node(back.index()));
            if (m_buf.size() > 2)
                m_buf.pop_front();
        }
    }

    raw_stack_node& node = !m_buf.front().full() ? m_buf.front() : m_buf.back();
    node.push(value);
    set_size(size() + 1);
}

void raw_stack_impl::pop() {
    if (empty())
        PREQUEL_THROW(bad_operation("The stack is empty."));

    check_buffer_invariants();

    // The second node is allowed to become completely empty to avoid trashing.
    if (m_buf.size() > 1 && !m_buf.back().empty()) {
        m_buf.back().pop();
        set_size(size() - 1);
        return;
    }

    PREQUEL_ASSERT(!m_buf.front().empty(), "The first buffered block cannot be empty.");
    const raw_stack_node& first = m_buf.front();
    first.pop();
    set_size(size() - 1);

    if (first.empty()) {
        if (m_buf.size() > 1) {
            const raw_stack_node& second = m_buf.back();
            free_node(second.index());
            m_buf.pop_back();

            set_top_node(first.index());
        }

        if (block_index prev = first.get_prev()) {
            // Load predecessor node.
            m_buf.push_front(read_node(prev));
        } else {
            // Stack has become completely empty.
            free_node(first.index());
            m_buf.clear();
            set_top_node(block_index());
        }
    }
}

void raw_stack_impl::clear() {
    reset();
}

void raw_stack_impl::reset() {
    for (block_index index = top_node(); index;) {
        raw_stack_node node = read_node(index);
        index = node.get_prev();
        free_node(node.index());
    }

    m_buf.clear();
    set_top_node(block_index());
    set_size(0);
    set_nodes(0);
}

void raw_stack_impl::validate() const {
#define ERROR(...) PREQUEL_THROW(corruption_error(fmt::format("validate: " __VA_ARGS__)));

    u64 total_values = 0;
    u64 total_nodes = 0;

    if (empty() && top_node())
        ERROR("Empty stack has a valid top node.");

    if (!empty() && !top_node())
        ERROR("Non-empty stack has an invalid top node.");

    for (block_index index = top_node(); index;) {
        raw_stack_node node = read_node(index);

        if (node.get_size() > m_node_capacity)
            ERROR("Node has more entries than the capacity allows.");
        if (index != top_node() && node.empty())
            ERROR("Non-top stack node is empty.");

        total_nodes += 1;
        total_values += node.get_size();
        index = node.get_prev();
    }

    if (total_values != size())
        ERROR("Value count does not match the stack's size.");

    if (total_nodes != nodes())
        ERROR("Node count does not match the stack's state.");

#undef ERROR
}

void raw_stack_impl::dump(std::ostream& os) const {
    fmt::print(os,
               "Raw stack:\n"
               "  Value size: {}\n"
               "  Block size: {}\n"
               "  Node Capacity: {}\n"
               "  Size: {}\n"
               "  Nodes: {}\n",
               value_size(), get_engine().block_size(), node_capacity(), size(), nodes());

    if (!empty())
        os << "\n";

    for (block_index index = top_node(); index;) {
        raw_stack_node node = read_node(index);

        fmt::print(os,
                   "  Node @{}:\n"
                   "    Previous: @{}\n"
                   "    Size: {}\n",
                   node.index(), node.get_prev(), node.get_size());

        const u32 size = node.get_size();
        for (u32 i = size; i-- > 0;) {
            const byte* data = static_cast<const byte*>(node.get(i));
            fmt::print(os, "    {:>4}: {}\n", i, format_hex(data, value_size()));
        }
        fmt::print(os, "\n");

        index = node.get_prev();
    }
}

raw_stack_node raw_stack_impl::read_node(block_index index) const {
    return raw_stack_node(get_engine().read(index), m_value_size, m_node_capacity);
}

raw_stack_node raw_stack_impl::create_node() {
    block_index index = get_allocator().allocate(1);
    raw_stack_node node(get_engine().overwrite_zero(index), m_value_size, m_node_capacity);
    node.init();
    set_nodes(nodes() + 1);
    return node;
}

void raw_stack_impl::free_node(block_index index) {
    PREQUEL_ASSERT(nodes() > 0, "Inconsistent node counter.");
    get_allocator().free(index, 1);
    set_nodes(nodes() - 1);
}

// Checks invariants in debug builds only.
void raw_stack_impl::check_buffer_invariants() const {
#ifdef PREQUEL_DEBUG
    if (empty()) {
        PREQUEL_ASSERT(m_buf.empty(), "No blocks can be buffered.");
        return;
    }

    if (nodes() == 1) {
        PREQUEL_ASSERT(m_buf[0] && m_buf[0].index() == top_node(), "Must buffer the last node.");
        PREQUEL_ASSERT(m_buf.size() == 1, "Must not buffer any other node.");
        return;
    }

    PREQUEL_ASSERT(m_buf.size() == 2, "Must buffer two nodes.");
    PREQUEL_ASSERT(m_buf[1] && m_buf[1].index() == top_node(), "Must buffer the last node.");
    PREQUEL_ASSERT(m_buf[0] && m_buf[0].index() == m_buf[1].get_prev(),
                   "Must buffer the second to last node.");
    PREQUEL_ASSERT(m_buf[0].index() == m_buf[1].get_prev(),
                   "Last node must point to second to last node");
    PREQUEL_ASSERT(!m_buf[0].empty(), "The first buffered block cannot be empty.");
#endif
}

} // namespace detail

// --------------------------------
//
//   Stack public interface
//
// --------------------------------

raw_stack::raw_stack(anchor_handle<anchor> anchor_, u32 value_size, allocator& alloc_)
    : m_impl(std::make_unique<detail::raw_stack_impl>(std::move(anchor_), value_size, alloc_)) {}

raw_stack::~raw_stack() {}

raw_stack::raw_stack(raw_stack&& other) noexcept
    : m_impl(std::move(other.m_impl)) {}

raw_stack& raw_stack::operator=(raw_stack&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

engine& raw_stack::get_engine() const {
    return impl().get_engine();
}
allocator& raw_stack::get_allocator() const {
    return impl().get_allocator();
}
u32 raw_stack::value_size() const {
    return impl().value_size();
}
u32 raw_stack::node_capacity() const {
    return impl().node_capacity();
}
bool raw_stack::empty() const {
    return impl().empty();
}
u64 raw_stack::size() const {
    return impl().size();
}
u64 raw_stack::nodes() const {
    return impl().nodes();
}

double raw_stack::fill_factor() const {
    return empty() ? 0 : double(size()) / (nodes() * node_capacity());
}

u64 raw_stack::byte_size() const {
    return nodes() * get_engine().block_size();
}

double raw_stack::overhead() const {
    return empty() ? 1.0 : double(byte_size()) / (size() * value_size());
}

void raw_stack::top(byte* value) const {
    impl().top(value);
}
void raw_stack::push(const byte* value) {
    impl().push(value);
}
void raw_stack::pop() {
    impl().pop();
}
void raw_stack::clear() {
    impl().clear();
}
void raw_stack::reset() {
    impl().reset();
}

void raw_stack::validate() const {
    impl().validate();
}
void raw_stack::dump(std::ostream& os) const {
    impl().dump(os);
}

detail::raw_stack_impl& raw_stack::impl() const {
    if (!m_impl)
        PREQUEL_THROW(bad_operation("Invalid stack instance."));
    return *m_impl;
}

} // namespace prequel
