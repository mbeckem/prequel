#include <extpp/detail/free_list.hpp>

namespace extpp {
namespace detail {
namespace {

struct free_list_header {
    block_index next;
    u32 size = 0;

    static constexpr auto get_binary_format() {
        return make_binary_format(&free_list_header::next,
                                  &free_list_header::size);
    }
};

class free_list_node {
public:
    free_list_node(block_handle handle, u32 capacity)
        : m_handle(std::move(handle), 0)
        , m_capacity(capacity)
    {
        EXTPP_ASSERT(capacity > 0, "Invalid capacity.");
    }

    const block_handle& block() const { return m_handle.block(); }

    void init() {
        m_handle.set(free_list_header());
    }

    bool full() const { return get_size() == m_capacity; }
    bool empty() const { return get_size() == 0; }

    block_index get_next() const { return m_handle.get<&free_list_header::next>(); }
    void set_next(block_index index) {
        m_handle.set<&free_list_header::next>(index);
    }

    void push(block_index block) {
        EXTPP_ASSERT(!full(), "Node is already full.");
        u32 size = get_size();
        set_value(size, block);
        set_size(size + 1);
    }

    block_index pop() {
        EXTPP_ASSERT(!empty(), "Node is already empty.");
        u32 size = get_size();
        block_index result = get_value(size - 1);
        set_size(size - 1);
        return result;
    }

private:
    u32 offset_of_value(u32 index) const {
        return header_size() + value_size() * index;
    }

    u32 get_size() const { return m_handle.get<&free_list_header::size>(); }
    void set_size(u32 new_size) {
        EXTPP_ASSERT(new_size <= m_capacity, "Invalid size.");
        m_handle.set<&free_list_header::size>(new_size);
    }

    block_index get_value(u32 index) const {
        EXTPP_ASSERT(index <= m_capacity, "Invalid index");
        return m_handle.block().get<block_index>(offset_of_value(index));
    }

    void set_value(u32 index, block_index value) {
        EXTPP_ASSERT(index <= m_capacity, "Invalid index");
        m_handle.block().set(offset_of_value(index), value);
    }

public:
    static u32 header_size() {
        return serialized_size<free_list_header>();
    }

    static u32 value_size() {
        return serialized_size<block_index>();
    }

    static u32 capacity(u32 block_size) {
        if (header_size() >= block_size)
            return 0;

        return (block_size - header_size()) / value_size();
    }

private:
    handle<free_list_header> m_handle;
    u32 m_capacity;
};

} // namespace

free_list::free_list(anchor_handle<anchor> anchor_, engine& engine_)
    : m_anchor(std::move(anchor_))
    , m_engine(&engine_)
    , m_block_capacity(free_list_node::capacity(m_engine->block_size()))
{
    EXTPP_CHECK(m_block_capacity > 0, "Blocks are too small.");
}

bool free_list::empty() const {
    return !m_anchor.get<&anchor::head>().valid();
}

void free_list::push(block_index block) {
    block_index head = m_anchor.get<&anchor::head>();
    if (head) {
        free_list_node node(m_engine->read(head), block_capacity());
        if (!node.full()) {
            node.push(block);
            return;
        }
    }

    free_list_node node(m_engine->overwrite_zero(block), block_capacity());
    node.init();
    node.set_next(head);
    m_anchor.set<&anchor::head>(block);
}

block_index free_list::pop() {
    block_index head = m_anchor.get<&anchor::head>();
    if (!head)
        EXTPP_THROW(bad_operation("Freelist is empty."));

    free_list_node node(m_engine->read(head), block_capacity());
    if (!node.empty()) {
        return node.pop();
    }

    // use the empty list node itself to satisfy the request.
    m_anchor.set<&anchor::head>(node.get_next());
    return head;
}

} // namespace detail
} // namespace extpp
