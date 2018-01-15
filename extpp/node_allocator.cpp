#include <extpp/node_allocator.hpp>

#include <extpp/exception.hpp>

namespace extpp {

node_allocator::node_allocator(handle<anchor> anchor_, engine& engine_)
    : allocator(engine_)
    , m_anchor(std::move(anchor_))
    , m_list(m_anchor.member<&anchor::list>(), engine_)
{}

void node_allocator::chunk_size(u32 size) {
    EXTPP_CHECK(size >= 1, "Invalid chunk size, must be greater than 0.");
    m_chunk_size = size;
}

u64 node_allocator::data_total() const {
    return m_anchor.get<&anchor::total>();
}

u64 node_allocator::data_used() const {
    return data_total() - data_free();
}

u64 node_allocator::data_free() const {
    return m_anchor.get<&anchor::free>();
}

raw_address node_allocator::do_allocate(u64 n) {
    if (n != 1)
        EXTPP_THROW(unsupported("The node_allocator does not support allocation sizes other than 1."));

    if (m_list.empty()) {
        block_index begin(get_engine().size());
        block_index end(begin + m_chunk_size);
        get_engine().grow(m_chunk_size);
        for (auto i = end; i-- > begin; )
            m_list.push(i);

        m_anchor.set<&anchor::total>(m_anchor.get<&anchor::total>() + m_chunk_size);
        m_anchor.set<&anchor::free>(m_anchor.get<&anchor::free>() + m_chunk_size);
    }

    m_anchor.set<&anchor::free>(m_anchor.get<&anchor::free>() - 1);
    return raw_address::block_address(m_list.pop(), block_size());
}

raw_address node_allocator::do_reallocate(raw_address a, u64 n) {
    unused(a, n);
    EXTPP_THROW(unsupported("The node_allocator does not support reallocation."));
}

void node_allocator::do_free(raw_address a) {
    m_anchor.set<&anchor::free>(m_anchor.get<&anchor::free>() + 1);
    m_list.push(a.get_block_index(block_size()));
}

} // namespace extpp
