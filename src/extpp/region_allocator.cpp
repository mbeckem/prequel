#include <extpp/region_allocator.hpp>

#include <fmt/ostream.h>

namespace extpp {

region_allocator::region_allocator(anchor_handle<anchor> _anchor, engine& _engine)
    : allocator(_engine)
    , m_anchor(std::move(_anchor))
    , m_source(this)
    , m_alloc(m_anchor.member<&anchor::alloc>(), _engine, m_source)
{}

void region_allocator::initialize(block_index begin, u64 capacity) {
    if (this->begin()) {
        EXTPP_THROW(invalid_argument("Region allocator was already initialized."));
    }
    if (!begin || capacity == 0) {
        EXTPP_THROW(invalid_argument("Region must not be empty."));
    }

    m_anchor.set<&anchor::begin>(begin);
    m_anchor.set<&anchor::size>(capacity);
}

block_index region_allocator::begin() const {
    return m_anchor.get<&anchor::begin>();
}

u64 region_allocator::used() const {
    return m_anchor.get<&anchor::used>();
}

u64 region_allocator::size() const {
    return m_anchor.get<&anchor::size>();
}

u32 region_allocator::min_chunk() const {
    return m_alloc.min_chunk();
}

void region_allocator::min_chunk(u32 chunk_size) {
    m_alloc.min_chunk(chunk_size);
}

u32 region_allocator::min_meta_chunk() const {
    return m_alloc.min_meta_chunk();
}

void region_allocator::min_meta_chunk(u32 chunk_size) {
    m_alloc.min_meta_chunk(chunk_size);
}

void region_allocator::dump(std::ostream& os) const {
    fmt::print(os,
               "Region allocator state:\n"
               "  Region start: {}\n"
               "  Region size: {}\n"
               "  Used blocks: {}\n"
               "\n",
               begin(), size(), used());

    m_alloc.dump(os);
}

void region_allocator::validate() const {
    if (begin()) {
        if (size() == 0) {
            EXTPP_THROW(corruption_error("Region was initialized with zero size."));
        }
        if (used() > size()) {
            EXTPP_THROW(corruption_error("More blocks used than are available."));
        }
    } else {
        if (size() != 0) {
            EXTPP_THROW(corruption_error("Uninitialized region with non-zero size."));
        }
        if (used() != 0) {
            EXTPP_THROW(corruption_error("Uninitialized region with non-zero used blocks."));
        }
    }

    m_alloc.validate();
}

block_index region_allocator::do_allocate(u64 n) {
    check_initialized();
    return m_alloc.allocate(n);
}

block_index region_allocator::do_reallocate(block_index a, u64 n) {
    check_initialized();
    return m_alloc.reallocate(a, n);
}

void region_allocator::do_free(block_index a) {
    check_initialized();
    return m_alloc.free(a);
}

void region_allocator::check_initialized() const {
    if (!begin()) {
        EXTPP_THROW(bad_alloc("Region allocator has no region. Call initialize() first."));
    }
}

region_allocator::allocator_source::allocator_source(region_allocator* parent)
    : m_parent(parent)
{}

region_allocator::allocator_source::~allocator_source() {}

block_index region_allocator::allocator_source::begin() {
    return m_parent->begin();
}

u64 region_allocator::allocator_source::available() {
    return m_parent->size() - m_parent->used();
}

u64 region_allocator::allocator_source::size() {
    return m_parent->used();
}

void region_allocator::allocator_source::grow(u64 n) {
    if (n > available()) {
        EXTPP_THROW(bad_alloc("Not enough space left in region."));
    }
    m_parent->m_anchor.set<&anchor::used>(m_parent->used() + n);
}

} // namespace extpp
