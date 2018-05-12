#include <extpp/extent.hpp>

#include <extpp/exception.hpp>

namespace extpp {

class extent_impl : public uses_allocator {
public:
    using anchor = extent_anchor;

    extent_impl(handle<anchor> _anchor, allocator& _alloc)
        : uses_allocator(_alloc)
        , m_anchor(std::move(_anchor))
        , m_block_size(_alloc.block_size())
    {}

    ~extent_impl() = default;

    extent_impl(const extent_impl&) = delete;
    extent_impl& operator=(const extent_impl&) = delete;

public:
    u32 block_size() const { return m_block_size; }

    bool empty() const { return size() == 0; }
    u64 size() const { return m_anchor.get<&anchor::size>(); }
    block_index data() const { return m_anchor.get<&anchor::start>(); }

    block_index get(u64 index) const {
        check_index(index);
        return data() + index;
    }

    block_handle read(u64 index) const {
        return get_engine().read(get(index));
    }

    block_handle zeroed(u64 index) const {
        return get_engine().zeroed(get(index));
    }

    block_handle overwritten(u64 index, const byte* data, size_t data_size) const {
        return get_engine().overwritten(get(index), data, data_size);
    }

    void clear() {
        if (empty())
            return;

        get_allocator().free(addr());
        m_anchor.set<&anchor::start>(block_index());
        m_anchor.set<&anchor::size>(0);
    }

    void resize(u64 new_size) {
        if (new_size == size())
            return;

        raw_address new_addr = get_allocator().reallocate(addr(), new_size);
        m_anchor.set<&anchor::start>(new_addr.get_block_index(block_size()));
        m_anchor.set<&anchor::size>(new_size);
    }

    // TODO: Sucks
    raw_address addr() const { return raw_address::block_address(data(), block_size()); }

private:
    void check_index(u64 index) const {
        if (index >= size())
            EXTPP_THROW(bad_element());
    }

private:
    handle<anchor> m_anchor;

    u32 m_block_size = 0;
};

// --------------------------------
//
//   Extent public interface
//
// --------------------------------

extent::extent(handle<anchor> _anchor, allocator& alloc)
    : m_impl(std::make_unique<extent_impl>(std::move(_anchor), alloc))
{}

extent::~extent() {}

extent::extent(extent&& other) noexcept
    : m_impl(std::move(other.m_impl)) {}

extent& extent::operator=(extent&& other) noexcept {
    if (this != &other)
        m_impl = std::move(other.m_impl);
    return *this;
}

engine& extent::get_engine() const { return impl().get_engine(); }
allocator& extent::get_allocator() const { return impl().get_allocator(); }
u32 extent::block_size() const { return impl().block_size(); }

bool extent::empty() const { return impl().empty(); }
u64 extent::size() const { return impl().size(); }
block_index extent::data() const { return impl().data(); }
block_index extent::get(u64 index) const { return impl().get(index); }
block_handle extent::read(u64 index) const { return impl().read(index); }
block_handle extent::zeroed(u64 index) const { return impl().zeroed(index); }
block_handle extent::overwritten(u64 index, const byte* data, size_t data_size) const { return impl().overwritten(index, data, data_size); }
void extent::clear() { impl().clear(); }
void extent::resize(u64 new_size) { impl().resize(new_size); }

extent_impl& extent::impl() const {
    EXTPP_ASSERT(m_impl, "Invalid list.");
    return *m_impl;
}

} // namespace extpp
