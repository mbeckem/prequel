#include <prequel/container/extent.hpp>

#include <prequel/exception.hpp>

namespace prequel {

namespace detail {

class extent_impl : public uses_allocator {
public:
    using anchor = extent_anchor;

    extent_impl(anchor_handle<anchor> _anchor, allocator& _alloc)
        : uses_allocator(_alloc)
        , m_anchor(std::move(_anchor))
        , m_block_size(_alloc.block_size()) {}

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

    block_handle read(u64 index) const { return get_engine().read(get(index)); }

    block_handle overwrite_zero(u64 index) const { return get_engine().overwrite_zero(get(index)); }

    block_handle overwrite(u64 index, const byte* data, size_t data_size) const {
        return get_engine().overwrite(get(index), data, data_size);
    }

    void clear() {
        if (empty())
            return;

        get_allocator().free(data(), size());
        m_anchor.set<&anchor::start>(block_index());
        m_anchor.set<&anchor::size>(0);
    }

    void resize(u64 new_size) {
        if (new_size == size())
            return;

        block_index new_data = get_allocator().reallocate(data(), size(), new_size);
        m_anchor.set<&anchor::start>(new_data);
        m_anchor.set<&anchor::size>(new_size);
    }

private:
    void check_index(u64 index) const {
        if (index >= size())
            PREQUEL_THROW(bad_argument("Index out of bounds."));
    }

private:
    anchor_handle<anchor> m_anchor;

    u32 m_block_size = 0;
};

} // namespace detail

// --------------------------------
//
//   Extent public interface
//
// --------------------------------

extent::extent(anchor_handle<anchor> _anchor, allocator& alloc)
    : m_impl(std::make_unique<detail::extent_impl>(std::move(_anchor), alloc)) {}

extent::~extent() {}

extent::extent(extent&& other) noexcept
    : m_impl(std::move(other.m_impl)) {}

extent& extent::operator=(extent&& other) noexcept {
    if (this != &other)
        m_impl = std::move(other.m_impl);
    return *this;
}

engine& extent::get_engine() const {
    return impl().get_engine();
}
allocator& extent::get_allocator() const {
    return impl().get_allocator();
}
u32 extent::block_size() const {
    return impl().block_size();
}

bool extent::empty() const {
    return impl().empty();
}
u64 extent::size() const {
    return impl().size();
}
u64 extent::byte_size() const {
    return size() * block_size();
}

block_index extent::data() const {
    return impl().data();
}
block_index extent::get(u64 index) const {
    return impl().get(index);
}
block_handle extent::read(u64 index) const {
    return impl().read(index);
}
block_handle extent::overwrite_zero(u64 index) const {
    return impl().overwrite_zero(index);
}
block_handle extent::overwrite(u64 index, const byte* data, size_t data_size) const {
    return impl().overwrite(index, data, data_size);
}
void extent::clear() {
    impl().clear();
}
void extent::reset() {
    impl().clear();
}
void extent::resize(u64 new_size) {
    impl().resize(new_size);
}

detail::extent_impl& extent::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid list.");
    return *m_impl;
}

} // namespace prequel
