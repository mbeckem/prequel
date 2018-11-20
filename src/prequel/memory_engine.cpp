#include <prequel/memory_engine.hpp>

#include <prequel/exception.hpp>

#include <vector>

namespace prequel {

namespace detail {

namespace {

struct free_deleter {
    void operator()(void* data) const {
        return std::free(data);
    }
};

using buffer_ptr = std::unique_ptr<byte, free_deleter>;

buffer_ptr create_buffer(u32 size) {
    byte* data = static_cast<byte*>(std::malloc(size));
    if (!data) {
        PREQUEL_THROW(io_error("Cannot grow the file: Out of memory."));
    }

    std::memset(data, 0, size);
    return buffer_ptr(data);
}

} // namespace

class memory_engine_impl {
public:
    memory_engine_impl(u32 block_size);
    ~memory_engine_impl();

    memory_engine_impl(memory_engine_impl&&) noexcept = delete;
    memory_engine_impl& operator=(const memory_engine_impl&&) = delete;

    byte* access(u64 index);

    void grow(u64 n);
    u64 size() const;

private:
    u32 m_block_size = 0;
    std::vector<buffer_ptr> m_blocks;
};

memory_engine_impl::memory_engine_impl(u32 block_size)
    : m_block_size(block_size)
{}

memory_engine_impl::~memory_engine_impl() {
}

byte* memory_engine_impl::access(u64 index) {
    if (index >= m_blocks.size()) {
        PREQUEL_THROW(io_error(
            fmt::format("Failed to access a block at index {}, beyond the end of file.", index)
        ));
    }

    return m_blocks[index].get();
}

u64 memory_engine_impl::size() const {
    return m_blocks.size();
}

void memory_engine_impl::grow(u64 n) {
    for (u64 i = 0; i < n; ++i) {
        m_blocks.push_back(create_buffer(m_block_size));
    }
}

} // namespace detail

memory_engine::memory_engine(u32 block_size)
    : engine(block_size)
    , m_impl(new detail::memory_engine_impl(block_size))
{}

memory_engine::~memory_engine() {}

u64 memory_engine::do_size() const { return impl().size(); }
void memory_engine::do_grow(u64 n) { impl().grow(n); }

void memory_engine::do_flush() {}

engine::pin_result memory_engine::do_pin(block_index index, bool initialize) {
    unused(initialize);

    pin_result result;
    result.data = impl().access(index.value());
    result.cookie = 0; /* unused */
    return result;
}

void memory_engine::do_unpin(block_index index, uintptr_t cookie) noexcept {
    unused(index, cookie);
}

void memory_engine::do_flush(block_index index, uintptr_t cookie) {
    unused(index, cookie);
}

void memory_engine::do_dirty(block_index index, uintptr_t cookie) {
    unused(index, cookie);
}

detail::memory_engine_impl& memory_engine::impl() const {
    if (!m_impl) {
        PREQUEL_THROW(bad_operation("Invalid engine instance."));
    }
    return *m_impl;
}

} // namespace prequel
