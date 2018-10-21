#include <prequel/memory_engine.hpp>

#include <prequel/exception.hpp>

#include <new>
#include <vector>

namespace prequel {

namespace detail {
namespace {

// Implements the block handle interface.
// Note that the data array is allocated immediately after this struct
// to save one indirection.
class block final : public detail::block_handle_impl {
public:
    block(u64 index, u32 block_size)
        : m_index(index)
        , m_block_size(block_size)
    {
        std::memset(data_ptr(), 0, block_size);
    }

public:
    // Interface implementation.
    void handle_ref() override {}
    void handle_unref() override {}

    u64 index() const noexcept override {
        return m_index;
    }

    const byte* data() const noexcept override {
        return data_ptr();
    }

    byte* writable_data() override {
        return data_ptr();
    }

private:
    byte* data_ptr() const {
        return reinterpret_cast<byte*>(const_cast<block*>(this) + 1);
    }

private:
    u64 m_index = 0;
    u32 m_block_size = 0;
};

static block* allocate_block(u64 index, u32 block_size) {
    void* addr = ::operator new(sizeof(block) + block_size);
    try {
        block* blk = new (addr) block(index, block_size);
        return blk;
    } catch (...) {
        ::operator delete(addr);
        throw;
    }
}

static void destroy_block(block* blk) noexcept {
    blk->~block();
    ::operator delete(reinterpret_cast<void*>(blk));
}

} // namespace

class memory_engine_impl {
public:
    memory_engine_impl(u32 block_size);
    ~memory_engine_impl();

    memory_engine_impl(memory_engine_impl&&) noexcept = delete;
    memory_engine_impl& operator=(const memory_engine_impl&&) = delete;

    block* access(u64 index);
    block* overwrite(u64 index, const byte* data);
    block* overwrite_zero(u64 index);

    void grow(u64 n);
    u64 size() const;

private:
    u32 m_block_size = 0;
    std::vector<block*> m_blocks;
};

memory_engine_impl::memory_engine_impl(u32 block_size)
    : m_block_size(block_size)
{}

memory_engine_impl::~memory_engine_impl() {
    for (block* b : m_blocks)
        destroy_block(b);
}

block* memory_engine_impl::access(u64 index) {
    if (index >= m_blocks.size()) {
        PREQUEL_THROW(io_error(
            fmt::format("Failed to access a block at index {}, beyond the end of file.", index)
        ));
    }

    return m_blocks[index];
}

block* memory_engine_impl::overwrite(u64 index, const byte* data) {
    block* blk = access(index);
    std::memmove(blk->writable_data(), data, m_block_size);
    return blk;
}

block* memory_engine_impl::overwrite_zero(u64 index) {
    block* blk = access(index);
    std::memset(blk->writable_data(), 0, m_block_size);
    return blk;
}

u64 memory_engine_impl::size() const {
    return m_blocks.size();
}

void memory_engine_impl::grow(u64 n) {
    u64 size = this->size();
    for (u64 i = 0; i < n; ++i) {
        m_blocks.push_back(allocate_block(size + i, m_block_size));
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

block_handle memory_engine::do_access(block_index index) {
    return block_handle(this, impl().access(index.value()));
}

block_handle memory_engine::do_read(block_index index) {
    return block_handle(this, impl().access(index.value()));
}

block_handle memory_engine::do_overwrite_zero(block_index index) {
    return block_handle(this, impl().overwrite_zero(index.value()));
}

block_handle memory_engine::do_overwrite(block_index index, const byte* data) {
    return block_handle(this, impl().overwrite(index.value(), data));
}

void memory_engine::do_flush() {}

detail::memory_engine_impl& memory_engine::impl() const {
    if (!m_impl) {
        PREQUEL_THROW(bad_operation("Invalid engine instance."));
    }
    return *m_impl;
}

} // namespace prequel
