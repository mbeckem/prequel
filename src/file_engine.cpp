#include <prequel/file_engine.hpp>

#include "engine/block.hpp"
#include "engine/block_cache.hpp"
#include "engine/block_dirty_set.hpp"
#include "engine/block_map.hpp"
#include "engine/block_pool.hpp"
#include "engine/file_engine.hpp"

#include "engine/block.ipp"
#include "engine/file_engine.ipp"

#include <exception>
#include <type_traits>

namespace prequel {

file_engine::file_engine(file& fd, u32 block_size, size_t cache_size)
    : engine(block_size)
    , m_impl(std::make_unique<detail::engine_impl::file_engine>(fd, block_size, cache_size)) {}

file_engine::~file_engine() {}

file& file_engine::fd() const {
    return impl().fd();
}

file_engine_stats file_engine::stats() const {
    return impl().stats();
}

u64 file_engine::do_size() const {
    return fd().file_size() >> block_size_log();
}

void file_engine::do_grow(u64 n) {
    u64 new_size_blocks = checked_add(do_size(), n);
    u64 new_size_bytes = checked_mul<u64>(new_size_blocks, block_size());
    fd().truncate(new_size_bytes);
}

void file_engine::do_flush() {
    impl().flush();
}

engine::pin_result file_engine::do_pin(block_index index, bool initialize) {
    detail::engine_impl::block* blk = impl().pin(index.value(), initialize);

    pin_result result;
    result.data = blk->data();
    result.cookie = reinterpret_cast<uintptr_t>(blk);
    return result;
}

void file_engine::do_unpin(block_index index, uintptr_t cookie) noexcept {
    impl().unpin(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

void file_engine::do_dirty(block_index index, uintptr_t cookie) {
    impl().dirty(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

void file_engine::do_flush(block_index index, uintptr_t cookie) {
    impl().flush(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

detail::engine_impl::file_engine& file_engine::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid engine instance.");
    return *m_impl;
}

} // namespace prequel
