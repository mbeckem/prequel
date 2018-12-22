#include <prequel/transaction_engine.hpp>

#include "engine/transaction_engine.hpp"

#include "engine/block.ipp"
#include "engine/engine_base.ipp"
#include "engine/journal.ipp"
#include "engine/transaction_engine.ipp"

namespace prequel {

transaction_engine::transaction_engine(file& dbfd, file& journalfd, u32 block_size,
                                       size_t cache_blocks)
    : engine(block_size)
    , m_impl(std::make_unique<detail::engine_impl::transaction_engine>(dbfd, journalfd, block_size,
                                                                       cache_blocks)) {}

transaction_engine::~transaction_engine() {}

file& transaction_engine::database_fd() const {
    return impl().dbfd();
}

file& transaction_engine::journal_fd() const {
    return impl().journalfd();
}

file_engine_stats transaction_engine::stats() const {
    return impl().stats();
}

bool transaction_engine::in_transaction() const {
    return impl().in_transaction();
}

void transaction_engine::begin() {
    impl().begin();
}

void transaction_engine::commit() {
    impl().commit();
}

void transaction_engine::rollback() {
    impl().rollback();
}

u64 transaction_engine::journal_size() const {
    return impl().journal_size();
}

bool transaction_engine::journal_has_changes() const {
    return impl().journal_has_changes();
}

void transaction_engine::checkpoint() {
    impl().checkpoint();
}

u64 transaction_engine::do_size() const {
    return impl().size();
}

void transaction_engine::do_grow(u64 n) {
    impl().grow(n);
}

void transaction_engine::do_flush() {
    impl().flush();
}

engine::pin_result transaction_engine::do_pin(block_index index, bool initialize) {
    detail::engine_impl::block* blk = impl().pin(index.value(), initialize);

    pin_result result;
    result.data = blk->data();
    result.cookie = reinterpret_cast<uintptr_t>(blk);
    return result;
}

void transaction_engine::do_unpin(block_index index, uintptr_t cookie) noexcept {
    impl().unpin(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

void transaction_engine::do_dirty(block_index index, uintptr_t cookie) {
    impl().dirty(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

void transaction_engine::do_flush(block_index index, uintptr_t cookie) {
    impl().flush(index.value(), reinterpret_cast<detail::engine_impl::block*>(cookie));
}

detail::engine_impl::transaction_engine& transaction_engine::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid engine instance.");
    return *m_impl;
}

} // namespace prequel
