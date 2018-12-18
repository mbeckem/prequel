#ifndef PREQUEL_TRANSACTION_ENGINE_HPP
#define PREQUEL_TRANSACTION_ENGINE_HPP

#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/file_engine.hpp> // TODO for stats only.

#include <memory>

namespace prequel {

namespace detail::engine_impl {

class transaction_engine;

} // namespace detail::engine_impl

class transaction_engine final : public engine {
public:
    transaction_engine(file& dbfd, file& journalfd, u32 block_size, size_t cache_size);
    ~transaction_engine();

    file& database_fd() const;
    file& journal_fd() const;

    file_engine_stats stats() const;

    /**
     * Returns true if a transaction is currently active (i.e. begin() without commit() or rollback()).
     */
    bool in_transaction() const;

    /**
     * Begins a new transaction. Every transaction must be terminated using
     * either commit() or rollback() before a new transaction can be started.
     * A transaction that is interrupted by a crash (e.g. power loss) will be
     * rolled back implicitly.
     */
    void begin();

    /**
     * Commits all changes made during the current transaction.
     * Requires that a transaction has been started earlier using begin().
     *
     * You must drop all references to blocks read through this engine before
     * you can commit a transaction (i.e. destroy all datastructure handles etc.).
     *
     * Note that read-only transactions (i.e. transaction that never modify the database)
     * are free in terms of disk I/O.
     */
    void commit();

    /**
     * Rolls back all changes made during the current transaction.
     * Requires that a transaction has been started earlier using begin().
     *
     * You must drop all references to blocks read through this engine before
     * you can commit a transaction (i.e. destroy all datastructure handles etc.).
     */
    void rollback();

    /**
     * Returns true if the journal contains committed changes that have not yet been committed
     * to the database file. They can be transferred over using the `checkpoint()` function.
     */
    bool journal_has_changes() const;

    /**
     * Returns the current size of the journal, in bytes. This is usually larger
     * than the size of the journal on disk because of buffered changes that have not yet been
     * flushed to storage.
     */
    u64 journal_size() const;

    /**
     * Transfers all changes (all committed transactions) from the journal file
     * to the main database file, then resets the size of the journal.
     * This function should be invoked when the journal has become too large.
     *
     * There must *not* be an active transaction.
     */
    void checkpoint();

private:
    u64 do_size() const override;
    void do_grow(u64 n) override;
    void do_flush() override;

    pin_result do_pin(block_index index, bool initialize) override;
    void do_unpin(block_index index, uintptr_t cookie) noexcept override;
    void do_dirty(block_index index, uintptr_t cookie) override;
    void do_flush(block_index index, uintptr_t cookie) override;

private:
    detail::engine_impl::transaction_engine& impl() const;

private:
    std::unique_ptr<detail::engine_impl::transaction_engine> m_impl;
};

} // namespace prequel

#endif // PREQUEL_TRANSACTION_ENGINE_HPP
