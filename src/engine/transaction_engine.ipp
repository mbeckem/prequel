#ifndef PREQUEL_ENGINE_TRANSACTION_ENGINE_IPP
#define PREQUEL_ENGINE_TRANSACTION_ENGINE_IPP

#include "transaction_engine.hpp"

#include <prequel/exception.hpp>
#include <prequel/math.hpp>

namespace prequel::detail::engine_impl {

inline constexpr size_t default_journal_buffer_bytes = 4 * 1024 * 1024;

transaction_engine::transaction_engine(file& dbfd, file& journalfd, u32 block_size,
                                       size_t cache_blocks)
    : engine_base(block_size, cache_blocks, journalfd.read_only())
    , m_dbfd(&dbfd)
    , m_journalfd(&journalfd)
    , m_journal(journalfd, block_size, default_journal_buffer_bytes) {
    u64 size_bytes = m_dbfd->file_size();
    if (size_bytes % m_block_size != 0) {
        PREQUEL_THROW(corruption_error("Database size is not a multiple of the block size."));
    }
    m_dbfile_size = size_bytes / m_block_size;
    m_size = m_journal.database_size().value_or(
        m_dbfile_size); // Use most recent committed size from the journal.
}

transaction_engine::~transaction_engine() {
    // Nothing. If there is an active transaction, doing nothing will abort it.
}

void transaction_engine::begin() {
    PREQUEL_ASSERT(m_pinned_blocks == 0, "There cannot be any pinned blocks.");

    if (m_transaction_started) {
        PREQUEL_THROW(
            bad_operation("A transaction is already running. "
                          "Call commit() or rollback() before invoking begin() again."));
    }

    m_transaction_started = true;
}

void transaction_engine::commit() {
    if (!m_transaction_started) {
        PREQUEL_THROW(
            bad_operation("Cannot commit without starting a transaction first. "
                          "Call begin() before invoking commit()."));
    }
    if (m_pinned_blocks > 0) {
        PREQUEL_THROW(bad_operation(
            "All references to blocks must be dropped before committing a transaction."));
    }

    /*
     * Write all dirty blocks to disk.
     */
    flush();

    /*
     * The journal might not have an active internal transaction because this
     * transaction could have been read-only. Write() lazily starts a real transaction
     * if needed.
     */
    if (m_journal.in_transaction())
        m_journal.commit(m_size);

    m_transaction_started = false;
}

void transaction_engine::rollback() {
    if (!m_transaction_started) {
        PREQUEL_THROW(
            bad_operation("Cannot rollback without starting a transaction first. "
                          "Call begin() before invoking rollback()."));
    }
    if (m_pinned_blocks > 0) {
        PREQUEL_THROW(bad_operation(
            "All references to blocks must be dropped before rolling back a transaction."));
    }

    /*
     * Simply drop all dirty blocks to restore their content. Future read operations
     * will read the clean version back into memory.
     */
    discard_dirty();

    /*
     * Abort the transaction in the journal (if there were actual write operations).
     * Make sure to discard all blocks that have been written to the journal this transaction,
     * even if they are currently "clean" in memory.
     */
    if (m_journal.in_transaction()) {
        m_journal.iterate_uncommitted([&](block_index index) {
            PREQUEL_ASSERT(index.valid(), "Must be a valid block index.");
            discard(index.value());
        });
        m_journal.abort();
    }

    /*
     * Reset the database size to a safe value.
     */
    m_size = m_journal.database_size().value_or(m_dbfile_size);
    m_transaction_started = false;
}

void transaction_engine::checkpoint() {
    if (m_transaction_started) {
        PREQUEL_THROW(
            bad_operation("Cannot perform a checkpoint while in a transaction. "
                          "Invoke abort() or commit() first."));
    }

    if (m_dbfd->read_only()) {
        PREQUEL_THROW(bad_operation("Cannot perform a checkpoint on a read-only database file."));
    }

    if (m_journalfd->read_only()) {
        PREQUEL_THROW(bad_operation("Cannot perform a checkpoint on a read-only journal file."));
    }

    if (!m_journal.has_committed_changes())
        return;

    PREQUEL_ASSERT(!m_journal.in_transaction(), "Journal cannot be in a transaction.");
    PREQUEL_ASSERT(m_journal.database_size() && m_journal.database_size().value() == m_size,
                   "Database size is consistent with journal.");

    m_journal.checkpoint(*m_dbfd);
    m_dbfile_size = m_size;
}

block* transaction_engine::pin(u64 index, bool initialize) {
    if (!m_transaction_started) {
        PREQUEL_THROW(bad_operation("Must start a transaction before accessing database blocks."));
    }

    if (index >= m_size) {
        PREQUEL_THROW(bad_argument(fmt::format(
            "Block index {} is out of bounds (database size is {} blocks).", index, m_size)));
    }

    block* blk = engine_base::pin(index, initialize);
    m_pinned_blocks += 1;
    return blk;
}

void transaction_engine::unpin(u64 index, block* blk) noexcept {
    PREQUEL_ASSERT(m_pinned_blocks > 0, "Inconsistent pin counter.");
    engine_base::unpin(index, blk);
    m_pinned_blocks -= 1;
}

void transaction_engine::grow(u64 n) {
    if (!m_transaction_started) {
        PREQUEL_THROW(bad_operation("Must start a transaction before changing the database size."));
    }

    m_size = checked_add(m_size, n);
}

void transaction_engine::do_read(u64 index, byte* buffer) {
    PREQUEL_ASSERT(m_transaction_started, "A transaction must be active.");

    // Check the journal first for updated block contents.
    if (m_journal.read(block_index(index), buffer)) {
        return;
    }

    // Otherwise, read the block from the main database file.
    if (index < m_dbfile_size) {
        m_dbfd->read(index << m_block_size_log, buffer, m_block_size);
    } else {
        std::memset(buffer, 0, m_block_size);
    }
}

void transaction_engine::do_write(u64 index, const byte* buffer) {
    PREQUEL_ASSERT(m_transaction_started, "A transaction must be active.");

    /*
     * Never update the database file - write the new version to the journal instead.
     * We lazily start a transaction within the journal on the first write operation.
     * This makes read-only transactions much more efficient because they
     * perform 0 I/O operations.
     */
    if (!m_journal.in_transaction()) {
        m_journal.begin();
    }
    m_journal.write(block_index(index), buffer);
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_TRANSACTION_ENGINE_IPP
