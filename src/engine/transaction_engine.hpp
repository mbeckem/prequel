#ifndef PREQUEL_ENGINE_TRANSACTION_ENGINE_HPP
#define PREQUEL_ENGINE_TRANSACTION_ENGINE_HPP

#include "engine_base.hpp"
#include "journal.hpp"

#include <prequel/vfs.hpp>

namespace prequel::detail::engine_impl {

class transaction_engine final : public engine_base {
public:
    inline transaction_engine(file& dbfd, file& journalfd, u32 block_size, size_t cache_blocks);

    inline ~transaction_engine();

    file& dbfd() const { return *m_dbfd; }
    file& journalfd() const { return *m_journalfd; }

    bool in_transaction() const { return m_transaction_started; }

    // TODO expose as option in public interface
    bool sync_on_commit() const { return m_journal.sync_on_commit(); }
    void sync_on_commit(bool enabled) { m_journal.sync_on_commit(enabled); }

    bool journal_has_changes() const { return m_journal.has_committed_changes(); }
    u64 journal_size() const { return m_journal.log_size(); }

    inline void begin();
    inline void commit();
    inline void rollback();

    inline void checkpoint();

    inline block* pin(u64 index, bool initialize) override;
    inline void unpin(u64 index, block* blk) noexcept override;

    inline void grow(u64 n);
    inline u64 size() const { return m_size; }

protected:
    inline void do_read(u64 index, byte* buffer) override;
    inline void do_write(u64 index, const byte* buffer) override;

private:
    /// Database file. Usually not modified, except for checkpoint operations.
    file* m_dbfd = nullptr;

    /// Journal file. Changes are written to this file instead of the database file.
    /// Successfull transactions can be merged into the database file by executing the checkpoint operation.
    file* m_journalfd = nullptr;

    /// Reads from and writes to the journal fd.
    journal m_journal;

    /// Number of pinned blocks.
    size_t m_pinned_blocks = 0;

    /// Size of the database file on disk, in blocks.
    u64 m_dbfile_size = 0;

    /// begin() was called - no commit() or rollback() yet.
    bool m_transaction_started = false;

    /// Current (non-commited) size of the database, in blocks.
    u64 m_size = 0;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_TRANSACTION_ENGINE_HPP
