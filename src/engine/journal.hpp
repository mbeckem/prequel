#ifndef PREQUEL_ENGINE_JOURNAL_HPP
#define PREQUEL_ENGINE_JOURNAL_HPP

#include <prequel/block_index.hpp>
#include <prequel/serialization.hpp>
#include <prequel/simple_file_format.hpp> // TODO because of magic header, move it?
#include <prequel/vfs.hpp>

#include <map>
#include <vector>

namespace prequel::detail::engine_impl {

/*
 * The journal is a write-ahead-log (redo log) for the transactional engine.
 * Changes made to in-memory blocks by the application are not written back to the database file to
 * protect against data loss: a program crash or power loss could leave an inconsistent version of the
 * database behind.
 * Instead, all changes are appended to this journal. After all changes have been made,
 * the application can commit (or roll back). Only then can the database file be modified without
 * losing data integrity.
 *
 * After a transaction has been committed, all blocks CAN be copied over into the main database file, because
 * we will be able to reconstruct a fully consistent version of the database. This will be the case
 * even if the copy fails (e.g. because of power loss): we can just restart the copy operation from the beginning
 * the next time the application runs. The contents of a commit transaction are not copied over to the main database
 * immediately - instead the process is delayed until the journal has grown to a certain size; all transactions
 * will then be copied in one batch.
 *
 * Note that this journal implements redo logging on a physical layer, i.e. we log complete blocks instead of logical
 * (or physio-logical) changes. This is because we know nothing about the upper layers of the application, every block
 * is an arbitrary blob of bytes to us. This approach is very similar to the system in use by sqlite 3 (WAL mode), so
 * I expect it to perform reasonably well for a first implementation.
 * A more sophisticated system (like logging record-level changes for modified blocks) would be interesting for a
 * future version of this library, but I am unsure of how to implement that right now without making all our datastructures
 * much more complicated (the journal would have to know about their layout).
 *
 * A fast diff algorithm for binary deltas might be a good option.
 */
class journal {
private:
    static constexpr const char LOG_MAGIC[] = "PREQUEL_TX_JOURNAL";
    static constexpr u32 LOG_VERSION = 1;

    // Header at the start of the file.
    struct log_header {
        magic_header magic;
        u32 version = 0;
        u32 database_block_size = 0;

        static constexpr auto get_binary_format() {
            return binary_format(&log_header::magic, &log_header::version,
                                 &log_header::database_block_size);
        }
    };

public:
    // Size of the header at the start of the journal file.
    static constexpr u32 log_header_size() { return serialized_size<log_header>(); }

public:
    inline journal(file& logfd, u32 database_block_size, size_t buffer_size);
    inline ~journal();

    journal(const journal&) = delete;
    journal& operator=(const journal&) = delete;

    // Logical database block size.
    u32 database_block_size() const { return m_database_block_size; }

    // Size of the in memory buffer (tail of the journal), in bytes.
    size_t buffer_size() const { return m_buffer.size(); }

    /*
     * Current size of the journal, in bytes. The only way to reduce
     * this size is to perform a checkpoint operation.
     */
    u64 log_size() const { return m_log_size; }

    /*
     * Enables (the default) or disables "sync on commit".
     * If sync on commit is enabled, every commit will result in a log flush and a single fsync() call
     * in order to flush everything to persistent storage.
     *
     * Disabling this behaviour might result in data loss on crash/power loss, because committed
     * transactions might not have been written to disk, i.e. durability is weakened.
     * It does not affect the integrity of the database, because it will just revert to an earlier version of itself.
     */
    bool sync_on_commit() const { return m_sync_on_commit; }
    void sync_on_commit(bool enabled) { m_sync_on_commit = enabled; }

    // True if a transaction was started and as not (yet) been committed nor aborted.
    bool in_transaction() const { return m_in_transaction; }

    /*
     * Returns true if this journal contains any committed changes. It can be safely deleted
     * only if that is not the case.
     */
    bool has_committed_changes() const { return static_cast<bool>(m_database_size); }

    /*
     * Returns the (committed) size of the database in blocks, as recorded by this journal file.
     * It it the argument to the latest successful `commit()` call known to the log
     * or an empty optional if no such call was recorded.
     */
    std::optional<u64> database_size() const { return m_database_size; }

    /*
     * Attempts to read the most recent version of the block at `index`
     * from this journal. Returns false if that block is not known to the journal, i.e.
     * when that block should be read from the main database file instead.
     *
     * `data` must be large enough to write a logical database block to it.
     */
    inline bool read(block_index index, byte* data) const;

    /*
     * Begins/commits/aborts a transaction.
     * Note that all 3 functions result in at least one record written to the journal,
     * so you should not call them if all you need to do is read.
     *
     * A read-only transaction should not call these functions and a write transaction
     * must call it at some point before the first write.
     */
    inline void begin();
    inline void abort();
    inline void commit(u64 database_size /* size of database at the point of commit, in blocks */);

    /*
     * Writes the given version of the block to the journal.
     * Must be inside a transaction.
     *
     * `data` must be large enough to read a logical database block from it.
     */
    inline void write(block_index index, const byte* data);

    /*
     * Transfer all committed changes from the journal to the main database file.
     * The journal will be empty again after a successful checkpoint (except for its file header).
     *
     * Returns true if the database file was modified.
     *
     * Improvement: Could use a way to avoid very long pauses (i.e. incremental checkpoints).
     */
    inline bool checkpoint(file& database_fd);

    /*
     * The function will be invoked for every block index
     * that has been modified in this transaction.
     */
    template<typename Func>
    inline void iterate_uncommited(Func&& fn) const;

private:
    // Restore the state of the journal by scanning the log file. Called from the constructor.
    inline void restore();

    // Replay the next transaction in the log, starting at the given offset.
    // Returns the offset just after the replayed transaction on success.
    inline std::tuple<bool, u64> restore_transaction(u64 offset, u64 size);

    // Read from the file and/or the buffer, depending on the file offset.
    inline void read_internal(u64 offset, byte* data, size_t size) const;

    // Overwrite a part of the existing journal (in the file and/or the buffer, depending
    // on the file offset.
    inline void write_internal(u64 offset, const byte* data, size_t size);

    // Writes the data to the end of the log (i.e. the in-memory buffer). The buffer
    // is flushed as often as required.
    inline void append_to_buffer(const byte* data, size_t size);

    template<typename T>
    void append_to_buffer(const T& value) {
        serialized_buffer<T> serialized;
        serialize(value, serialized.data());
        append_to_buffer(serialized.data(), serialized.size());
    }

    // Flushes the content of the buffer to disk (no fsync).
    // The buffer is empty (m_buffer_used == 0) on success.
    inline void flush_buffer();

private:
    // Log record types.
    enum record_type_t : byte {
        record_invalid = 0,

        record_begin = 1,
        record_abort = 2,
        record_commit = 3,
        record_write = 4,
    };

    // Start of every log record.
    struct record_header {
        record_type_t type = record_invalid;

        record_header() = default;
        record_header(record_type_t type_)
            : type(type_) {}

        static constexpr auto get_binary_format() { return binary_format(&record_header::type); }
    };

    // The record indicates a succesful commit.
    struct commit_record {
        record_header header;
        u64 database_size = 0;

        commit_record() = default;
        commit_record(u64 database_size_)
            : header(record_commit)
            , database_size(database_size_) {}

        static constexpr auto get_binary_format() {
            return binary_format(&commit_record::header, &commit_record::database_size);
        }
    };

    // The record indicates a block write and is followed by the block data.
    struct write_record {
        record_header header;
        block_index index;

        write_record() = default;
        write_record(block_index index_)
            : header(record_write)
            , index(index_) {}

        static constexpr auto get_binary_format() {
            return binary_format(&write_record::header, &write_record::index);
        }
    };

private:
    // Journal records are appended to this file.
    file* m_logfd = nullptr;

    // Read only log file?
    bool m_read_only = false;

    // Logical block size (in bytes) of the database file.
    // Does not need to be the same as the logfile's or database file's native block size.
    u32 m_database_block_size = 0;

    // Whether to flush the log buffer and fsync() after commiting a transaction.
    bool m_sync_on_commit = true;

private:
    // -- Journal file management --
    // -----------------------------

    /*
     * TODO: Make better use of the buffer. Currently the buffer gets reset after every commit, so
     * we cant use it to cache the data we have just written for the previous commit (which might be likely to read,
     * depending on the application).
     */

    // Logical size of the journal (in bytes). Includes the unflushed buffer and serves
    // as the log sequence number for the next record. Usually not the same as m_logfd->file_size().
    u64 m_log_size = 0;

    // Offset at which we will write the content of the buffer when it has to be flushed, i.e.
    // this is both the end of the file on disk and the beginning of the buffer in memory.
    u64 m_buffer_offset = 0;

    // Buffer is flushed when out of space.
    u32 m_buffer_used = 0;

    // Output buffer (tail of the log).
    std::vector<byte> m_buffer;

private:
    // -- Committed database state --
    // ------------------------------

    /*
     * TODO: the block indices below might become large and should support partial swapping to disk.
     * We could use a temp file with a file_engine and a small cache size for that purpose.
     *
     * Note: if we are every going to support multithreading, we can easily support
     * concurrent read transactions (plus one concurrent write transaction) by remembering the
     * current log sequence number for every transaction. A read transaction would then retrieve the "most
     * up to date committed block up to log sequence number N" instead of the most recent version,
     * which is currently implemented.
     */

    // Size of the database (committed). Empty if not a single committed transaction in the journal.
    std::optional<u64> m_database_size;

    // Indexes the contents of committed block changes within this journal.
    // Maps block index to raw offset within the log file. Reading m_db_block_size bytes
    // from that offset will return the most recent committed version of that block.
    // TODO: Can become very large.
    std::map<block_index, u64> m_block_positions;

private:
    // -- Current transaction state --
    // -------------------------------

    // True if there is an active transaction.
    bool m_in_transaction = false;

    // File offset of the "begin" record for the running transaction.
    u64 m_transaction_begin = 0;

    // Indexes the contents of changed blocks within the running transaction.
    // Once the transaction commits, these values will be merged with m_block_index.
    // If the transaction is rolled back, these changes will be thrown away.
    // TODO: Can become very large.
    std::map<block_index, u64> m_uncommitted_block_positions;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_JOURNAL_HPP
