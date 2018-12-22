#ifndef PREQUEL_ENGINE_JOURNAL_IPP
#define PREQUEL_ENGINE_JOURNAL_IPP

#include "journal.hpp"

#include <prequel/math.hpp>

#include <cstring>

namespace prequel::detail::engine_impl {

/*
 * TODO: Running checksum over the whole file?
 */

journal::journal(file& logfd, u32 db_block_size, size_t buffer_size)
    : m_logfd(&logfd)
    , m_read_only(m_logfd->read_only())
    , m_database_block_size(db_block_size)
    , m_buffer(buffer_size) {
    PREQUEL_ASSERT(buffer_size > 0, "Invalid buffer size.");
    restore();
}

journal::~journal() {
    /*
     * We can just drop everything here. Committed transactions have been flushed to disk,
     * anything else does not matter anyway.
     */
}

void journal::restore() {
    const u64 log_size = m_logfd->file_size();
    if (log_size == 0) {
        /*
         * Attempt to initialize the empty file, then exit.
         */
        if (m_read_only)
            return;

        log_header header;
        header.magic = LOG_MAGIC;
        header.version = LOG_VERSION;
        header.database_block_size = m_database_block_size;

        auto buffer = serialize_to_buffer(header);
        m_logfd->write(0, buffer.data(), buffer.size());
        m_logfd->sync();

        m_log_size = log_header_size();
        m_buffer_offset = m_log_size;
        return;
    }

    /*
     * Read and validate the journal file header.
     */
    {
        if (log_size < log_header_size()) {
            PREQUEL_THROW(corruption_error("Invalid journal file (header size corrupted)."));
        }

        serialized_buffer<log_header> header_buffer;
        m_logfd->read(0, header_buffer.data(), header_buffer.size());

        log_header header = deserialize_from_buffer<log_header>(header_buffer);
        if (header.magic != magic_header(LOG_MAGIC)) {
            PREQUEL_THROW(corruption_error(
                "Invalid journal header (wrong magic bytes). Did you pass the correct file?"));
        }
        if (header.version != LOG_VERSION) {
            PREQUEL_THROW(corruption_error(
                fmt::format("Invalid journal header (unsupported version {}, expected version {}). "
                            "Did you pass the correct file?",
                            header.version, LOG_VERSION)));
        }
        if (header.database_block_size != m_database_block_size) {
            PREQUEL_THROW(corruption_error(fmt::format(
                "Invalid journal header (unexpected database block size {}, expected {}). "
                "Did you pass the correct file?",
                header.database_block_size, m_database_block_size)));
        }
    }

    /*
     * Scan and validate the file from start to end, replaying all committed transactions.
     * The position of the most recently written committed blocks are stored in the index for
     * future read operations.
     *
     * We can encounter incomplete records at the end of the file (which would be the result of a power loss,
     * for example). As soon as we cannot read a valid record, we consider all data from the there
     * on as invalid and treat the current offset as the end of file.
     */
    u64 offset = log_header_size();
    while (offset < log_size) {
        // Do not modify the offset until we know that we read a complete record.
        auto [success, next_offset] = restore_transaction(offset, log_size);
        if (!success)
            break;

        offset = next_offset;
    }

    /*
     * Position the journal at the end of the scanned file.
     * We cut off incomplete records here to keep the journal file well formed.
     */
    if (!m_read_only) {
        m_logfd->truncate(offset);
        m_logfd->sync();
    }
    m_log_size = offset;
    m_buffer_offset = offset;
}

std::tuple<bool, u64> journal::restore_transaction(u64 offset, u64 size) {
    PREQUEL_ASSERT(!m_in_transaction, "Must not be in a transaction.");
    PREQUEL_ASSERT(m_transaction_begin == 0, "Must not have a beginning.");
    PREQUEL_ASSERT(m_uncommitted_block_positions.empty(), "Must not have any block positions.");

    auto read_record_header = [&](u64 pos) {
        serialized_buffer<record_header> buffer;
        m_logfd->read(pos, buffer.data(), buffer.size());
        return deserialize_from_buffer<record_header>(buffer);
    };

    auto read_commit_record = [&](u64 pos) {
        serialized_buffer<commit_record> buffer;
        m_logfd->read(pos, buffer.data(), buffer.size());
        auto record = deserialize_from_buffer<commit_record>(buffer);
        PREQUEL_ASSERT(record.header.type == record_commit, "Invalid type.");
        return record;
    };

    auto read_write_record = [&](u64 pos) {
        serialized_buffer<write_record> buffer;
        m_logfd->read(pos, buffer.data(), buffer.size());
        auto record = deserialize_from_buffer<write_record>(buffer);
        PREQUEL_ASSERT(record.header.type == record_write, "Invalid type.");
        return record;
    };

    while (offset < size) {
        const u64 available = size - offset;
        if (serialized_size<record_header>() > available)
            return {};

        const record_header header = read_record_header(offset);
        if (!m_in_transaction) {
            if (header.type != record_begin)
                return {};

            m_in_transaction = true;
            m_transaction_begin = offset;
            offset += serialized_size(header);
            continue;
        }

        switch (header.type) {

        /*
         * A block updated within the current transaction. Remember the location
         * for future read operations.
         */
        case record_write: {
            if (serialized_size<write_record>() + m_database_block_size > available)
                return {};

            const write_record record = read_write_record(offset);
            offset += serialized_size(record);
            m_uncommitted_block_positions[record.index] = offset;
            offset += m_database_block_size;
            break;
        }

        /*
         * Commit records mark the end of the active transaction. We know the transaction succeeded,
         * so we can move all uncommitted state into the main index.
         */
        case record_commit: {
            if (serialized_size<commit_record>() > available)
                return {};

            const commit_record record = read_commit_record(offset);
            offset += serialized_size(record);

            /*
             * Move all uncommited blocks into the commited index,
             * then adjust the content of the index by removing entries that have been
             * invalidated because of the new database size.
             */
            for (const auto& pair : m_uncommitted_block_positions) {
                m_block_positions.insert_or_assign(pair.first, pair.second);
            }
            m_block_positions.erase(m_block_positions.lower_bound(block_index(record.database_size)),
                                    m_block_positions.end());
            m_database_size = record.database_size;

            m_in_transaction = false;
            m_transaction_begin = 0;
            m_uncommitted_block_positions.clear();
            return std::tuple(true, offset);
        }

        /*
         * The transaction was aborted - just throw the state away.
         */
        case record_abort: {
            offset += serialized_size(header);
            m_in_transaction = false;
            m_transaction_begin = 0;
            m_uncommitted_block_positions.clear();
            return std::tuple(true, offset);
        }

        default: return {};
        }
    }
    return {};
}

bool journal::read(block_index index, byte* data) const {
    PREQUEL_ASSERT(index, "Cannot read an invalid block index.");
    PREQUEL_ASSERT(data, "Null data pointer.");

    // Attempt to read an uncommitted block, but only when we're inside a transaction.
    if (m_in_transaction) {
        if (auto pos = m_uncommitted_block_positions.find(index);
            pos != m_uncommitted_block_positions.end()) {
            read_internal(pos->second, data, m_database_block_size);
            return true;
        }
    }

    // Attempt to read a committed block.
    if (auto pos = m_block_positions.find(index); pos != m_block_positions.end()) {
        read_internal(pos->second, data, m_database_block_size);
        return true;
    }

    // Block is not in the journal.
    return false;
}

void journal::begin() {
    PREQUEL_ASSERT(!m_in_transaction, "Already in a transaction.");
    PREQUEL_ASSERT(m_uncommitted_block_positions.empty(), "No changed blocks.");
    PREQUEL_ASSERT(!m_read_only, "Cannot start a write transaction in a read only log.");
    PREQUEL_ASSERT(m_logfd->file_size() + m_buffer_used == m_log_size, "Log size invariant.");

    m_in_transaction = true;
    m_transaction_begin = m_log_size;
    append_to_buffer(record_header(record_begin));
}

void journal::commit(u64 database_size) {
    PREQUEL_ASSERT(m_in_transaction, "Must be in a transaction.");
    PREQUEL_ASSERT(!m_read_only, "Cannot commit a write transaction in a read only log.");

    /*
     * A flush isn't really necessary after every transaction commit,
     * it would be more efficient if we had a background thread that regularily
     * invokes flush. This way (combined with fsync), the user would only lose
     * the last N seconds of work.
     */
    append_to_buffer(commit_record(database_size));
    flush_buffer();
    if (m_sync_on_commit) {
        m_logfd->sync();
    }
    // ^ This is the point of successful commit. Anything from here on is index/program state
    // management, which we will be able to restore after a crash by scanning the journal.

    PREQUEL_ASSERT(m_logfd->file_size() + m_buffer_used == m_log_size, "Log size invariant.");

    /*
     * Remember the positions of the committed block versions for later reads.
     * Make sure to erase blocks from the index that may have been erased with
     * the new database size.
     */
    for (const auto& pair : m_uncommitted_block_positions) {
        m_block_positions.insert_or_assign(pair.first, pair.second);
    }
    m_block_positions.erase(m_block_positions.lower_bound(block_index(database_size)),
                            m_block_positions.end());
    m_database_size = database_size;

    m_in_transaction = false;
    m_transaction_begin = 0;
    m_uncommitted_block_positions.clear();
}

void journal::abort() {
    PREQUEL_ASSERT(m_in_transaction, "Must be in a transaction.");
    PREQUEL_ASSERT(!m_read_only, "Cannot abort a write transaction in a read only log.");

    /*
     * Abort the transaction by erasing the last part of the log.
     */
    if (m_transaction_begin < m_buffer_offset) {
        /*
         * Parts of the transaction have been flushed to disk.
         */
        m_logfd->truncate(m_transaction_begin);
        m_log_size = m_transaction_begin;
        m_buffer_offset = m_log_size;
        m_buffer_used = 0;
    } else {
        /*
         * Transaction is in buffer only. just remove the part of the buffer that we can throw away.
         */
        m_log_size = m_transaction_begin;
        m_buffer_used = m_transaction_begin - m_buffer_offset;
        PREQUEL_ASSERT(m_logfd->file_size() + m_buffer_used == m_log_size, "Log size invariant.");
    }

    m_in_transaction = false;
    m_transaction_begin = 0;
    m_uncommitted_block_positions.clear();
}

void journal::write(block_index index, const byte* data) {
    PREQUEL_ASSERT(!m_read_only, "Cannot write to a read only log.");
    PREQUEL_ASSERT(m_in_transaction, "Must be inside a transaction in order to write.");
    PREQUEL_ASSERT(index, "Cannot write an invalid index.");
    PREQUEL_ASSERT(data, "Null data pointer.");

    // We might have already modified this block in this transaction; if so, we overwrite it.
    if (auto pos = m_uncommitted_block_positions.find(index);
        pos != m_uncommitted_block_positions.end()) {
        write_internal(pos->second, data, m_database_block_size);
        return;
    }

    // The modified block is new for this transaction. Append it to the log and
    // remember the position for future reads.
    append_to_buffer(write_record(index));
    append_to_buffer(data, m_database_block_size);

    u64 data_offset = m_log_size - m_database_block_size;
    m_uncommitted_block_positions.emplace(index, data_offset);
}

bool journal::checkpoint(file& database_fd) {
    PREQUEL_ASSERT(!m_in_transaction, "Must not be in a transaction.");

    if (!has_committed_changes()) {
        return false;
    }

    bool changed = false;

    /*
     * Sync the log once to make sure that everything is on disk.
     * There might be previous transactions with sync_on_commit == false,
     * so this makes sure we got everything.
     */
    m_logfd->sync();

    /*
     * Apply the new size, if necessary.
     */
    const u64 db_size_blocks = *m_database_size;
    const u64 db_size_bytes = checked_mul<u64>(db_size_blocks, m_database_block_size);
    if (database_fd.file_size() != db_size_bytes) {
        database_fd.truncate(db_size_bytes);
        changed = true;
    }

    /*
     * Copy the most recent version of all blocks in the journal into the database file.
     *
     * Improvement: Note that this currenty writes the blocks in database-order, so we might be seeking
     * a lot through the log. The other way around might be faster, because the database file
     * should generally support better random access I/O than the log file.
     */
    std::vector<byte> block(m_database_block_size);
    for (const auto& pair : m_block_positions) {
        const block_index index = pair.first;
        const u64 offset_in_log = pair.second;
        PREQUEL_ASSERT(index, "Must be a valid block index.");
        PREQUEL_ASSERT(index < block_index(db_size_blocks), "Block index out of bounds.");

        const u64 offset_in_db = checked_mul<u64>(index.value(), m_database_block_size);
        read_internal(offset_in_log, block.data(), block.size());
        database_fd.write(offset_in_db, block.data(), block.size());
    }
    changed |= m_block_positions.size() > 0;

    /*
     * Sync the database file.
     */
    database_fd.sync();
    // ^ Checkpoint successful here.

    /*
     * Shrink the log. This is safe because all changes have been successfully applied to the database.
     * Then simply forget all state and start from the beginning.
     */
    m_logfd->truncate(log_header_size());
    m_logfd->sync();
    // ^ Checkpoint will not be repeated after a crash when the sync was executed successfully.

    m_log_size = log_header_size();
    m_buffer_offset = m_log_size;
    m_buffer_used = 0;
    m_block_positions.clear();
    m_database_size.reset();
    return changed;
}

template<typename Func>
void journal::iterate_uncommitted(Func&& fn) const {
    PREQUEL_ASSERT(in_transaction(), "Must be in a transaction.");

    for (const auto& pair : m_uncommitted_block_positions) {
        const block_index& index = pair.first;
        fn(index);
    }
}

void journal::read_internal(u64 offset, byte* data, size_t size) const {
    PREQUEL_ASSERT(range_in_bounds<u64>(m_log_size, offset, size), "Read out of bounds.");
    PREQUEL_ASSERT(data, "Invalid data array.");

    if (size == 0)
        return;

    // There might be a portion of the block before the buffer, read from it directly.
    if (offset < m_buffer_offset) {
        u32 read_size = size;
        if (offset + read_size > m_buffer_offset) {
            read_size = m_buffer_offset - offset;
        }
        m_logfd->read(offset, data, read_size);

        offset += read_size;
        data += read_size;
        size -= read_size;
    }

    // Read the part that overlaps the buffer (if any).
    if (size > 0) {
        PREQUEL_ASSERT(offset >= m_buffer_offset, "Data must start in the buffer.");
        PREQUEL_ASSERT(offset - m_buffer_offset + size <= m_buffer_used,
                       "Must be in the used part of the buffer.");

        std::memcpy(data, m_buffer.data() + (offset - m_buffer_offset), size);
    }
}

void journal::write_internal(u64 offset, const byte* data, size_t size) {
    PREQUEL_ASSERT(!m_read_only, "Cannot write to a read-only journal.");
    PREQUEL_ASSERT(range_in_bounds<u64>(m_log_size, offset, size), "Write out of bounds.");
    PREQUEL_ASSERT(data, "Invalid data array.");

    if (size == 0)
        return;

    // There might be a portion before the buffer, write to it directly.
    if (offset < m_buffer_offset) {
        u32 write_size = size;
        if (offset + write_size > m_buffer_offset) {
            write_size = m_buffer_offset - offset;
        }
        m_logfd->write(offset, data, write_size);

        offset += write_size;
        data += write_size;
        size -= write_size;
    }

    // Write the part that overlaps the buffer (if any).
    if (size > 0) {
        PREQUEL_ASSERT(offset >= m_buffer_offset, "Data must start in the buffer.");
        PREQUEL_ASSERT(offset - m_buffer_offset + size <= m_buffer_used,
                       "Must be in the used part of the buffer.");

        std::memcpy(m_buffer.data() + (offset - m_buffer_offset), data, size);
    }
}

void journal::append_to_buffer(const byte* data, size_t size) {
    PREQUEL_ASSERT(!m_read_only, "Cannot write to a read-only journal.");

    while (size > 0) {
        if (m_buffer_used == m_buffer.size())
            flush_buffer();

        PREQUEL_ASSERT(m_buffer_used < m_buffer.size(), "Flush must have made space.");
        const size_t space = m_buffer.size() - m_buffer_used;
        const size_t write = std::min(space, size);
        std::memcpy(m_buffer.data() + m_buffer_used, data, write);

        m_buffer_used += write;
        m_log_size += write;
        PREQUEL_ASSERT(m_buffer_used <= m_buffer.size(), "Invalid buffer state.");

        data += write;
        size -= write;
    }
}

void journal::flush_buffer() {
    PREQUEL_ASSERT(!m_read_only, "Cannot write to a read-only journal.");
    PREQUEL_ASSERT(m_buffer_used <= m_buffer.size(), "Invalid buffer state.");

    if (m_buffer_used > 0) {
        m_logfd->write(m_buffer_offset, m_buffer.data(), m_buffer_used);
        m_buffer_offset += m_buffer_used;
        m_buffer_used = 0;
        PREQUEL_ASSERT(m_buffer_offset == m_log_size, "Cursor and size must be equal after flush.");
    }
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_JOURNAL_IPP
