#ifndef EXTPP_DETAIL_WAL_HPP
#define EXTPP_DETAIL_WAL_HPP

#include <extpp/assert.hpp>
#include <extpp/block.hpp>
#include <extpp/exception.hpp>
#include <extpp/defs.hpp>
#include <extpp/io.hpp>
#include <extpp/math.hpp>

#include <optional>
#include <unordered_map>

namespace extpp::detail {

template<u32 BlockSize>
class wal {
public:
    static constexpr u32 block_size = BlockSize;

private:
    enum index_flags {
        BEGIN = 1 << 0,
        COMMIT = 1 << 1,
    };

    struct index_block_header {
        /// Bitset of \ref index_flags
        u32 flags = 0;

        /// Number of blocks described by this index block.
        /// Equal to the number of data blocks that follow this block
        /// and equal to the number of entries in this block's value array.
        u32 count = 0;
    };

    struct index_block : make_array_block_t<index_block_header, u64, BlockSize> {
        void reset() {
            *this = index_block();
        }
    };

    /// Buffer size for (index_block, data_block...) writes, in blocks.
    static constexpr u32 max_buffer_blocks = index_block::capacity;

public:
    explicit wal(file& logfile)
        : m_file(logfile)
        , m_file_blocks(logfile.file_size() / BlockSize)
        , m_index_block(std::make_unique<index_block>())
        , m_buffer(new byte[checked_mul(max_buffer_blocks, block_size)])
    {}

    /// Begin a new transaction. Transactions in the wal-layer must not be empty,
    /// i.e. at least one block must be written.
    void begin() {
        EXTPP_ASSERT(!m_in_transaction, "Already in a transaction.");

        m_in_transaction = true;
        m_index_block->flags |= BEGIN;
    }

    /// Commit the current transaction.
    void commit() {
        EXTPP_ASSERT(m_in_transaction, "Not in a transaction.");

        flush_buffer();
        m_index_block->flags |= COMMIT;
        // TODO filesize
        flush_buffer();
        m_file.sync();

        m_tx_blocks.clear();
        m_in_transaction = false;

        // TODO: Promote tx blocks to all blocks.
    }

    /// Aborts the current transaction.
    void abort() {
        EXTPP_ASSERT(m_in_transaction, "Not in a transaction.");

        m_tx_blocks.clear();
        m_index_block->reset();
        m_in_transaction = false;
    }

    /// Write a block in the current transaction.
    /// \pre Must be within a transaction.
    /// \pre `len == block_size`.
    void write(u64 block_index, const byte* data, u32 len) {
        EXTPP_ASSERT(m_in_transaction, "Must be in a transaction");
        EXTPP_ASSERT(len == block_size, "Invalid block size.");

        if (auto pos = m_tx_blocks.find(block_index); pos != m_tx_blocks.end()) {
            // The block has already been written in this transaction - update it in place
            // instead of writing a new copy. This unfortunately might require a disk seek
            // but greatly reduces the space requirements for large transactions.
            u64 block_log_index = pos->second;
            if (auto index = buffer_index(block_log_index)) {
                // The block is still in the output buffer.
                std::memcpy(m_buffer.get() + (*index * block_size), data, block_size);
                return;
            }

            // The block is on disk, update it there.
            m_file.write(block_log_index * block_size, data, block_size);
            return;
        }

        // This is a new block - append it to the log.
        append_block(block_index, data);
    }

    /// Try to read that might have been written to the wal.
    /// Returns true if the block is stored in the wal, false otherwise (the block remains untouched
    /// and can be read from the database file in this case).
    /// When `read` returns true, then `data` will have been filled with the block's current data.
    ///
    /// \pre `len >= block_size`.
    bool read(u64 block_index, byte* data, u32 len) const {
        EXTPP_ASSERT(len >= block_size, "data array not large enough.");

        // TODO: Read from previous transactions.

        auto pos = m_tx_blocks.find(block_index);
        if (pos == m_tx_blocks.end())
            return false;

        u64 block_log_index = pos->second;
        if (auto index = buffer_index(block_log_index)) {
            // The block is still in the buffer, no I/O required.
            std::memcpy(data, m_buffer.get() + (*index * block_size), block_size);
            return true;
        }

        // The block is on disk.
        m_file.read(block_log_index * block_size, data, block_size);
        return true;
    }

    /// The current number of blocks in the output buffer.
    u32 buffer_size() const { return m_index_block->count; }

    /// The number of blocks that can be buffered before they have to be flushed to disk.
    u32 buffer_capacity() const { return max_buffer_blocks * block_size; }

    wal(const wal&) = delete;
    wal& operator=(const wal&) = delete;

private:
    void append_block(u64 block_index, const byte* data) {
        EXTPP_ASSERT(m_tx_blocks.find(block_index) == m_tx_blocks.end(),
                     "Must be a new block");

        if (m_index_block->count == max_buffer_blocks) {
            flush_buffer();
        }

        // Copy the data into the buffer and remember the real database position of the block.
        u32 buffer_index = m_index_block->count++;
        EXTPP_ASSERT(buffer_index < max_buffer_blocks, "Invalid index into the buffer.");
        m_index_block->values[buffer_index] = block_index;
        std::memcpy(m_buffer.get() + (buffer_index * block_size), data, block_size);

        // Remember the position of the block for later reads or writes.
        m_tx_blocks.emplace(block_index, buffer_begin() + buffer_index);
    }

    void flush_buffer() {
        m_file.write(m_file_blocks * block_size, m_index_block.get(), block_size);
        m_file_blocks++;

        u32 blocks = buffer_size();
        if (blocks > 0) {
            m_file.write(m_file_blocks * block_size, m_buffer.get(), blocks * block_size);
            m_file_blocks += blocks;
        }

        m_index_block->reset();
    }

    /// Returns the first block position in the buffer.
    /// (+1 to account for the block that would store the index).
    u64 buffer_begin() const { return m_file_blocks + 1; }

    /// Returns the last block position in the buffer.
    u64 buffer_end() const { return buffer_begin() + buffer_size(); }

    /// Returns the block's index within the output buffer (or nullopt if the block is on disk).
    std::optional<u32> buffer_index(u64 block_log_index) {
        if (block_log_index < buffer_begin())
            return {};
        EXTPP_ASSERT(block_log_index <= buffer_end(), "Invalid log index (beyond the end of the log).");
        return block_log_index - buffer_begin();
    }

private:
    file& m_file;

    /// The number of blocks in the file (on disk; does not include the output buffer).
    u64 m_file_blocks = 0;

    bool m_in_transaction = false;

    /// The current index block. Written on transaction commit or when the buffer is full.
    std::unique_ptr<index_block> m_index_block; // TODO: Must be block aligned for direct IO

    /// Block write buffer. The blocks' real addresses (in the file) are described by the
    /// index block that precedes them in the log file.
    std::unique_ptr<byte[]> m_buffer; // TODO: Must be block aligned for direct IO

    /// Maps block indices written in the current transaction to their position within the log.
    /// Key: block index in the database file.
    /// Value: block index in the log file (might be in the buffer and not yet on disk).
    std::unordered_map<u64, u64> m_tx_blocks;
};

} // namespace extpp::detail

#endif // EXTPP_DETAIL_WAL_HPP
