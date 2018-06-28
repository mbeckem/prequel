#ifndef EXTPP_FILE_ENGINE_HPP
#define EXTPP_FILE_ENGINE_HPP

#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/io.hpp>

#include <memory>

namespace extpp {

class file_engine_impl;

/// Contains performance statistics for a single engine.
struct file_engine_stats {
    /// Number of blocks read from disk. Also the
    /// total number of cache misses.
    u64 reads = 0;

    /// Number of blocks written to disk.
    u64 writes = 0;

    /// Number of times a block was retrieved
    /// from the cache (i.e. no read was required).
    u64 cache_hits = 0;
};

class file_engine : public engine
{
public:
    /// Constructs a new block engine.
    ///
    /// \param fd
    ///     The file used for input and output. The reference must remain
    ///     valid for the lifetime of this instance.
    ///
    /// \param block_size
    ///     The size of a single block, in bytes.
    ///     Must be a power of two.
    ///
    /// \param cache_size
    ///     The number of blocks that can be cached in memory.
    file_engine(file& fd, u32 block_size, u32 cache_size);
    ~file_engine();

    /// Returns the underlying file handle. The file should not be manipulated
    /// directly unless you know exactly what you're doing.
    file& fd() const;

    /// Returns performance statistics for this engine.
    file_engine_stats stats() const;

private:
    u64 do_size() const override;
    void do_grow(u64 n) override;
    block_handle do_access(block_index index) override;
    block_handle do_read(block_index index) override;
    block_handle do_zeroed(block_index index) override;
    block_handle do_overwritten(block_index index, const byte* data) override;

    // TODO: Should flush imply sync() ?
    void do_flush() override;

private:
    file_engine_impl& impl() const;

private:
    std::unique_ptr<file_engine_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_FILE_ENGINE_HPP
