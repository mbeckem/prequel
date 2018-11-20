#ifndef PREQUEL_FILE_ENGINE_HPP
#define PREQUEL_FILE_ENGINE_HPP

#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/vfs.hpp>

#include <memory>

namespace prequel {

namespace detail {

class file_engine_impl;

} // namespace detail

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
    /// Constructs a new file engine.
    ///
    /// \param fd
    ///     The file used for input and output. The reference must remain
    ///     valid for the lifetime of the engine instance.
    ///
    /// \param block_size
    ///     The size of a single block, in bytes.
    ///     Must be a power of two.
    ///
    /// \param cache_size
    ///     The number of blocks that can be cached in memory.
    file_engine(file& fd, u32 block_size, size_t cache_size);
    ~file_engine();

    /// Returns the underlying file handle. The file should not be manipulated
    /// directly unless you know exactly what you're doing.
    file& fd() const;

    /// Returns performance statistics for this engine.
    file_engine_stats stats() const;

private:
    u64 do_size() const override;
    void do_grow(u64 n) override;
    void do_flush() override;

    pin_result do_pin(block_index index, bool initialize) override;
    void do_unpin(block_index index, uintptr_t cookie) noexcept override;
    void do_dirty(block_index index, uintptr_t cookie) override;
    void do_flush(block_index index, uintptr_t cookie) override;

private:
    detail::file_engine_impl& impl() const;

private:
    std::unique_ptr<detail::file_engine_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_FILE_ENGINE_HPP
