#ifndef EXTPP_IO_HPP
#define EXTPP_IO_HPP

#include <extpp/defs.hpp>

#include <memory>
#include <system_error>

namespace extpp {

// TODO: Mechanism for fadvice()
class file {
public:
    file() = default;

    virtual ~file();

    /// Returns the name of this file (for error reporting only).
    virtual const char* name() const noexcept = 0;

    /// Reads exactly `count` bytes at the given offset
    /// into the provided buffer.
    virtual void read(u64 offset, void* buffer, u32 count) = 0;

    /// Writes exactly `count` bytes at the given offset
    /// from the provided buffer into the file.
    virtual void write(u64 offset, const void* buffer, u32 count) = 0;

    /// Returns the size of the file, in bytes.
    virtual u64 file_size() = 0;

    /// Resizes the file to the given number of bytes.
    virtual void truncate(u64 size) = 0;

    /// Closes this file handle.
    virtual void close() = 0;

    file(const file&) = delete;
    file& operator=(const file&) = delete;
};

class vfs {
public:
    enum access_t {
        /// Whether the file should be read-only.
        read_only,

        /// Whether the file should be both readable and writable.
        read_write,
    };

    /// Additional flags for the open() function.
    enum flags_t {
        /// Default flags.
        open_normal = 0,

        /// Whether the file should be created if it doesn't exist.
        open_create = 1 << 0,
    };

public:
    vfs() = default;

    virtual ~vfs();

    /// Name of this vfs.
    virtual const char* name() const noexcept = 0;

    /// Opens the file at the given path or throws an exception.
    virtual std::unique_ptr<file> open(const char* path,
                                       access_t access = read_only,
                                       flags_t mode = open_normal) = 0;

    vfs(const vfs&) = delete;
    vfs& operator=(const vfs&) = delete;
};

/// Returns the system's file system.
///
/// \relates vfs
vfs& system_vfs();

// TODO: in-memory vfs.
std::unique_ptr<file> create_memory_file(std::string name);

} // namespace extpp

#endif // EXTPP_IO_HPP
