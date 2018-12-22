#ifndef PREQUEL_VFS_HPP
#define PREQUEL_VFS_HPP

#include <prequel/assert.hpp>
#include <prequel/defs.hpp>

#include <memory>
#include <system_error>

namespace prequel {

class file;
class vfs;

// TODO: Mechanism for fadvice() .
class file {
public:
    file(prequel::vfs& v)
        : m_vfs(v) {}

    virtual ~file();

    /// The virtual file system that this file belongs to.
    prequel::vfs& get_vfs() const { return m_vfs; }

    /// True if the file has been opened in read-only mode.
    virtual bool read_only() const noexcept = 0;

    /// Returns the name of this file (for error reporting only).
    virtual const char* name() const noexcept = 0;

    /// The block size of the underlying I/O device.
    virtual u32 block_size() const noexcept = 0;

    /// Reads exactly `count` bytes at the given offset
    /// into the provided buffer.
    virtual void read(u64 offset, void* buffer, u32 count) = 0;

    /// Writes exactly `count` bytes at the given offset
    /// from the provided buffer into the file.
    ///
    /// Writing to beyond the end of the file automatically makes the file grow.
    virtual void write(u64 offset, const void* buffer, u32 count) = 0;

    /// Returns the size of the file, in bytes.
    virtual u64 file_size() = 0;

    /// Returns the maximum file size, in bytes. Platforms that do not have an inherent
    /// maximum file size simple return the maximum value.
    virtual u64 max_file_size() = 0;

    /// Resizes the file to the given number of bytes.
    virtual void truncate(u64 size) = 0;

    /// Writes all buffered changes of the file to the disk.
    virtual void sync() = 0;

    /// Closes this file handle.
    virtual void close() = 0;

    file(const file&) = delete;
    file& operator=(const file&) = delete;

private:
    prequel::vfs& m_vfs;
};

/// The virtual file system provides the bare necessities for opening files.
class vfs {
public:
    /// Access mode for the open() function.
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

        /// When open_create is specified: Errors when the file already exists.
        open_exlusive = 1 << 1,

        /// Open the file in direct mode, if possible. Direct I/O mode must obey platform
        /// restriction. For example on linux, all reads and writes must be aligned to the native
        /// block size.
        open_direct = 1 << 2
    };

public:
    vfs() = default;

    virtual ~vfs();

    /// Name of this vfs.
    virtual const char* name() const noexcept = 0;

    /// Opens the file at the given path or throws an exception.
    virtual std::unique_ptr<file>
    open(const char* path, access_t access = read_only, int mode = open_normal) = 0;

    /// Creates and opens a new temporary file.
    /// The new file will be deleted automatically when it is no longer
    /// referred to by any file objects.
    ///
    /// TODO: Specifiy in which directory the file is to be created?
    /// TODO: Temporary directories?
    /// TODO: Take a restricted subset of open flags (such as open_direct)
    virtual std::unique_ptr<file> create_temp() = 0;

    /// Removes the file with the given name or throws an exception.
    /// Note that on many implementations, existing file handles can still be used
    /// after a file has been removed.
    virtual void remove(const char* path) = 0;

    /// Maps a portion of the file into the process address space.
    /// Throws if not supported by the platform.
    ///
    /// \note On supported platforms, this function allows for mapping
    /// a range that is greater than the size of the file, i.e. one can
    /// reserve large chunks for virtual memory ahead of time and have the
    /// file grow in order to fill it. No memory that lies outside
    /// the actual size of the file must be read or written to.
    virtual void* memory_map(file& f, u64 offset, u64 length);

    /// Synchronize (part of) a mapped address range with the disk.
    virtual void memory_sync(void* addr, u64 length);

    /// Unmaps a memory mapping created using map().
    virtual void memory_unmap(void* addr, u64 length);

    /// Returns true if the pages that contain [addr, addr + length) are
    /// currently loaded into memory.
    virtual bool memory_in_core(void* addr, u64 length);

    vfs(const vfs&) = delete;
    vfs& operator=(const vfs&) = delete;

protected:
    void check_vfs(file& f) const {
        PREQUEL_CHECK(&f.get_vfs() == this, "The file does not belong to this filesystem.");
    }
};

/// Returns the current platform's file system.
///
/// \relates vfs
vfs& system_vfs();

/// Returns the in-memory file system.
///
/// \relates vfs
vfs& memory_vfs();

} // namespace prequel

#endif // PREQUEL_VFS_HPP
