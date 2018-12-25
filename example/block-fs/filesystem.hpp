#ifndef PREQUEL_TYPES_HPP
#define PREQUEL_TYPES_HPP

#include <prequel/binary_format.hpp>
#include <prequel/container/btree.hpp>
#include <prequel/container/default_allocator.hpp>
#include <prequel/container/extent.hpp>
#include <prequel/defs.hpp>
#include <prequel/file_engine.hpp>
#include <prequel/fixed_string.hpp>

#include <optional>
#include <string_view>
#include <vector>

namespace blockfs {

using prequel::byte;

using prequel::u16;
using prequel::u32;
using prequel::u64;
using prequel::u8;

static constexpr u32 block_size = 4096;

using fixed_string = prequel::fixed_cstring<32>;

struct file_metadata {
    fixed_string name;   // Name of the file (max 32 chars)
    u32 permissions = 0; // Access flags etc
    u64 mtime = 0;       // Last modification time (file content)
    u64 ctime = 0;       // Last change (file metadata)
    u64 size = 0;        // In bytes

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&file_metadata::name, &file_metadata::permissions,
                                      &file_metadata::mtime, &file_metadata::ctime,
                                      &file_metadata::size);
    }
};

// Contains the meta information associated with a file.
struct file_entry {
    file_metadata metadata;
    prequel::extent::anchor content; // File storage

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&file_entry::metadata, &file_entry::content);
    }

    // Files are indexed by name.
    struct extract_key {
        fixed_string operator()(const file_entry& entry) const { return entry.metadata.name; }
    };
};

// A directory is an indexed collection of file entries.
using directory = prequel::btree<file_entry, file_entry::extract_key>;

// The format of the first block of the filesystem.
struct master_block {
    fixed_string magic{};                     // Magic string that identifies this FS
    u64 partition_size = 0;                   // Size of the partition, in bytes
    prequel::default_allocator::anchor alloc; // Allocates from the rest of the file
    directory::anchor root;                   // Root directory tree

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&master_block::magic, &master_block::partition_size,
                                      &master_block::alloc, &master_block::root);
    }

    static fixed_string magic_value() { return fixed_string("PREQUEL_BLOCK_FS_EXAMPLE_1"); }
};

// Thrown by filesystem operations
struct filesystem_exception : std::runtime_error {
    using runtime_error::runtime_error;
};

struct path_exception : filesystem_exception {
    path_exception(const std::string& message_, const std::string& path_)
        : filesystem_exception(message_)
        , path(path_) {}

    std::string path;
};

struct invalid_file_name : path_exception {
    invalid_file_name(const std::string& path_)
        : path_exception("Invalid filename", path_) {}
};

struct file_not_found : path_exception {
    file_not_found(const std::string& path_)
        : path_exception("File not found", path_) {}
};

struct invalid_file_offset : path_exception {
    invalid_file_offset(const std::string& path_)
        : path_exception("Invalid file ofset", path_) {}
};

class filesystem {
public:
    filesystem(prequel::file_engine& engine);
    ~filesystem();

    std::vector<file_metadata> list_files();

    // Returns true if the file exists.
    bool exists(const char* path);

    // Returns the file's metadata.
    file_metadata get_metadata(const char* path);

    // Updates the modification time of the file to the given value.
    void update_modification_time(const char* path, u64 mtime);

    // Create the file if it does not already exists. Returns true if the file was created.
    // Permissions should be the unix permissions of the new file (e.g. 0755).
    bool create(const char* path, u32 permissons);

    // Rename the file.
    void rename(const char* from, const char* to);

    // Remove the file.
    void remove(const char* path);

    // Resize the file (new space is filled with zeroes).
    void resize(const char* path, u64 size);

    // Read up to size bytes from the file at the given file offset (it's fewer
    // than size bytes at the end of the file).
    size_t read(const char* path, u64 offset, byte* buffer, size_t size);

    // Write size bytes to the file at the given file offset.
    size_t write(const char* path, u64 offset, const byte* buffer, size_t size);

    // Make sure all cached data is written to disk.
    void flush();

    filesystem(const filesystem&) = delete;
    filesystem& operator=(const filesystem&) = delete;

private:
    directory::cursor find_file(const char* path);

    void adapt_capacity(prequel::extent& extent, u64 required_bytes);
    void destroy_file(file_entry& entry);

    void writeback_master();

private:
    prequel::file_engine& m_engine;

    // Master block management.
    prequel::block_handle m_master_handle;
    master_block m_master;
    prequel::anchor_flag m_master_changed;

    // Persistent datastructures.
    prequel::default_allocator m_alloc;
    directory m_root;
};

} // namespace blockfs

#endif // PREQUEL_TYPES_HPP
