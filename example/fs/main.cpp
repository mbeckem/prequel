#include <fmt/format.h>

#include <extpp/btree.hpp>
#include <extpp/engine.hpp>
#include <extpp/default_allocator.hpp>
#include <extpp/default_file_format.hpp>
#include <extpp/extent.hpp>
#include <extpp/math.hpp>

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <sys/stat.h>
#include <sys/types.h>

/*
 * This example program contains a very simple implementation
 * of a FUSE based file system.
 *
 * The file system contains a single root directory and is implemented
 * using an extpp::btree. File entries are stored directly in the tree,
 * and are ordered by their name, which can be up to 32 bytes long.
 *
 * The extent of each file is stored in a extpp::extent, which is
 * range of contiguous blocks. Every non-empty file therefore occupies
 * at least one block on disk. Storing large files in a single contiguous
 * sequence of blocks is a very bad approach (because lots of copying
 * on resize operations will degrade performance), but this is just an example.
 * File storage grows exponentially, i.e. we always allocate 2^N blocks for some N.
 *
 * Little to no thought has been spent on error handling: any I/O errors
 * thrown by extpp remain completely unhandled and will simply crash the process.
 */

namespace example {

using std::size_t;
using std::uint64_t;

// The block size must be a power of two.
static constexpr std::uint32_t block_size = 4096;

// We use short strings to represent file names.
// A real file system would allow for much larger names than 32 bytes.
// However, the current btree implemention can only support fixed length keys.
struct short_string {
public:
    static constexpr size_t max_size = 32;

private:
    /// Unset bytes at the end are zero.
    char m_data[max_size];

public:
    short_string() {
        std::fill(std::begin(m_data), std::end(m_data), 0);
    }

    explicit short_string(std::string_view str) {
        if (str.size() > max_size)
            throw std::runtime_error("String is too long.");

        auto pos = std::copy(str.begin(), str.end(), std::begin(m_data));
        std::fill(pos, std::end(m_data), 0);
    }

    const char* data() const { return m_data; }

    size_t size() const {
        return std::find(std::begin(m_data), std::end(m_data), 0) - std::begin(m_data);
    }

    std::string_view view() const {
        return {m_data, size()};
    }

    friend bool operator<(const short_string& lhs, const short_string& rhs)  {
        return lhs.view() < rhs.view();
    }

    friend bool operator==(const short_string& lhs, const short_string& rhs) {
        return lhs.view() == rhs.view();
    }

    friend bool operator!=(const short_string& lhs, const short_string& rhs) {
        return lhs.view() != rhs.view();
    }
};

// File extent is stored in a range of contiguous blocks.
using file_extent = extpp::extent<block_size>;

// Represents a file and will be stored in the directory's btree.
// Only trivally copyable types are allowed for external datastructures.
struct file {
    short_string name;
    uint64_t size = 0; // In bytes.
    file_extent::anchor extent;

    struct key_extract {
        short_string operator()(const file& f) const { return f.name; }
    };
};

// A directory is an ordered tree of file entries.
using directory = extpp::btree<file, file::key_extract, std::less<>, block_size>;

// The file system has only a single directory, the root directory.
// It's btree is anchored in the first block on disk.
struct header {
    directory::anchor root;
};

class file_system {
    // A helper class that automatically constructs the first block
    // the first time the file is opened and comes with a default
    // allocation strategy.
    extpp::default_file_format<header, block_size> m_fmt;

    // The header block of our filesystem remains pinned in memory.
    extpp::handle<header, block_size> m_header;

    // The root directory that contains all our files.
    directory m_root;

public:
    explicit file_system(std::unique_ptr<extpp::file> file, std::uint32_t cache_size)
        : m_fmt(std::move(file), cache_size)
        , m_header(m_fmt.user_data())
        , m_root(m_header.neighbor(&m_header->root), m_fmt.engine(), m_fmt.allocator())
    {
        m_fmt.allocator().debug_stats(std::cout);
    }

    auto& engine() { return m_fmt.engine(); }
    auto& allocator() { return m_fmt.allocator(); }

    directory& root() { return m_root; }

    std::optional<short_string> filename_from_path(std::string_view path) {
        if (path.size() <= 1 || path.front() != '/')
            return {};
        path = path.substr(1);
        if (path.size() > short_string::max_size)
            return {};
        return short_string(path);
    }

    auto create_file(const short_string& name) {
        file new_entry;
        new_entry.name = name;
        return root().insert(new_entry);
    }

    void flush() {
        m_fmt.flush();
    }
};

// Retrieve the file_system instance from the fuse context.
// The instance is initialized in main().
file_system& fs_context() {
    void* ptr = fuse_get_context()->private_data;
    assert(ptr);
    return *static_cast<file_system*>(ptr);
}

// List the files in the root directory.
static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi)
{
    (void) offset;
    (void) fi;

    if (std::strcmp(path, "/") != 0)
        return -ENOENT;

    file_system& fs = fs_context();

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    // Fuse expects zero-terminated strings.
    std::string name_buf;
    for (const file& entry : fs.root()) {
        name_buf.assign(entry.name.data(), entry.name.size());

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_mode = S_IFREG | 0644;
        st.st_size = entry.size;

        filler(buf, name_buf.c_str(), &st, 0);
    }
    return 0;
}

// Get file attributes.
static int fs_getattr(const char* path, struct stat *st) {
    memset(st, 0, sizeof(*st));

    std::string_view view(path);

    if (view == "/") {
        st->st_mode = S_IFDIR | 0755;
        return 0;
    }

    file_system& fs = fs_context();
    if (auto name = fs.filename_from_path(view)) {
        auto entry = fs.root().find(*name);
        if (entry != fs.root().end()) {
            st->st_mode = S_IFREG | 0644;
            st->st_size = entry->size;
            return 0;
        }
    }
    return -ENOENT;
}

// Change the name of an existing file.
static int fs_rename(const char* from, const char* to) {
    file_system& fs = fs_context();

    auto from_name = fs.filename_from_path(from);
    auto to_name = fs.filename_from_path(to);
    if (!from_name || !to_name)
        return -EINVAL;

    auto entry_pos = fs.root().find(*from_name);
    if (entry_pos == fs.root().end())
        return -ENOENT;

    if (*from_name == *to_name)
        return 0;

    file entry = *entry_pos;
    entry.name = *to_name;
    fs.root().erase(entry_pos); // Invalidates iterators.

    // Try to insert the new entry. If an entry with the same
    // name exists, simply overwrite it.
    auto [to_pos, inserted] = fs.root().insert(entry);
    if (!inserted) {
        file_extent::destroy(to_pos->extent, fs.engine(), fs.allocator());
        fs.root().replace(to_pos, entry);
    }
    return 0;
}

// Create a file if does not already exist.
static int fs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    (void) fi;
    (void) mode;

    std::string_view view();
    file_system& fs = fs_context();
    if (auto name = fs.filename_from_path(path)) {
        auto [iter, inserted] = fs.create_file(*name);
        (void) iter;
        (void) inserted;
        return 0;
    }
    return -EINVAL;
}

// "Open" a file. This currently just checks for the file's existence.
static int fs_open(const char* path, struct fuse_file_info* fi) {
    (void) fi;

    file_system& fs = fs_context();
    if (auto name = fs.filename_from_path(path)) {
        auto entry = fs.root().find(*name);
        // We could allocate some state here and store it in fi->fh
        // for later use in read() etc.
        if (entry != fs.root().end())
            return 0;
    }
    return -EINVAL;
}

// Read from a file.
static int fs_read(const char* path, char* buf, size_t size, off_t offset,
                   struct fuse_file_info* fi)
{
    (void) fi;

    file_system& fs = fs_context();
    auto name = fs.filename_from_path(path);
    if (!name)
        return -ENOENT;

    auto entry_pos = fs.root().find(*name);
    if (entry_pos == fs.root().end())
        return -ENOENT;

    // Very careful with raw pointers into disk based memory.
    // If we would modify the btree, it can move its values around,
    // which will invalidate all pointers.
    auto entry = fs.root().pointer_to(entry_pos);
    if (offset < 0)
        return -EINVAL;
    if (static_cast<uint64_t>(offset) >= entry->size)
        return 0; // End of file.

    const size_t n = std::min(entry->size - offset, size);
    file_extent extent(entry.neighbor(&entry->extent), fs.engine(), fs.allocator());
    extpp::read(fs.engine(), extent.data() + static_cast<uint64_t>(offset), buf, n);
    return n;
}

static bool adapt_capacity(file_extent& extent, uint64_t required_bytes) {
    const uint64_t old_blocks = extent.size();
    const uint64_t required_blocks = extpp::ceil_div(required_bytes, static_cast<uint64_t>(block_size));
    if (required_blocks > old_blocks) {
        // Need to allocate more memory.
        const uint64_t new_blocks = extpp::round_towards_pow2(required_blocks);
        extent.resize(new_blocks);

        // Zero the new memory.
        extpp::zero(extent.engine(),
                    extent.data() + old_blocks * block_size,
                    (new_blocks - old_blocks) * block_size);
        assert(extent.size() * block_size >= required_bytes);
        return true;
    }
    if (required_blocks <= old_blocks / 4) {
        // Shrink.
        const uint64_t new_blocks = extpp::round_towards_pow2(required_blocks);
        extent.resize(new_blocks);
        return true;
    }
    return false;
}

// Write to a file.
static int fs_write(const char* path, const char* buf, size_t size, off_t offset,
                    struct fuse_file_info* fi)
{
    (void) fi;
    if (offset < 0)
        return -EINVAL;

    file_system& fs = fs_context();

    auto name = fs.filename_from_path(path);
    if (!name)
        return -ENOENT;

    auto entry_pos = fs.root().find(*name);
    if (entry_pos == fs.root().end())
        return -ENOENT;

    auto entry = fs.root().pointer_to(entry_pos);
    file_extent extent(entry.neighbor(&entry->extent), fs.engine(), fs.allocator());
    adapt_capacity(extent, offset + size);

    extpp::write(fs.engine(), extent.data() + static_cast<uint64_t>(offset), buf, size);
    if (offset + size > entry->size) {
        entry->size = offset + size;
        entry.dirty();
    }
    return size;
}

// Change the size of a file.
static int fs_truncate(const char* path, off_t off) {
    if (off < 0)
        return -EINVAL;

    const uint64_t new_size = static_cast<uint64_t>(off);

    file_system& fs = fs_context();
    auto name = fs.filename_from_path(path);
    if (!name)
        return -ENOENT;

    auto entry_pos = fs.root().find(*name);
    if (entry_pos == fs.root().end())
        return -ENOENT;

    auto entry = fs.root().pointer_to(entry_pos);
    file_extent extent(entry.neighbor(&entry->extent), fs.engine(), fs.allocator());
    adapt_capacity(extent, new_size);

    if (new_size != entry->size) {
        entry->size = new_size;
        entry.dirty();
    }
    return 0;
}

// Delete a file.
static int fs_unlink(const char* path) {
    file_system& fs = fs_context();
    auto name = fs.filename_from_path(path);
    if (!name)
        return -ENOENT;

    auto entry_pos = fs.root().find(*name);
    if (entry_pos == fs.root().end())
        return -ENOENT;

    file_extent::destroy(entry_pos->extent, fs.engine(), fs.allocator());
    fs.root().erase(entry_pos);
    return 0;
}

static const fuse_operations operations = []{
    fuse_operations ops;
    std::memset(&ops, 0, sizeof(ops));

    ops.readdir = fs_readdir;
    ops.getattr = fs_getattr;
    ops.rename = fs_rename;
    ops.create = fs_create;
    ops.open = fs_open;
    ops.read = fs_read;
    ops.write = fs_write;
    ops.truncate = fs_truncate;
    ops.unlink = fs_unlink;
    return ops;
}();

} // namespace example

static struct options_t {
    const char* filename;
} options;

enum {
     KEY_HELP,
     KEY_VERSION,
};

#define FS_OPTION(t, p)    \
    { t, offsetof(options_t, p), 1}

static const struct fuse_opt option_spec[] = {
    FS_OPTION("--file=%s", filename),

    FUSE_OPT_KEY("-V",          KEY_VERSION),
    FUSE_OPT_KEY("--version",   KEY_VERSION),
    FUSE_OPT_KEY("-h",          KEY_HELP),
    FUSE_OPT_KEY("--help",      KEY_HELP),
    FUSE_OPT_END
};

static int option_callback(void *data, const char *arg, int key, struct fuse_args *outargs)
{
    (void) data;
    (void) arg;

    static const fuse_operations noops{};

    switch (key) {
    case KEY_HELP:
        fprintf(stderr,
                "usage: %s mountpoint [options]\n"
                "\n"
                "general options:\n"
                "    -o opt,[opt...]  mount options\n"
                "    -h   --help      print help\n"
                "    -V   --version   print version\n"
                "\n"
                "Example-FS options:\n"
                "    --file=<s>       the file that contains the file system.\n"
                "\n",
                outargs->argv[0]);
        fuse_opt_add_arg(outargs, "-ho");
        fuse_main(outargs->argc, outargs->argv, &noops, NULL);
        exit(1);

    case KEY_VERSION:
        fprintf(stderr, "Example-FS version 1\n");
        fuse_opt_add_arg(outargs, "--version");
        fuse_main(outargs->argc, outargs->argv, &noops, NULL);
        exit(0);
    }
    return 1;
}

int main(int argc, char* argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    options.filename = strdup("");
    if (fuse_opt_parse(&args, &options, option_spec, option_callback) == -1) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }

    auto file = extpp::system_vfs().open(options.filename,
                                         extpp::vfs::read_write,
                                         extpp::vfs::open_create);
    example::file_system fs(std::move(file), 128);
    return fuse_main(args.argc, args.argv, &example::operations, &fs);
}
