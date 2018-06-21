#include <fmt/format.h>

#include <extpp/btree.hpp>
#include <extpp/engine.hpp>
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
 * The content of each file is stored in a extpp::extent, which is a
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
using std::uint32_t;
using std::uint64_t;
using std::optional;

// The block size must be a power of two.
static constexpr uint32_t block_size = 4096;

// We use short strings to represent file names.
// A real file system would allow for much larger names than 32 bytes.
// However, the current btree implemention can only support fixed length keys,
// so variable length names would have to be stored by address with their
// real data somewhere else.
struct file_name {
public:
    static constexpr size_t max_size = 32;

private:
    // Unset bytes at the end are zero.
    // String is not null terminated (i.e. all max_size bytes can be used).
    char m_data[max_size];

public:
    file_name() {
        std::memset(m_data, 0, max_size);
    }

    explicit file_name(std::string_view str) {
        if (str.size() > max_size)
            throw std::runtime_error("String is too long.");

        std::memcpy(m_data, str.data(), str.size());
        std::memset(m_data + str.size(), 0, max_size - str.size());
    }

    const char* begin() const { return m_data; }
    const char* end() const { return m_data + size(); }

    const char* data() const { return m_data; }

    size_t size() const {
        // Index of first 0 byte (or max_size).
        return std::find(std::begin(m_data), std::end(m_data), 0) - std::begin(m_data);
    }

    static constexpr auto get_binary_format() {
        return extpp::make_binary_format(&file_name::m_data);
    }

    friend bool operator<(const file_name& lhs, const file_name& rhs)  {
        return std::lexicographical_compare(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator==(const file_name& lhs, const file_name& rhs) {
        return std::equal(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    }

    friend bool operator!=(const file_name& lhs, const file_name& rhs) {
        return !(lhs == rhs);
    }
};

// Represents a file and will be stored in the directory's btree.
struct file {
    file_name name;
    uint64_t size = 0; // In bytes.
    extpp::extent::anchor extent;

    static constexpr auto get_binary_format() {
        return extpp::make_binary_format(&file::name, &file::size, &file::extent);
    }

    // Files are indexed by their name.
    struct key_extract {
        file_name operator()(const file& f) const { return f.name; }
    };
};

// A directory is an ordered tree of file entries, indexed by file name.
using directory = extpp::btree<file, file::key_extract>;

// The file system has only a single directory.
// It's btree is anchored in the first block on disk.
struct header {
    directory::anchor root;

    static constexpr auto get_binary_format() {
        return extpp::make_binary_format(&header::root);
    }
};

class file_system {
    // A helper class that automatically constructs the first block
    // the first time the file is opened and comes with a default
    // allocation strategy.
    extpp::default_file_format<header> m_fmt;

    /// The header (content of the first block on disk).
    header m_header;
    extpp::anchor_flag m_header_changed;

    // The root directory that contains all our files.
    directory m_root;

public:
    explicit file_system(extpp::file& file, uint32_t cache_size)
        : m_fmt(file, block_size, cache_size)
        , m_header(m_fmt.get_user_data().get())
        , m_root(extpp::anchor_handle(m_header, m_header_changed).member<&header::root>(),
                 m_fmt.get_allocator())
    {
        m_fmt.get_allocator().dump(std::cout);
    }

    ~file_system() {
        flush();
    }

    extpp::engine& get_engine() { return m_fmt.get_engine(); }
    extpp::allocator& get_allocator() { return m_fmt.get_allocator(); }

    directory& root() { return m_root; }

    optional<file_name> filename_from_path(std::string_view path) {
        if (path.size() <= 1 || path.front() != '/')
            return {};
        path = path.substr(1);
        if (path.size() > file_name::max_size)
            return {};
        return file_name(path);
    }

    /// Attempts to create a new (empty) file with the given name.
    /// Returns the cursor (a reference to the location in the btree)
    /// and a boolean (true if a file with that name did not exist
    /// and the entry is now new).
    std::pair<directory::cursor, bool> create_file(const file_name& name) {
        file new_entry;
        new_entry.name = name;
        return root().insert(new_entry);
    }

    /// Search for the file. Returns a cursor to the file entry or an invalid cursor
    /// if no such file exists.
    directory::cursor find_file(std::string_view path) {
        optional<file_name> filename = filename_from_path(path);
        if (!filename)
            return {};
        return root().find(*filename);
    }

    /// Return the file's data to the allocator.
    void destroy_file(file entry) {
        extpp::extent data(make_anchor_handle(entry.extent), m_fmt.get_allocator());
        data.reset();
    }

    /// Flush unwritten data to disk.
    void flush() {
        std::cout << "Flush" <<  std::endl;

        if (m_header_changed) {
            m_fmt.get_user_data().set(m_header);
            m_header_changed.reset();
        }

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

    // Iterate over all files in the root directory and push them into FUSE's filler function.
    for (auto cursor = fs.root().create_cursor(directory::seek_min); cursor; cursor.move_next()) {
        file entry = cursor.get();
        name_buf.assign(entry.name.data(), entry.name.size());

        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_mode = S_IFREG | 0644;    // All files are regular in this example.
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
    directory::cursor cursor = fs.find_file(view);
    if (cursor) {
        file entry = cursor.get();
        st->st_mode = S_IFREG | 0644;
        st->st_size = entry.size;
        return 0;
    }
    return -ENOENT;
}

// Change the name of an existing file.
static int fs_rename(const char* from, const char* to) {
    file_system& fs = fs_context();

    optional<file_name> from_name = fs.filename_from_path(from);
    optional<file_name> to_name = fs.filename_from_path(to);
    if (!from_name || !to_name)
        return -EINVAL;

    auto cursor = fs.root().find(*from_name);
    if (!cursor)
        return -ENOENT;

    if (*from_name == *to_name)
        return 0;

    file entry = cursor.get();
    entry.name = *to_name;
    cursor.erase();

    // Try to insert the new entry. If an entry with the same
    // name exists, simply overwrite it.
    auto [new_cursor, inserted] = fs.root().insert(entry);
    if (!inserted) {
        fs.destroy_file(new_cursor.get()); // Remove the old file's data.
        new_cursor.set(entry);
    }
    return 0;
}

// Create a file if it does not already exist.
static int fs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    (void) fi;
    (void) mode;

    file_system& fs = fs_context();
    if (optional<file_name> name = fs.filename_from_path(path)) {
        std::pair<directory::cursor, bool> result = fs.create_file(*name);
        (void) result; // Ignored for now; just ensure that the file exists.
        return 0;
    }
    return -EINVAL;
}

// "Open" a file. This currently just checks for the file's existence.
static int fs_open(const char* path, struct fuse_file_info* fi) {
    (void) fi;

    file_system& fs = fs_context();
    directory::cursor cursor = fs.find_file(path);
    if (cursor) {
        return 0;
    }
    return -ENOENT;
}

// Read from a file.
static int fs_read(const char* path, char* buf, size_t size, off_t offset,
                   struct fuse_file_info* fi)
{
    (void) fi;

    file_system& fs = fs_context();
    directory::cursor cursor = fs.find_file(path);
    if (!cursor)
        return -ENOENT;

    file entry = cursor.get();
    if (offset < 0)
        return -EINVAL;
    if (static_cast<uint64_t>(offset) >= entry.size)
        return 0; // End of file.

    const size_t n = std::min(entry.size - offset, size);

    extpp::anchor_flag extent_changed;
    extpp::extent extent(extpp::anchor_handle(entry.extent, extent_changed), fs.get_allocator());
    extpp::read(fs.get_engine(), fs.get_engine().to_address(extent.data()) + static_cast<uint64_t>(offset), buf, n);
    if (extent_changed) {
        cursor.set(entry);
    }

    return n;
}

// Grows or shrinks the given extent to fit the number of required bytes.
static void adapt_capacity(extpp::extent& extent, uint64_t required_bytes) {
    const uint64_t old_blocks = extent.size();
    const uint64_t required_blocks = extpp::ceil_div(required_bytes, static_cast<uint64_t>(block_size));
    if (required_blocks > old_blocks) {
        // Need to allocate more memory.
        const uint64_t new_blocks = extpp::round_towards_pow2(required_blocks);
        extent.resize(new_blocks);

        // Zero the new memory.
        extpp::zero(extent.get_engine(),
                    extent.get_engine().to_address(extent.data()) + old_blocks * block_size,
                    (new_blocks - old_blocks) * block_size);
        assert(extent.size() * block_size >= required_bytes);
        return;
    }
    if (required_blocks <= old_blocks / 4) {
        // Shrink.
        const uint64_t new_blocks = extpp::round_towards_pow2(required_blocks);
        extent.resize(new_blocks);
        return;
    }
}

// Write to a file.
static int fs_write(const char* path, const char* buf, size_t size, off_t offset,
                    struct fuse_file_info* fi)
{
    (void) fi;
    if (offset < 0)
        return -EINVAL;

    file_system& fs = fs_context();
    directory::cursor cursor = fs.find_file(path);
    if (!cursor)
        return -ENOENT;

    file entry = cursor.get();
    extpp::anchor_flag entry_changed;

    // Write the data to the file.
    extpp::extent extent(extpp::anchor_handle(entry.extent, entry_changed), fs.get_allocator());
    if (offset + size > entry.size) {
        adapt_capacity(extent, offset + size);
    }
    extpp::write(fs.get_engine(), fs.get_engine().to_address(extent.data()) + static_cast<uint64_t>(offset), buf, size);

    // Update the file entry if something changed.
    if (offset + size > entry.size) {
        entry.size = offset + size;
        entry_changed.set();
    }
    if (entry_changed) {
        cursor.set(entry);
    }
    return size;
}

// Change the size of a file.
static int fs_truncate(const char* path, off_t off) {
    if (off < 0)
        return -EINVAL;

    const uint64_t new_size = static_cast<uint64_t>(off);

    file_system& fs = fs_context();
    directory::cursor cursor = fs.find_file(path);
    if (!cursor)
        return -ENOENT;

    file entry = cursor.get();
    extpp::anchor_flag entry_changed;

    extpp::extent extent(extpp::anchor_handle(entry.extent, entry_changed), fs.get_allocator());
    adapt_capacity(extent, new_size);

    if (new_size != entry.size) {
        entry.size = new_size;
        entry_changed.set();
    }
    if (entry_changed) {
        cursor.set(entry);
    }
    return 0;
}

// Delete a file.
static int fs_unlink(const char* path) {
    file_system& fs = fs_context();
    directory::cursor cursor = fs.find_file(path);
    if (!cursor)
        return -ENOENT;

    fs.destroy_file(cursor.get());
    cursor.erase();
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
        fprintf(stderr, "Example-FS version 2\n");
        fuse_opt_add_arg(outargs, "--version");
        fuse_main(outargs->argc, outargs->argv, &noops, NULL);
        exit(0);
    }
    return 1;
}

/*
 * Call the program with --file=myfile.blob to specify where the "file system" contents
 * shall be stored. The path will be forwarded to the file engine used by this program.
 *
 * NOTE: Program must be launched with -s to force single threaded fuse mode.
 * Multithreaded access to extpp datastructures is not supported (for now).
 *
 * Call the program with -d for debug output or -f for foreground operation (fuse
 * will deamonize otherwise).
 */
int main(int argc, char* argv[]) {
    struct fuse_args args = FUSE_ARGS_INIT(argc, argv);

    options.filename = strdup("");
    if (fuse_opt_parse(&args, &options, option_spec, option_callback) == -1) {
        std::cerr << "Failed to parse command line arguments." << std::endl;
        return 1;
    }

    if (strcmp(options.filename, "") == 0) {
        std::cerr << "You must specify the --file= option." << std::endl;
        return 1;
    }

    auto file = extpp::system_vfs().open(options.filename,
                                         extpp::vfs::read_write,
                                         extpp::vfs::open_create);
    example::file_system fs(*file, 128);
    return fuse_main(args.argc, args.argv, &example::operations, &fs);
}
