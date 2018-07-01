#include <fmt/format.h>

#include <iostream>

#define FUSE_USE_VERSION 26
#include <fuse.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "filesystem.hpp"

using namespace blockfs;

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
 */

// Retrieve the filesystem instance from the fuse context.
// The instance is initialized in main().
static filesystem& filesystem_context() {
    void* ptr = fuse_get_context()->private_data;
    assert(ptr);
    return *static_cast<filesystem*>(ptr);
}

// Transform the c++ metadata object to the format that linux expects.
static struct stat to_stat(const file_metadata& metadata) {
    struct stat st;
    memset(&st, 0, sizeof(st));

    st.st_mode = S_IFREG | metadata.permissions;
    st.st_size = metadata.size;
    st.st_ctim.tv_sec = metadata.ctime;
    st.st_mtim.tv_sec = metadata.mtime;
    st.st_atim.tv_sec = std::max(metadata.ctime, metadata.mtime);

    return st;
}

// List the files in the root directory.
static int fs_readdir(const char* path, void* buf, fuse_fill_dir_t filler,
                      off_t offset, struct fuse_file_info* fi)
{
    extpp::unused(offset, fi);

    if (std::strcmp(path, "/") != 0)
        return -ENOENT;

    filesystem& fs = filesystem_context();

    // Add the special files.
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    // Iterate over all normal files in the root directory and hand them
    // over to the filler function.
    std::string name_buf;
    for (const file_metadata& metadata : fs.list_files()) {
        // Fuse expects zero-terminated strings.
        name_buf.assign(metadata.name.begin(), metadata.name.end());
        struct stat st = to_stat(metadata);
        filler(buf, name_buf.c_str(), &st, 0);
    }
    return 0;
}

// Set file access times (required by "touch").
static int fs_utimens(const char* path, const struct timespec tv[2]) {
    filesystem& fs = filesystem_context();

    if (std::strcmp(path, "/") == 0)
        return -EINVAL;

    // Ignore atime.
    fs.update_modification_time(path, tv[1].tv_sec);
    return 0;
}

// Get file attributes.
static int fs_getattr(const char* path, struct stat *st) {
    std::memset(st, 0, sizeof(*st));

    filesystem& fs = filesystem_context();

    if (std::strcmp(path, "/") == 0) {
        st->st_mode = S_IFDIR | 0755;
        return 0;
    }

    if (!fs.exists(path)) {
        return -ENOENT;
    }

    *st = to_stat(fs.get_metadata(path));
    return 0;
}

// Change the name of an existing file.
static int fs_rename(const char* from, const char* to) {
    if (std::strcmp(from, "/") == 0 || std::strcmp(to, "/") == 0)
        return -EINVAL;

    filesystem& fs = filesystem_context();
    fs.rename(from, to);
    return 0;
}

// Create a file if it does not already exist.
static int fs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    extpp::unused(fi, mode);

    if (!S_ISREG(mode)) {
        return -EINVAL; // Only regular files are supported.
    }

    // Cut off everything but the permission bits.
    u32 permissions = mode & 07000;

    filesystem& fs = filesystem_context();
    fs.create(path, permissions);
    return 0;
}

// "Open" a file. This currently just checks for the file's existence.
static int fs_open(const char* path, struct fuse_file_info* fi) {
    extpp::unused(fi);

    filesystem& fs = filesystem_context();
    if (fs.exists(path)) {
        return 0;
    }
    return -ENOENT;
}

// Read from a file.
static int fs_read(const char* path, char* buf, size_t size, off_t offset,
                   struct fuse_file_info* fi)
{
    extpp::unused(fi);

    if (offset < 0) {
        return -EINVAL;
    }

    filesystem& fs = filesystem_context();
    return (int) fs.read(path, offset, reinterpret_cast<byte*>(buf), size);
}

// Write to a file.
static int fs_write(const char* path, const char* buf, size_t size, off_t offset,
                    struct fuse_file_info* fi)
{
    extpp::unused(fi);

    if (offset < 0) {
        return -EINVAL;
    }

    filesystem& fs = filesystem_context();
    return (int) fs.write(path, offset, reinterpret_cast<const byte*>(buf), size);
}

// Change the size of a file.
static int fs_truncate(const char* path, off_t size) {
    if (size < 0) {
        return -EINVAL;
    }

    filesystem& fs = filesystem_context();
    fs.resize(path, size);
    return 0;
}

// Delete a file.
static int fs_unlink(const char* path) {
    filesystem& fs = filesystem_context();
    fs.remove(path);
    return 0;
}

// Invokes the function and catches all exceptions,
// which are then translated into fitting errno values.
// Exceptions must not leak into C code.
template<auto Function, typename... Args>
static int trap_exceptions(Args... args) {
    try {
        return Function(args...);
    } catch (const invalid_file_name& e) {
        fprintf(stderr, "Access to an invalid file name: %s\n", e.path.c_str());
        return -EINVAL;
    } catch (const invalid_file_offset& e) {
        fprintf(stderr, "Access to an invalid file offset: %s\n", e.path.c_str());
        return -EINVAL;
    } catch (const file_not_found& e) {
        fprintf(stderr, "Access to a non-existant file: %s\n", e.path.c_str());
        return -ENOENT;
    } catch (const std::exception& e) {
        fprintf(stderr, "Error: %s\n", e.what());
        return -EIO;
    } catch (...) {
        fprintf(stderr, "Unknown error\n");
        return -EIO;
    }
}

// Generates a wrapper function pointer for the calling c code.
template<auto Function>
struct trap {
    template<typename... Args>
    using func = int(*)(Args...);

    // Overloaded conversion to function pointer ;)
    template<typename... Args>
    operator func<Args...>() const {
        return [](Args... args) -> int {
            return trap_exceptions<Function>(args...);
        };
    }
};

static const fuse_operations operations = []{
    fuse_operations ops;
    std::memset(&ops, 0, sizeof(ops));

    ops.readdir = trap<fs_readdir>();
    ops.utimens = trap<fs_utimens>();
    ops.getattr = trap<fs_getattr>();
    ops.rename = trap<fs_rename>();
    ops.create = trap<fs_create>();
    ops.open = trap<fs_open>();
    ops.read = trap<fs_read>();
    ops.write = trap<fs_write>();
    ops.truncate = trap<fs_truncate>();
    ops.unlink = trap<fs_unlink>();
    return ops;
}();

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
                "Block-FS options:\n"
                "    --file=<s>       the file that contains the file system.\n"
                "\n",
                outargs->argv[0]);
        fuse_opt_add_arg(outargs, "-ho");
        fuse_main(outargs->argc, outargs->argv, &noops, NULL);
        exit(1);

    case KEY_VERSION:
        fprintf(stderr, "Block-FS version 1\n");
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
                                         extpp::vfs::open_normal);

    // Limits the cache to 128 MB.
    extpp::file_engine engine(*file, block_size, (128 * (1 << 20)) / block_size);
    filesystem fs(engine);

    int ret = fuse_main(args.argc, args.argv, &operations, &fs);

    fs.flush();
    return ret;
}
