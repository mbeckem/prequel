#include <prequel/vfs.hpp>

#include <prequel/assert.hpp>
#include <prequel/exception.hpp>
#include <prequel/math.hpp>
#include <prequel/detail/deferred.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

namespace prequel {

class unix_vfs;

class unix_file : public file {
private:
    int m_fd = -1;
    std::string m_name;

public:
    unix_file(unix_vfs& vfs, int fd, std::string name);

    unix_file() = default;

    ~unix_file();

    const char* name() const noexcept override { return m_name.c_str(); }

    int fd() const;

    void read(u64 offset, void* buffer, u32 count) override;

    void write(u64 offset, const void* buffer, u32 count) override;

    u64 file_size() override;

    void truncate(u64 size) override;

    void sync() override;

    void close() override;

private:
    void check_open() const;
};

class unix_vfs : public vfs {
private:
    // System wide page size. Usually 4K.
    size_t m_page_size;

public:
    unix_vfs();

    const char* name() const noexcept override { return "unix_vfs"; }

    std::unique_ptr<file> open(const char* path, access_t access, flags_t mode) override;

    std::unique_ptr<file> create_temp() override;

    void* memory_map(file& f, u64 offset, u64 length) override;

    void memory_sync(void* addr, u64 length) override;

    void memory_unmap(void* addr, u64 length) override;

    bool memory_in_core(void* addr, u64 length) override;
};

static std::error_code get_errno()
{
    return std::error_code(errno, std::system_category());
}

static size_t get_pagesize() {
    long size = ::sysconf(_SC_PAGESIZE);
    if (size == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to query the page size: {}\n", ec.message())
        ));
    }

    return static_cast<size_t>(size);
}

unix_file::unix_file(unix_vfs& v, int fd, std::string name)
    : file(v)
    , m_fd(fd)
    , m_name(std::move(name))
{}

unix_file::~unix_file()
{
    if (m_fd != -1)
        ::close(m_fd);
}

int unix_file::fd() const {
    check_open();

    return m_fd;
}

void unix_file::read(u64 offset, void* buffer, u32 count)
{
    PREQUEL_ASSERT(buffer != nullptr, "null buffer");
    PREQUEL_ASSERT(count > 0, "zero sized read");

    check_open();

    byte* data = reinterpret_cast<byte*>(buffer);
    while (count > 0) {
        ssize_t n = ::pread(m_fd, data, count, offset);
        if (n == -1) {
            auto ec = get_errno();
            PREQUEL_THROW(io_error(
                fmt::format("Failed to read from `{}`: {}.",
                            name(), ec.message())));
        }

        if (n == 0) {
            PREQUEL_THROW(io_error(
                fmt::format("Failed to read from `{}`: Unexpected end of file.",
                            name())));
        }

        count -= n;
        offset += n;
        data += n;
    }
}

void unix_file::write(u64 offset, const void* buffer, u32 count)
{
    PREQUEL_ASSERT(buffer != nullptr, "null buffer");
    PREQUEL_ASSERT(count > 0, "zero sized write");

    check_open();

    const byte* data = reinterpret_cast<const byte*>(buffer);
    while (count > 0) {
        ssize_t n = ::pwrite(m_fd, data, count, offset);
        if (n == -1) {
            auto ec = get_errno();
            PREQUEL_THROW(io_error(
                fmt::format("Failed to write to `{}`: {}.",
                            name(), ec.message())));
        }

        count -= n;
        offset += n;
        data += n;
    }
}

u64 unix_file::file_size()
{
    check_open();

    struct stat st;
    if (::fstat(m_fd, &st) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to get attributes of `{}`: {}.",
                        name(), ec.message())));
    }

    return st.st_size;
}

void unix_file::truncate(u64 size)
{
    check_open();
    if (::ftruncate(m_fd, size) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to truncate `{}`: {}.",
                        name(), ec.message())));
    }
}

void unix_file::sync()
{
    check_open();
    if (::fsync(m_fd) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to sync `{}`: {}.",
                        name(), ec.message())));
    }
}

void unix_file::close()
{
    if (m_fd != -1) {
        int fd = std::exchange(m_fd, -1);
        if (::close(fd) == -1) {
            auto ec = get_errno();
            PREQUEL_THROW(io_error(
                fmt::format("Failed to close `{}`: {}.",
                            name(), ec.message())));
        }
    }
}

void unix_file::check_open() const
{
    if (m_fd == -1) {
        PREQUEL_THROW(io_error("File is closed."));
    }
}

unix_vfs::unix_vfs() {
    m_page_size = get_pagesize();
}

std::unique_ptr<file> unix_vfs::open(const char* path, access_t access, flags_t mode)
{
    PREQUEL_ASSERT(path != nullptr, "path null pointer");

    int flags = access == read_only ? O_RDONLY : O_RDWR;
    if (mode & open_create) {
        flags |= O_CREAT;
    }
    int createmode = S_IRUSR | S_IWUSR;

    int fd = ::open(path, flags, createmode);
    if (fd == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to open `{}`: {}.", path, ec.message())
        ));
    }

    detail::deferred guard = [&]{
        ::close(fd);
    };

    auto ret = std::make_unique<unix_file>(*this, fd, path);
    guard.disable();
    return ret;
}

std::unique_ptr<file> unix_vfs::create_temp()
{
    std::string name = "prequel-XXXXXX";

    int fd = ::mkstemp(name.data());
    if (fd == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to create temporary file: {}.", ec.message())
        ));
    }

    detail::deferred guard = [&]{
        ::close(fd);
    };

    if (unlink(name.data()) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to unlink temporary file: {}.", ec.message())
        ));
    }

    auto ret = std::make_unique<unix_file>(*this, fd, std::move(name));
    guard.disable();
    return ret;
}

void* unix_vfs::memory_map(file& f, u64 offset, u64 length) {
    check_vfs(f);

    unix_file& uf = static_cast<unix_file&>(f);
    int prot = PROT_READ | PROT_WRITE; // TODO: Not writable if file is read only
    int flags = MAP_SHARED;

    if (offset > std::numeric_limits<off_t>::max()) {
        PREQUEL_THROW(bad_argument(
            fmt::format("File offset {} is too large for this platform.", offset)
        ));
    }

    if (length > std::numeric_limits<size_t>::max()) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Length {} is too large for this platform.", length)
        ));
    }

    void* result = ::mmap(nullptr, static_cast<size_t>(length), prot, flags, uf.fd(), static_cast<off_t>(offset));
    if (result == MAP_FAILED) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to map `{}`: {}.", f.name(), ec.message())
        ));
    }
    return result;
}

void unix_vfs::memory_sync(void* addr, u64 length) {
    if (length > std::numeric_limits<size_t>::max()) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Length {} is too large for this platform.", length)
        ));
    }

    if (::msync(addr, length, MS_SYNC) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(fmt::format("Failed to sync: {}.", ec.message())));
    }
}

void unix_vfs::memory_unmap(void* addr, u64 length) {
    if (length > std::numeric_limits<size_t>::max()) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Length {} is too large for this platform.", length)
        ));
    }

    if (::munmap(addr, length) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(fmt::format("Failed to unmap: {}.", ec.message())));
    }
}

bool unix_vfs::memory_in_core(void* addr, u64 length) {
    if (length > std::numeric_limits<size_t>::max()) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Length {} is too large for this platform.", length)
        ));
    }

    // Must make sure that the address passed to mincore is a multiple of the page size.
    const uintptr_t addr_value = reinterpret_cast<uintptr_t>(addr);
    const uintptr_t addr_rounded = (reinterpret_cast<uintptr_t>(addr) / m_page_size) * m_page_size;
    const size_t length_rounded = static_cast<size_t>(length) + (addr_value - addr_rounded);

    // Implementation limit for the maximum number of pages we can query at a time:
    static constexpr size_t max_pages = 1024;
    const size_t npages = (length_rounded + m_page_size - 1) / m_page_size;
    if (npages > max_pages) {
        PREQUEL_THROW(bad_operation(
            fmt::format("Querying too many pages: {} (current max is {})", npages, max_pages)
        ));
    }

    // Query the in-core status of all relevant pages.
    unsigned char page_status[max_pages];
    std::memset(page_status, 0, sizeof(page_status));
    if (::mincore(reinterpret_cast<void*>(addr_rounded), length_rounded, page_status) == -1) {
        auto ec = get_errno();
        PREQUEL_THROW(io_error(
            fmt::format("Failed to call mincore(): {}", ec.message())
        ));
    }

    // Return true if all relevant pages are in core.
    bool in_core = true;
    for (size_t i = 0; i < npages; ++i) {
        in_core &= (page_status[i] & 1);
    }
    return in_core;
}

vfs& system_vfs()
{
     static unix_vfs vfs;
     return vfs;
}

} // namespace prequel
