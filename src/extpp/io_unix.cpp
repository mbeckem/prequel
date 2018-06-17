#include <extpp/io.hpp>

#include <extpp/assert.hpp>
#include <extpp/exception.hpp>
#include <extpp/math.hpp>
#include <extpp/detail/deferred.hpp>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>

namespace extpp {

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
public:
    unix_vfs() {}

    const char* name() const noexcept override { return "unix_vfs"; }

    std::unique_ptr<file> open(const char* path, access_t access, flags_t mode) override;

    std::unique_ptr<file> create_temp() override;

    void* memory_map(file& f, u64 offset, u64 length) override;

    void memory_sync(void* addr, u64 length) override;

    void memory_unmap(void* addr, u64 length) override;
};

static std::error_code get_errno()
{
    return std::error_code(errno, std::system_category());
}

} // namespace extpp

namespace extpp {

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
    EXTPP_ASSERT(buffer != nullptr, "null buffer");
    EXTPP_ASSERT(count > 0, "zero sized read");

    check_open();

    byte* data = reinterpret_cast<byte*>(buffer);
    while (count > 0) {
        ssize_t n = ::pread(m_fd, data, count, offset);
        if (n == -1) {
            auto ec = get_errno();
            EXTPP_THROW(io_error(
                fmt::format("Failed to read from `{}`: {}.",
                            name(), ec.message())));
        }

        if (n == 0) {
            EXTPP_THROW(io_error(
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
    EXTPP_ASSERT(buffer != nullptr, "null buffer");
    EXTPP_ASSERT(count > 0, "zero sized write");

    check_open();

    const byte* data = reinterpret_cast<const byte*>(buffer);
    while (count > 0) {
        ssize_t n = ::pwrite(m_fd, data, count, offset);
        if (n == -1) {
            auto ec = get_errno();
            EXTPP_THROW(io_error(
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
        EXTPP_THROW(io_error(
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
        EXTPP_THROW(io_error(
            fmt::format("Failed to truncate `{}`: {}.",
                        name(), ec.message())));
    }
}

void unix_file::sync()
{
    check_open();
    if (::fsync(m_fd) == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(
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
            EXTPP_THROW(io_error(
                fmt::format("Failed to close `{}`: {}.",
                            name(), ec.message())));
        }
    }
}

void unix_file::check_open() const
{
    if (m_fd == -1) {
        EXTPP_THROW(io_error("File is closed."));
    }
}

std::unique_ptr<file> unix_vfs::open(const char* path, access_t access, flags_t mode)
{
    EXTPP_ASSERT(path != nullptr, "path null pointer");

    int flags = access == read_only ? O_RDONLY : O_RDWR;
    if (mode & open_create) {
        flags |= O_CREAT;
    }
    int createmode = S_IRUSR | S_IWUSR;

    int fd = ::open(path, flags, createmode);
    if (fd == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(
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
    std::string name = "extpp-XXXXXX";

    int fd = ::mkstemp(name.data());
    if (fd == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(
            fmt::format("Failed to create temporary file: {}.", ec.message())
        ));
    }

    detail::deferred guard = [&]{
        ::close(fd);
    };

    if (unlink(name.data()) == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(
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

    void* result = ::mmap(nullptr, narrow<size_t>(length), prot, flags, uf.fd(), narrow<size_t>(offset));
    if (result == MAP_FAILED) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(
            fmt::format("Failed to map `{}`: {}.", f.name(), ec.message())
        ));
    }
    return result;
}

void unix_vfs::memory_sync(void* addr, u64 length) {
    if (::msync(addr, length, MS_SYNC) == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(fmt::format("Failed to sync: {}.", ec.message())));
    }
}

void unix_vfs::memory_unmap(void* addr, u64 length) {
    if (::munmap(addr, narrow<size_t>(length)) == -1) {
        auto ec = get_errno();
        EXTPP_THROW(io_error(fmt::format("Failed to unmap: {}.", ec.message())));
    }
}

vfs& system_vfs()
{
     static unix_vfs vfs;
     return vfs;
}

} // namespace extpp
