#include "./io.hpp"

#include "./assert.hpp"
#include "./util.hpp"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace extpp {

class unix_file : public file {
private:
    int m_fd = -1;
    std::string m_name;

public:
    unix_file(int fd, std::string name)
        : m_fd(fd)
        , m_name(std::move(name))
    {}

    unix_file() = default;

    ~unix_file();

    const char* name() const noexcept override { return m_name.c_str(); }

    void read(u64 offset, void* buffer, u32 count) override;

    void write(u64 offset, const void* buffer, u32 count) override;

    u64 file_size() override;

    void truncate(u64 size) override;

    void close() override;

private:
    void check_open();
};

class unix_vfs : public vfs {
private:
public:
    unix_vfs() {}

    const char* name() const noexcept override { return "unix_vfs"; }

    std::unique_ptr<file> open(const char* path, access_t access, flags_t mode) override;
};

static std::error_code get_errno()
{
    return std::error_code(errno, std::system_category());
}

} // namespace extpp

namespace extpp {

unix_file::~unix_file()
{
    if (m_fd != -1)
        ::close(m_fd);
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
            throw std::system_error(ec, __PRETTY_FUNCTION__);
        }

        if (n == 0) {
            // TODO: Exception type
            throw std::runtime_error("Unexpected end of file.");
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
            throw std::system_error(ec, __PRETTY_FUNCTION__);
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
        throw std::system_error(ec, __PRETTY_FUNCTION__);
    }

    return st.st_size;
}

void unix_file::truncate(u64 size)
{
    check_open();
    if (::ftruncate(m_fd, size) == -1) {
        auto ec = get_errno();
        throw std::system_error(ec, __PRETTY_FUNCTION__);
    }
}

void unix_file::close()
{
    if (m_fd != -1) {
        int fd = std::exchange(m_fd, -1);
        if (::close(fd) == -1) {
            auto ec = get_errno();
            throw std::system_error(ec, __PRETTY_FUNCTION__);
        }
    }
}

void unix_file::check_open()
{
    if (m_fd == -1) {
        // TODO Own exception type
        throw std::runtime_error("File is closed.");
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
        // TODO Exceptoin type
        auto ec = get_errno();
        throw std::system_error(ec, __PRETTY_FUNCTION__);
    }

    auto guard = rollback([&]{
        ::close(fd);
    });

    auto ret = std::make_unique<unix_file>(fd, path);
    guard.commit();
    return ret;
}

vfs& system_vfs()
{
     static unix_vfs vfs;
     return vfs;
}

} // namespace extpp
