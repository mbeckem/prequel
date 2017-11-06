#include <extpp/io.hpp>

#include <extpp/assert.hpp>

#include <extpp/detail/rollback.hpp>

#include <cstring>
#include <utility>

#define NOMINAMX
#include <windows.h>

namespace extpp {

class win32_file : public file {
private:
    HANDLE m_handle;
    std::string m_name;

public:
    win32_file(HANDLE handle, std::string name)
        : m_handle(handle)
        , m_name(std::move(name))
    {}

    win32_file()
        : m_handle(INVALID_HANDLE_VALUE)
        , m_name()
    {}

    ~win32_file();

    const char* name() const noexcept override { return m_name.c_str(); }

    void read(u64 offset, void* buffer, u32 count) override;

    void write(u64 offset, const void* buffer, u32 count) override;

    u64 file_size() override;

    void truncate(u64 size) override;

    void close() override;

private:
    void seek(u64 offset);
    void check_open();
};

class win32_vfs : public vfs {
private:

public:
    win32_vfs() {}

    const char* name() const noexcept override { return "win32_vfs"; }

    std::unique_ptr<file> open(const char* path, access_t access, flags_t flags) override;
};

static std::error_code last_error()
{
    return std::error_code(GetLastError(), std::system_category());
}

win32_file::~win32_file()
{
    try {
        close();
    } catch (...) {}
}


void win32_file::seek(u64 offset)
{
    LARGE_INTEGER win_offset;
    win_offset.QuadPart = offset;
    if (!SetFilePointerEx(m_handle, win_offset, nullptr, FILE_BEGIN)) {
        // TODO: Own error exception
        throw std::system_error(last_error(), __PRETTY_FUNCTION__);
    }
}

void win32_file::read(u64 offset, void* buffer, u32 count)
{
    EXTPP_ASSERT(count > 0, "zero read size");
    EXTPP_ASSERT(buffer != nullptr, "buffer nullpointer");

    check_open();
    seek(offset);

    DWORD read;
    u32 remaining = count;
    byte* data = reinterpret_cast<byte*>(buffer);
    while (remaining > 0) {
        if (!ReadFile(m_handle, data, remaining, &read, nullptr)) {
            throw std::system_error(last_error(), __PRETTY_FUNCTION__);
        }

        if (read == 0) {
            // TODO Exception type.
            throw std::runtime_error("End of file.");
        }

        data += read;
        remaining -= read;
    }
}

void win32_file::write(u64 offset, const void* buffer, u32 count)
{
    EXTPP_ASSERT(count > 0, "zero write size");
    EXTPP_ASSERT(buffer != nullptr, "buffer nullpointer");

    check_open();
    seek(offset);

    DWORD written;
    u32 remaining = count;
    const byte* data = reinterpret_cast<const byte*>(buffer);
    while (remaining > 0) {
        if (!WriteFile(m_handle, data, remaining, &written, NULL)) {
            throw std::system_error(last_error(), __PRETTY_FUNCTION__);
        }

        data += written;
        remaining -= written;
    }
}

u64 win32_file::file_size()
{
    check_open();

    LARGE_INTEGER win_size;
    if (!GetFileSizeEx(m_handle, &win_size)) {
        throw std::system_error(last_error(), __PRETTY_FUNCTION__);
    }
    return static_cast<u64>(win_size.QuadPart);
}

void win32_file::truncate(u64 size)
{
    check_open();
    seek(size);

    if (!SetEndOfFile(m_handle)) {
        throw std::system_error(last_error(), __PRETTY_FUNCTION__);
    }
}

void win32_file::close()
{
    if (m_handle != INVALID_HANDLE_VALUE) {
        auto fd = std::exchange(m_handle, INVALID_HANDLE_VALUE);
        if (!CloseHandle(fd)) {
            throw std::system_error(last_error(), __PRETTY_FUNCTION__);
        }
    }
}

void win32_file::check_open()
{
    if (m_handle == INVALID_HANDLE_VALUE) {
        // TODO: Own exception types.
        throw std::runtime_error("File is closed.");
    }
}

static std::wstring to_utf16(const char* path)
{
    size_t length = std::strlen(path);

    std::wstring result;
    int wchars = MultiByteToWideChar(CP_UTF8, 0, path, length, NULL, 0);
    if (wchars > 0) {
        result.resize(wchars);
        MultiByteToWideChar(CP_UTF8, 0, path, length, &result[0], wchars);
    }
    return result;
}

std::unique_ptr<file> win32_vfs::open(const char* path, access_t access, flags_t flags)
{
    EXTPP_ASSERT(path != nullptr, "path null pointer");

    std::wstring utf16_path = to_utf16(path);
    DWORD win_access = access == read_only ? GENERIC_READ
                                           : GENERIC_READ | GENERIC_WRITE;
    DWORD win_creation = flags & open_create ? OPEN_ALWAYS : OPEN_EXISTING;
    DWORD win_share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
    DWORD win_flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;

    HANDLE result = CreateFileW(utf16_path.c_str(),
                                win_access,
                                win_share_mode,
                                NULL,
                                win_creation,
                                win_flags,
                                NULL);
    if (result == INVALID_HANDLE_VALUE) {
        // TODO Own exception type
        throw std::system_error(last_error(), __PRETTY_FUNCTION__);
    }

    auto guard = detail::rollback([&]{
        CloseHandle(result);
    });

    auto ret = std::make_unique<win32_file>(result, path);
    guard.commit();
    return ret;
}

vfs& system_vfs()
{
     static win32_vfs vfs;
     return vfs;
}

} // namespace extpp