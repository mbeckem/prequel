#include <prequel/vfs.hpp>

#include <prequel/assert.hpp>
#include <prequel/deferred.hpp>
#include <prequel/exception.hpp>

#include <fmt/format.h>

#include <cstring>
#include <utility>

#include <windows.h>
#include <Ntddstor.h>

namespace prequel {

namespace {

/* std::error_code with system_category does not work on mingw */
class win32_error {
public:
    win32_error() = default;
    win32_error(DWORD error): m_err(error) {}

    void clear() {
        m_err = NO_ERROR;
    }

    std::string message() const;

    bool operator==(const win32_error& other) const {
        return m_err == other.m_err;
    }

    bool operator!=(const win32_error& other) const {
        return m_err != other.m_err;
    }

    explicit operator bool() const { return m_err != NO_ERROR; }

private:
    DWORD m_err = NO_ERROR;
};

};

class win32_file : public file {
public:
    win32_file(vfs& v, HANDLE handle, std::string name, bool read_only);

    ~win32_file();

    bool read_only() const noexcept override { return m_read_only; }

    const char* name() const noexcept override { return m_name.c_str(); }

    u32 block_size() const noexcept override { return m_block_size; };

    void read(u64 offset, void* buffer, u32 count) override;

    void write(u64 offset, const void* buffer, u32 count) override;

    u64 file_size() override;

    u64 max_file_size() override { return u64(-1); }

    void truncate(u64 size) override;

    void sync() override;

    void close() override;

private:
    void check_open();
    void seek(u64 offset);

private:
    HANDLE m_handle;
    std::string m_name;
    bool m_read_only = false;
    u32 m_block_size = 0;
};

class win32_vfs : public vfs {
public:
    win32_vfs() {}

    const char* name() const noexcept override { return "win32_vfs"; }

    std::unique_ptr<file> open(const char* path, access_t access, int flags) override;

    std::unique_ptr<file> create_temp() override;

    void remove(const char* path) override;

private:
    enum native_flags_t {
        delete_on_close = 1 << 0,
    };

    HANDLE create_file(const wchar_t* path, access_t access, int flags, int native_flags, win32_error& err);
};

static win32_error last_error() {
    return win32_error(GetLastError());
}

static std::wstring to_utf16(const char* str, size_t length) {
    PREQUEL_CHECK(length <= std::numeric_limits<int>::max(),
                  "Length cannot be represented as an int.");

    std::wstring result;
    int wchars = MultiByteToWideChar(CP_UTF8, 0, str, static_cast<int>(length), NULL, 0);
    if (wchars > 0) {
        result.resize(wchars);
        MultiByteToWideChar(CP_UTF8, 0, str, static_cast<int>(length), &result[0], wchars);
    }
    return result;
}

static std::wstring to_utf16(const char* str) {
    return to_utf16(str, std::strlen(str));
}

static std::wstring to_utf16(const std::string& str) {
    return to_utf16(str.data(), str.size());
}

static std::string to_utf8(const wchar_t* str, size_t length) {
    PREQUEL_CHECK(length <= std::numeric_limits<int>::max(),
                  "Length cannot be represented as an int.");

    std::string result;
    int chars = WideCharToMultiByte(CP_UTF8, 0, str, static_cast<int>(length), nullptr, 0, nullptr, nullptr);
    if (chars > 0) {
        result.resize(chars);
        WideCharToMultiByte(CP_UTF8, 0, str, static_cast<int>(length), &result[0], chars, nullptr, nullptr);
    }
    return result;
}

static std::string to_utf8(const wchar_t* str) {
    return to_utf8(str, std::wcslen(str));
}

static std::string to_utf8(const std::wstring& str) {
    return to_utf8(str.data(), str.size());
}

// The final path for a file is the fully resolved file name (i.e. following all links).
static std::wstring final_path_name(HANDLE file, const char* name) {
    wchar_t buffer[MAX_PATH + 1];
    std::memset(buffer, 0, sizeof(buffer));

    DWORD ret = GetFinalPathNameByHandleW(file, buffer, MAX_PATH, FILE_NAME_NORMALIZED | VOLUME_NAME_GUID);
    if (ret == 0) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to get final path name of `{}`: {}",
                                           name, err.message())));
    }
    if (ret > MAX_PATH) {
        PREQUEL_THROW(io_error(fmt::format("Insufficient buffer size for final path name of `{}`: need {} chars.",
                                           name, ret)));
    }
    return std::wstring(buffer);
}

// The volume path can be used to open a device handle.
// Make sure to use an absolute path (including a drive identifier) because
// the windows api is retarded.
//
// Quote:
//      If you specify a relative directory or file name without a volume qualifier,
//      GetVolumePathName returns the drive letter of the boot volume
static std::wstring volume_path(const std::wstring& file_path) {
    wchar_t buffer[MAX_PATH + 1];
    std::memset(buffer, 0, sizeof(buffer));

    BOOL ret = GetVolumePathNameW(file_path.c_str(), buffer, MAX_PATH);
    if (ret == 0) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to get volume path for `{}`: {}",
                               to_utf8(file_path), err.message())));
    }
    return std::wstring(buffer);
}

// Queries the volume for its physical sector size.
static u32 physical_sector_size(std::wstring volume_path) {
    // Remove the trailing backslash to open the device.
    if (!volume_path.empty() && volume_path.back() == L'\\')
        volume_path.pop_back();

    HANDLE device = CreateFileW(volume_path.c_str(), 0, 0, nullptr, OPEN_EXISTING, 0, nullptr);
    if (device == INVALID_HANDLE_VALUE) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to open volume device file `{}`: {}",
                                           to_utf8(volume_path), err.message())));
    }
    deferred cleanup = [&] {
        CloseHandle(device);
    };

    STORAGE_PROPERTY_QUERY storage_query;
    memset(&storage_query, 0, sizeof(storage_query));
    storage_query.PropertyId = StorageAccessAlignmentProperty;
    storage_query.QueryType = PropertyStandardQuery;

    STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR disk_alignment;
    std::memset(&disk_alignment, 0, sizeof(disk_alignment));

    DWORD outsize;
    BOOL success = DeviceIoControl(device, IOCTL_STORAGE_QUERY_PROPERTY,
                                   &storage_query, sizeof(storage_query),
                                   &disk_alignment, sizeof(disk_alignment),
                                   &outsize, nullptr);
    if (success == 0) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to query storage properties of `{}`: {}",
                                           to_utf8(volume_path), err.message())));
    }

    return disk_alignment.BytesPerPhysicalSector;
}

std::string win32_error::message() const {
   wchar_t* buffer = nullptr;
   DWORD result = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER
                                | FORMAT_MESSAGE_FROM_SYSTEM
                                | FORMAT_MESSAGE_IGNORE_INSERTS,
                                nullptr,
                                m_err,
                                0,                     // Language
                                (wchar_t*) &buffer,    // System allocates a buffer
                                0,                     // Minimum size of allocated buffer
                                nullptr);              // Arguments
   if (result == 0) {
       PREQUEL_THROW(bad_operation("Failed to format system error message."));
   }

   deferred cleanup = [&] {
        LocalFree(buffer);
   };

   return to_utf8(buffer);
}

win32_file::win32_file(vfs& v, HANDLE handle, std::string name, bool read_only)
    : file(v)
    , m_handle(handle)
    , m_name(std::move(name))
    , m_read_only(read_only) {}

win32_file::~win32_file() {
    try {
        close();
    } catch (...) {
    }
}

void win32_file::seek(u64 offset) {
    LARGE_INTEGER win_offset;
    win_offset.QuadPart = offset;
    if (!SetFilePointerEx(m_handle, win_offset, nullptr, FILE_BEGIN)) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to seek in `{}`: {}", name(), err.message())));
    }
}

void win32_file::read(u64 offset, void* buffer, u32 count) {
    PREQUEL_ASSERT(count > 0, "zero read size");
    PREQUEL_ASSERT(buffer != nullptr, "buffer nullpointer");

    check_open();
    seek(offset);

    DWORD read;
    u32 remaining = count;
    byte* data = reinterpret_cast<byte*>(buffer);
    while (remaining > 0) {
        if (!ReadFile(m_handle, data, remaining, &read, nullptr)) {
            auto err = last_error();
            PREQUEL_THROW(
                io_error(fmt::format("Failed to read from `{}`: {}", name(), err.message())));
        }

        if (read == 0) {
            PREQUEL_THROW(
                io_error(fmt::format("Failed to read from `{}`: Unexpected end of file.", name())));
        }

        data += read;
        remaining -= read;
    }
}

void win32_file::write(u64 offset, const void* buffer, u32 count) {
    PREQUEL_ASSERT(count > 0, "zero write size");
    PREQUEL_ASSERT(buffer != nullptr, "buffer nullpointer");

    check_open();
    seek(offset);

    DWORD written;
    u32 remaining = count;
    const byte* data = reinterpret_cast<const byte*>(buffer);
    while (remaining > 0) {
        if (!WriteFile(m_handle, data, remaining, &written, NULL)) {
            auto err = last_error();
            PREQUEL_THROW(
                io_error(fmt::format("Failed to write to `{}`: {}", name(), err.message())));
        }

        data += written;
        remaining -= written;
    }
}

u64 win32_file::file_size() {
    check_open();

    LARGE_INTEGER win_size;
    if (!GetFileSizeEx(m_handle, &win_size)) {
        auto err = last_error();
        PREQUEL_THROW(
            io_error(fmt::format("Failed to get size of `{}`: {}", name(), err.message())));
    }
    return static_cast<u64>(win_size.QuadPart);
}

void win32_file::truncate(u64 size) {
    check_open();
    seek(size);

    if (!SetEndOfFile(m_handle)) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to truncate `{}`: {}", name(), err.message())));
    }
}

void win32_file::sync() {
    check_open();

    if (!FlushFileBuffers(m_handle)) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to sync `{}`: {}", name(), err.message())));
    }
}

void win32_file::close() {
    if (m_handle != INVALID_HANDLE_VALUE) {
        auto fd = std::exchange(m_handle, INVALID_HANDLE_VALUE);
        if (!CloseHandle(fd)) {
            auto err = last_error();
            PREQUEL_THROW(io_error(fmt::format("Failed to close `{}`: {}", name(), err.message())));
        }
    }
}

void win32_file::check_open() {
    if (m_handle == INVALID_HANDLE_VALUE) {
        PREQUEL_THROW(io_error("File is closed."));
    }
}

HANDLE win32_vfs::create_file(const wchar_t* path, access_t access, int flags, int native_flags, win32_error& err) {
    err.clear();

    DWORD win_access = access == read_only ? GENERIC_READ : GENERIC_READ | GENERIC_WRITE;

    DWORD win_creation = 0;
    if (flags & open_create) {
        if (flags & open_exlusive)
            win_creation = CREATE_NEW;
        else
            win_creation = OPEN_ALWAYS;
    } else {
        win_creation = OPEN_EXISTING;
    }

    DWORD win_share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;

    DWORD win_flags = FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS;
    if (native_flags & delete_on_close)
        win_flags |= FILE_FLAG_DELETE_ON_CLOSE;

    // TODO: Open direct.

    HANDLE result = CreateFileW(path, win_access, win_share_mode, NULL, win_creation,
                                win_flags, NULL);
    if (result == INVALID_HANDLE_VALUE) {
        err = last_error();
        return INVALID_HANDLE_VALUE;
    }
    return result;
}

std::unique_ptr<file> win32_vfs::open(const char* path, access_t access, int flags) {
    PREQUEL_ASSERT(path != nullptr, "path null pointer");

    std::wstring utf16_path = to_utf16(path);
    win32_error err;

    HANDLE hnd = create_file(utf16_path.c_str(), access, flags, 0, err);
    if (err) {
        PREQUEL_THROW(io_error(fmt::format("Failed to open `{}`: {}", path, err.message())));
    }
    deferred guard = [&] { CloseHandle(hnd); };

    auto ret = std::make_unique<win32_file>(*this, hnd, path, access == read_only);
    guard.disable();
    return ret;
}

std::unique_ptr<file> win32_vfs::create_temp() {
    static constexpr int max_attempts = 5;

    wchar_t temp_path[MAX_PATH + 1];
    std::memset(temp_path, 0, sizeof(temp_path));

    if (GetTempFileNameW(L".", L"prequel-", 0, temp_path) == 0) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to create a temporary file: {}", err.message())));
    }

    win32_error err;
    HANDLE hnd = create_file(temp_path, read_write, open_normal, delete_on_close, err);
    if (err) {
        PREQUEL_THROW(io_error(fmt::format("Failed to open temporary file: {}", err.message())));
    }

    deferred guard = [&] { CloseHandle(hnd); };

    std::string utf8_path = to_utf8(temp_path);
    auto ret = std::make_unique<win32_file>(*this, hnd, utf8_path.c_str(), false);
    guard.disable();
    return ret;
}

void win32_vfs::remove(const char* path) {
    std::wstring utf16_path = to_utf16(path);

    if (DeleteFileW(utf16_path.c_str()) == 0) {
        auto err = last_error();
        PREQUEL_THROW(io_error(fmt::format("Failed to remove `{}`: {}", path, err.message())));
    }
}

vfs& system_vfs() {
    static win32_vfs vfs;
    return vfs;
}

} // namespace prequel
