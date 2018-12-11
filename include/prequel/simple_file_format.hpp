#ifndef PREQUEL_SIMPLE_FILE_FORMAT_HPP
#define PREQUEL_SIMPLE_FILE_FORMAT_HPP

#include <prequel/container/default_allocator.hpp>
#include <prequel/defs.hpp>
#include <prequel/exception.hpp>
#include <prequel/file_engine.hpp>
#include <prequel/handle.hpp>
#include <prequel/mmap_engine.hpp>
#include <prequel/serialization.hpp>
#include <prequel/vfs.hpp>

#include <fmt/format.h>

#include <array>
#include <iostream>
#include <memory>

namespace prequel {

namespace detail {

class simple_file_format_impl;
}

/**
 * The first few bytes of a formatted file. Should serve as a reasonably unique
 * identifier for the file format.
 */
class magic_header {
public:
    static constexpr size_t max_size = 20;

public:
    magic_header() = default;

    /// Construct the magic header using a null terminated c-string.
    magic_header(const char* m) {
        if (!m) {
            PREQUEL_THROW(bad_argument("Invalid c-string."));
        }

        const size_t size = strlen(m);
        if (size > max_size) {
            PREQUEL_THROW(bad_argument(fmt::format(
                "String is too large (size is {}, but maximum size is {})", size, max_size)));
        }

        std::memcpy(m_magic.data(), m, size);
    }

    /// Construct the magic header using an std::array of appropriate size.
    template<size_t N>
    magic_header(const std::array<byte, N>& m) {
        static_assert(N <= max_size, "Array is too large.");
        std::memcpy(m_magic.data(), m.data(), N);
    }

    /// Construct the magic header using a c-style array of appropriate size.
    template<size_t N>
    magic_header(const byte (&m)[N]) {
        static_assert(N <= max_size, "Array is too large.");
        std::memcpy(m_magic.data(), m, N);
    }

    constexpr const byte* begin() const { return m_magic.data(); }
    constexpr const byte* end() const { return m_magic.data() + max_size; }

    constexpr const byte* data() const { return m_magic.data(); }
    constexpr size_t size() const { return max_size; }

    bool operator==(const magic_header& other) const {
        return std::equal(begin(), end(), other.begin());
    }

    bool operator!=(const magic_header& other) const { return !(*this == other); }

    static constexpr auto get_binary_format() { return binary_format(&magic_header::m_magic); }

private:
    std::array<byte, max_size> m_magic{};
};

class simple_file_format_base {
public:
    // TODO: These should be factories because they may need type dependent parameters.
    enum engine_type_t {
        /**
         * The file engine reads and writes using the standard operating system calls
         * and caches recently used blocks in memory. [ TODO: Direct I/O ]
         */
        file_engine,

        /**
         * The mmap engine uses the virtual memory facilities of the operating system (e.g. `mmap`
         * on unix) to access the file. This is wastly more efficient for short-lived processes,
         * because they can rely on the "outside" cache and can therefore skip populating their own cache.
         * Long running processes can profit from using the normal file engine instead (in particular
         * when direct I/O can be used).
         *
         * The mmap engine requires an address space that is large enough to support the size of the file.
         * The theoretical limit for 32-bit platforms is 4 GB, with the actual limit being probably much lower
         * depending on the machine. 64-bit platforms should be fine even with very large files.
         */
        mmap_engine,

        /**
         * The engine used by default.
         */
        default_engine = file_engine
    };

    /// Default number of blocks cached when using the normal file engine (64 megabyte).
    static constexpr u64 default_cache_bytes = u64(64) << 20;
};

namespace detail {

// The first block in the file, for meta data (version information, file state, ...).
// The format-dependend user data is stored right after this header.
struct simple_file_format_header {
    // Identifies the file format, set by the user at construction time.
    magic_header magic;

    // Version number. Set by the user at construction time.
    u32 version = 0;

    // Bitset of flags_t values (internal use).
    u8 flags = 0;

    // Block size when the file was created. Must stay consistent.
    u32 block_size = 0;

    // Byte size of the userdata after this header (if flags & INITIALIZED).
    u32 user_data_size = 0;

    // Bootstrap allocator for the user. Allocates storage by resizing the file.
    default_allocator::anchor alloc;

    simple_file_format_header(const magic_header& magic_, u32 version_, u32 block_size_,
                              u32 user_data_size_)
        : magic(magic_)
        , version(version_)
        , block_size(block_size_)
        , user_data_size(user_data_size_) {}

    simple_file_format_header(deserialization_tag) {}

    static constexpr auto get_binary_format() {
        return binary_format(
            &simple_file_format_header::magic, &simple_file_format_header::version,
            &simple_file_format_header::flags, &simple_file_format_header::block_size,
            &simple_file_format_header::user_data_size, &simple_file_format_header::alloc);
    }
};

// Type erased common implementation. Should not be used directly.
// TODO: Make it possible to read the first part of the master block (regardless of the underlying block size),
// to decide on the block size and whether the file format version is supported.
// TODO: Separate the concept of the format and the open file, i.e. format == open file factory? open() and create() would return open file objects.
class raw_simple_file_format {
public:
    raw_simple_file_format(const magic_header& magic, u32 version, u32 block_size,
                           u32 user_data_size);
    ~raw_simple_file_format();

    raw_simple_file_format(raw_simple_file_format&&) noexcept;
    raw_simple_file_format& operator=(raw_simple_file_format&&) noexcept;

    const magic_header& magic() const;
    u32 version() const;
    u32 block_size() const;
    u32 user_data_size() const;

    void cache_size(u64 cache_size_bytes);
    u64 cache_size() const;

    void engine_type(simple_file_format_base::engine_type_t type);
    simple_file_format_base::engine_type_t engine_type() const;

    void sync_enabled(bool enabled);
    bool sync_enabled() const;

    bool is_open() const;

    // TODO enum for access mode.
    void open(const char* path, bool read_only, vfs& fs = system_vfs());

    void create(const char* path, const byte* user_data, vfs& fs = system_vfs());

    bool open_or_create(const char* path, const std::function<void(byte*)>& create_user_data,
                        vfs& fs = system_vfs());

    void flush();

    void close();

public:
    /*
     * The operations in this section require the file to be open
     * and will fail with an exception of that is not the case.
     */

    bool read_only() const;

    engine& get_engine();

    default_allocator& get_allocator();

    const byte* get_user_data() const;

    void set_user_data(const byte* user_data);

private:
    friend simple_file_format_impl;

    simple_file_format_impl& impl() const;

private:
    std::unique_ptr<simple_file_format_impl> m_impl;
};

} // namespace detail

template<typename UserData>
class simple_file_format : public simple_file_format_base {
public:
    static constexpr u32 header_size =
        serialized_size<detail::simple_file_format_header>() + serialized_size<UserData>();

public:
    simple_file_format(const magic_header& magic, u32 version, u32 block_size)
        : m_raw(magic, version, block_size, user_data_size()) {}

    ~simple_file_format() = default;

    simple_file_format(simple_file_format&&) noexcept = default;
    simple_file_format& operator=(simple_file_format&&) noexcept = default;

    const magic_header& magic() const { return m_raw.magic(); }
    u32 version() const { return m_raw.version(); }
    u32 block_size() const { return m_raw.block_size(); }
    static constexpr u32 user_data_size() { return serialized_size<UserData>(); }

    void cache_size(u64 cache_size_bytes) { m_raw.cache_size(cache_size_bytes); }
    u64 cache_size() const { return m_raw.cache_size(); }

    void engine_type(engine_type_t type) { m_raw.engine_type(type); }
    engine_type_t engine_type() const { return m_raw.engine_type(); }

    void sync_enabled(bool enabled) { m_raw.sync_enabled(enabled); }
    bool sync_enabled() const { return m_raw.sync_enabled(); }

    bool is_open() const { return m_raw.is_open(); }

    void open(const char* path, bool read_only, vfs& fs = system_vfs()) {
        m_raw.open(path, read_only, fs);
    }

    void create(const char* path, const UserData& user_data, vfs& fs = system_vfs()) {
        auto buffer = serialize_to_buffer(user_data);
        m_raw.create(path, buffer.data(), fs);
    }

    bool open_or_create(const char* path, const std::function<UserData()>& create_user_data,
                        vfs& fs = system_vfs()) {
        auto create = [&](byte* user_data_buffer) {
            UserData user_data = create_user_data();
            serialize(user_data, user_data_buffer);
        };
        return m_raw.open_or_create(path, create, fs);
    }

    void flush() { m_raw.flush(); }

    void close() { m_raw.close(); }

public:
    bool read_only() const { return m_raw.read_only(); }

    engine& get_engine() { return m_raw.get_engine(); }

    default_allocator& get_allocator() { return m_raw.get_allocator(); }

    UserData get_user_data() const { return deserialize<UserData>(m_raw.get_user_data()); }

    void set_user_data(const UserData& user_data) {
        auto buffer = serialize_to_buffer(user_data);
        m_raw.set_user_data(buffer.data());
    }

private:
    detail::raw_simple_file_format m_raw;
};

} // namespace prequel

#endif // PREQUEL_SIMPLE_FILE_FORMAT_HPP
