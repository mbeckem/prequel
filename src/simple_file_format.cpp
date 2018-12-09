#include <prequel/simple_file_format.hpp>

#include <prequel/file_engine.hpp>
#include <prequel/mmap_engine.hpp>

#include <memory>

namespace prequel {

namespace detail {

class simple_file_format_impl {
private:
    static constexpr u32 user_data_offset = serialized_size<simple_file_format_header>();

    enum flags_t {
        // Set if the header was successfully initialized.
        FLAG_INITIALIZED = 1 << 0,

        // Set if the file was in write mode and has not been closed successfully.
        FLAG_DIRTY = 1 << 1,
    };

public:
    simple_file_format_impl(const magic_header& magic, u32 version, u32 block_size,
                            u32 user_data_size);
    ~simple_file_format_impl();

    simple_file_format_impl(const simple_file_format_impl&) = delete;
    simple_file_format_impl& operator=(const simple_file_format_impl&) = delete;

    const magic_header& magic() const { return m_magic; }
    u32 version() const { return m_version; }
    u32 block_size() const { return m_block_size; }
    u32 user_data_size() const { return m_user_data_size; }

    void cache_size(u64 cache_size_bytes);
    u64 cache_size() const { return m_cache_size_bytes; }

    void engine_type(simple_file_format_base::engine_type_t type);
    simple_file_format_base::engine_type_t engine_type() const { return m_engine_type; }

    void sync_enabled(bool enabled);
    bool sync_enabled() const { return m_sync_enabled; }

    bool is_open() const { return m_file != nullptr; }

    void open(const char* path, bool read_only, vfs& fs = system_vfs());

    void create(const char* path, const byte* user_data, vfs& fs = system_vfs());

    bool open_or_create(const char* path, const std::function<void(byte*)>& create_user_data,
                        vfs& fs = system_vfs());

    void flush();

    void close();

    bool read_only() const;

    engine& get_engine();

    default_allocator& get_allocator();

    const byte* get_user_data() const;
    void set_user_data(const byte* user_data);

private:
    /* Allocated on the heap, address is stable */
    struct open_file {
        // Owned handle to the raw file.
        std::unique_ptr<file> fd;

        // Selected engine implementation. Non-empty if initialization succeeded.
        std::unique_ptr<engine> en;

        // References the main block (index 0) and keeps it in memory.
        handle<simple_file_format_header> header_handle;

        // The in-memory content of the main block. This value is synced to `main_block`
        // on flush if it was changed.
        std::optional<simple_file_format_header> header_data;

        // True when the anchor was modified (see anchor_handle).
        anchor_flag header_changed;

        // Root allocator (optional for explicit lifetime control).
        std::optional<default_allocator> alloc;
    };

    void open_internal(std::unique_ptr<file> fd);
    void
    create_internal(std::unique_ptr<file> fd, const std::function<void(byte*)>& create_user_data);

    void sync_header(open_file& openfd) const {
        if (openfd.header_changed) {
            openfd.header_handle.set(*openfd.header_data);
            openfd.header_changed.reset();
        }
    }

    u8 get_flags(open_file& openfd) const { return openfd.header_data->flags; }

    void set_flags(open_file& openfd, u8 flags) const {
        PREQUEL_ASSERT(!openfd.fd->read_only(), "Not called for read-only files.");
        openfd.header_data->flags = flags;
        openfd.header_changed();
    }

    void flush_main_block(open_file& openfd) const {
        sync_header(openfd);
        openfd.header_handle.block().flush();
    }

    void check_header(const simple_file_format_header& a) const;

    std::unique_ptr<engine> create_engine(file& fd) const;

    anchor_handle<simple_file_format_header> get_anchor_handle(open_file& openfd) const {
        return anchor_handle<simple_file_format_header>(*openfd.header_data, openfd.header_changed);
    }

    void check_not_open() const {
        if (is_open()) {
            PREQUEL_THROW(bad_operation("This operation requires no file to be open."));
        }
    }

    void check_open() const {
        if (!is_open()) {
            PREQUEL_THROW(bad_operation("This operation requires an open file."));
        }
    }

private:
    magic_header m_magic;
    u32 m_version = 0;
    u32 m_block_size = 0;
    u32 m_user_data_size = 0;
    bool m_sync_enabled = true;

    u64 m_cache_size_bytes = simple_file_format_base::default_cache_bytes;
    simple_file_format_base::engine_type_t m_engine_type = simple_file_format_base::default_engine;
    std::unique_ptr<open_file> m_file;
};

simple_file_format_impl::simple_file_format_impl(const magic_header& magic, u32 version,
                                                 u32 block_size, u32 user_data_size)
    : m_magic(magic)
    , m_version(version)
    , m_block_size(block_size)
    , m_user_data_size(user_data_size) {
    // Improvement: it's not strictly required that the start of the file fits into a single block.
    const u32 required_block_size =
        checked_add<u32>(serialized_size<simple_file_format_header>(), m_user_data_size);
    if (m_block_size < required_block_size) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Insufficient block size: Header requires {} bytes but block size is {} "
                        "bytes. Increase block size or reduce user data size.",
                        required_block_size, m_block_size)));
    }
}

simple_file_format_impl::~simple_file_format_impl() {
    try {
        close();
    } catch (...) {
    }
}

void simple_file_format_impl::cache_size(u64 cache_size_bytes) {
    check_not_open();
    m_cache_size_bytes = cache_size_bytes;
}

void simple_file_format_impl::engine_type(simple_file_format_base::engine_type_t type) {
    check_not_open();
    m_engine_type = type;
}

void simple_file_format_impl::sync_enabled(bool enabled) {
    check_not_open();
    m_sync_enabled = enabled;
}

void simple_file_format_impl::open(const char* path, bool read_only, vfs& fs) {
    check_not_open();
    auto fd = fs.open(path, read_only ? vfs::read_only : vfs::read_write);
    return open_internal(std::move(fd));
}

void simple_file_format_impl::create(const char* path, const byte* user_data, vfs& fs) {
    if (is_open()) {
        PREQUEL_THROW(bad_operation("Another file has already been opened, close it first."));
    }

    auto fd = fs.open(path, vfs::read_write, vfs::open_create | vfs::open_exlusive);
    return create_internal(
        std::move(fd), [&](byte* data) { return std::memmove(data, user_data, m_user_data_size); });
}

bool simple_file_format_impl::open_or_create(const char* path,
                                             const std::function<void(byte*)>& create_user_data,
                                             vfs& fs) {
    if (is_open()) {
        PREQUEL_THROW(bad_operation("Another file has already been opened, close it first."));
    }

    auto fd = fs.open(path, vfs::read_write, vfs::open_create);
    if (fd->file_size() == 0) {
        // TODO: A more reliable check to determine whether the file was just created or already existed,
        // preferably without the race condition that would be introduced by fstat() followed by open().
        create_internal(std::move(fd), create_user_data);
        return false;
    } else {
        open_internal(std::move(fd));
        return true;
    }
}

void simple_file_format_impl::open_internal(std::unique_ptr<file> fd) {
    std::unique_ptr<open_file> openfd = std::make_unique<open_file>();

    openfd->fd = std::move(fd);
    if (u64 file_size = openfd->fd->file_size(); file_size % m_block_size != 0) {
        PREQUEL_THROW(corruption_error(
            fmt::format("The file size ({} bytes) is not a multiple of the block size ({} bytes).",
                        file_size, m_block_size)));
    }

    // Attempt to initialize the engine and check the content of the main block.
    openfd->en = create_engine(*openfd->fd);
    openfd->header_handle.reset(openfd->en->read(block_index(0)), 0);
    openfd->header_data = openfd->header_handle.get();
    check_header(*openfd->header_data);

    // Initialize the default allocator.
    anchor_handle<simple_file_format_header> anchor_handle = get_anchor_handle(*openfd);
    openfd->alloc.emplace(anchor_handle.template member<&simple_file_format_header::alloc>(),
                          *openfd->en);

    // Mark the file as dirty; this way we can detect program crashes
    // where the file was not closed properly.
    if (!openfd->fd->read_only()) {
        set_flags(*openfd, get_flags(*openfd) | FLAG_DIRTY);
        flush_main_block(*openfd);

        if (m_sync_enabled)
            openfd->fd->sync();
    }

    // Success.
    m_file = std::move(openfd);
}

void simple_file_format_impl::create_internal(std::unique_ptr<file> fd,
                                              const std::function<void(byte*)>& create_user_data) {
    PREQUEL_ASSERT(m_block_size >= checked_add<size_t>(serialized_size<simple_file_format_header>(),
                                                       m_user_data_size),
                   "Header plus userdata cannot fit into the block.");

    if (fd->read_only())
        PREQUEL_THROW(bad_argument("File has been opened as read-only."));

    std::unique_ptr<open_file> openfd = std::make_unique<open_file>();

    openfd->fd = std::move(fd);
    if (openfd->fd->file_size() != 0) {
        PREQUEL_THROW(corruption_error("The file is not empty."));
    }

    // Initialize the file size and the master block.
    openfd->en = create_engine(*openfd->fd);
    openfd->en->grow(1);

    // Zero the first block, then create the header followed by the userdata at offset 0.
    openfd->header_handle.reset(openfd->en->overwrite_zero(block_index(0)), 0);
    openfd->header_data =
        simple_file_format_header(m_magic, m_version, m_block_size, m_user_data_size);
    create_user_data(openfd->header_handle.block().writable_data() + user_data_offset);
    openfd->header_data->flags |= FLAG_INITIALIZED;
    openfd->header_handle.set(*openfd->header_data);

    // Sanity check. The fresh anchor must pass the same checks as an opened file.
    check_header(openfd->header_handle.get());

    // Open the allocator.
    anchor_handle<simple_file_format_header> anchor_handle = get_anchor_handle(*openfd);
    openfd->alloc.emplace(anchor_handle.template member<&simple_file_format_header::alloc>(),
                          *openfd->en);

    // Mark the file as dirty.
    openfd->en->flush();
    set_flags(*openfd, get_flags(*openfd) | FLAG_DIRTY);
    flush_main_block(*openfd);
    if (m_sync_enabled)
        openfd->fd->sync();

    // Success.
    m_file = std::move(openfd);
}

void simple_file_format_impl::flush() {
    check_open();
    if (!m_file->fd->read_only()) {
        sync_header(*m_file);
        m_file->en->flush();

        if (m_sync_enabled)
            m_file->fd->sync();
    }
}

void simple_file_format_impl::close() {
    if (!is_open())
        return;

    // Close the file only when everything else has been destroyed.
    if (!m_file->fd->read_only()) {
        flush();
        if (auto flags = get_flags(*m_file); flags & FLAG_DIRTY) {
            set_flags(*m_file, flags & ~FLAG_DIRTY);
            flush_main_block(*m_file);
        }
    }

    // Destroy the m_file struct first (without closing the fd)
    // and then sync one last time. Then, finally, close the file.
    std::unique_ptr<file> fd = std::move(m_file->fd);
    m_file.reset();
    if (!fd->read_only() && m_sync_enabled)
        fd->sync();
    fd->close();
}

bool simple_file_format_impl::read_only() const {
    check_open();
    return m_file->fd->read_only();
}

engine& simple_file_format_impl::get_engine() {
    check_open();
    return *m_file->en;
}

default_allocator& simple_file_format_impl::get_allocator() {
    check_open();
    return *m_file->alloc;
}

const byte* simple_file_format_impl::get_user_data() const {
    check_open();
    return m_file->header_handle.block().data() + user_data_offset;
}

void simple_file_format_impl::set_user_data(const byte* user_data) {
    check_open();
    byte* data = m_file->header_handle.block().writable_data() + user_data_offset;
    std::memmove(data, user_data, m_user_data_size);
}

inline void simple_file_format_impl::check_header(const simple_file_format_header& a) const {
    if (!std::equal(m_magic.begin(), m_magic.end(), a.magic.begin()))
        PREQUEL_THROW(
            corruption_error("Invalid input file, magic bytes do not match the file format."));

    if (a.version != m_version)
        PREQUEL_THROW(corruption_error(fmt::format(
            "Invalid version: expected {} but file specifies {}.", m_version, a.version)));

    if (a.block_size != m_block_size)
        PREQUEL_THROW(corruption_error(fmt::format(
            "Invalid block size: expected {} but file specifies {}.", m_block_size, a.block_size)));

    if (a.user_data_size != m_user_data_size)
        PREQUEL_THROW(corruption_error(
            fmt::format("Invalid user data size: expected {} but file specifies {}.",
                        m_user_data_size, a.user_data_size)));

    if (!(a.flags & FLAG_INITIALIZED))
        PREQUEL_THROW(corruption_error("The file was not initialized properly."));

    // TODO: Add an option to ignore this error.
    if (a.flags & FLAG_DIRTY)
        PREQUEL_THROW(corruption_error("The file was not closed properly and might be corrupted."));
}

std::unique_ptr<engine> simple_file_format_impl::create_engine(file& fd) const {
    switch (m_engine_type) {
    case simple_file_format_base::file_engine: {
        u64 cache_size = m_cache_size_bytes / m_block_size;
        if (cache_size > std::numeric_limits<size_t>::max())
            cache_size = std::numeric_limits<size_t>::max();
        return std::make_unique<prequel::file_engine>(fd, m_block_size,
                                                      static_cast<size_t>(cache_size));
    }
    case simple_file_format_base::mmap_engine: {
        return std::make_unique<prequel::mmap_engine>(fd, m_block_size);
    }
    }
    PREQUEL_UNREACHABLE("Case not handled by switch statement.");
}

/*
 * Raw simple file format implementation
 */

raw_simple_file_format::raw_simple_file_format(const magic_header& magic, u32 version,
                                               u32 block_size, u32 user_data_size)
    : m_impl(std::make_unique<simple_file_format_impl>(magic, version, block_size, user_data_size)) {
}

raw_simple_file_format::raw_simple_file_format(raw_simple_file_format&& other) noexcept
    : m_impl(std::move(other.m_impl)) {}

raw_simple_file_format& raw_simple_file_format::operator=(raw_simple_file_format&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

raw_simple_file_format::~raw_simple_file_format() {}

const magic_header& raw_simple_file_format::magic() const {
    return impl().magic();
}

u32 raw_simple_file_format::version() const {
    return impl().version();
}

u32 raw_simple_file_format::block_size() const {
    return impl().block_size();
}

u32 raw_simple_file_format::user_data_size() const {
    return impl().user_data_size();
}

void raw_simple_file_format::cache_size(u64 cache_size_bytes) {
    impl().cache_size(cache_size_bytes);
}
u64 raw_simple_file_format::cache_size() const {
    return impl().cache_size();
}

void raw_simple_file_format::engine_type(simple_file_format_base::engine_type_t type) {
    impl().engine_type(type);
}
simple_file_format_base::engine_type_t raw_simple_file_format::engine_type() const {
    return impl().engine_type();
}

void raw_simple_file_format::sync_enabled(bool enabled) {
    impl().sync_enabled(enabled);
}
bool raw_simple_file_format::sync_enabled() const {
    return impl().sync_enabled();
}

bool raw_simple_file_format::is_open() const {
    return impl().is_open();
}

void raw_simple_file_format::open(const char* path, bool read_only, vfs& fs) {
    impl().open(path, read_only, fs);
}

void raw_simple_file_format::create(const char* path, const byte* user_data, vfs& fs) {
    impl().create(path, user_data, fs);
}

bool raw_simple_file_format::open_or_create(const char* path,
                                            const std::function<void(byte*)>& create_user_data,
                                            vfs& fs) {
    return impl().open_or_create(path, create_user_data, fs);
}

void raw_simple_file_format::flush() {
    impl().flush();
}

void raw_simple_file_format::close() {
    impl().close();
}

bool raw_simple_file_format::read_only() const {
    return impl().read_only();
}

engine& raw_simple_file_format::get_engine() {
    return impl().get_engine();
}

default_allocator& raw_simple_file_format::get_allocator() {
    return impl().get_allocator();
}

const byte* raw_simple_file_format::get_user_data() const {
    return impl().get_user_data();
}

void raw_simple_file_format::set_user_data(const byte* user_data) {
    impl().set_user_data(user_data);
}

simple_file_format_impl& raw_simple_file_format::impl() const {
    if (!m_impl) {
        PREQUEL_THROW(bad_operation("Invalid default file format instance."));
    }
    return *m_impl;
}

} // namespace detail

} // namespace prequel
