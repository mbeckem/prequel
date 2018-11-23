#include <prequel/mmap_engine.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

#include <vector>

namespace prequel {

namespace detail {

namespace {

// Select a smaller size for 32 bit architectures.
constexpr u64 query_mmap_chunk_size() {
    if constexpr (sizeof(void*) == 4) {
        return u64(128) << 20;
    } else if constexpr (sizeof(void*) == 8) {
        return u64(1) << 30;
    } else {
        throw "Unsupported architecture.";
    }
}

constexpr u64 mmap_chunk_size = query_mmap_chunk_size();

} // namespace

/// This implementation relies on the fact that, on linux,
/// mapping beyond the end of the file is fine.
/// For reference: https://marc.info/?t=112482693600001
///
/// We map large chunks of size mmap_chunk_size into memory at once,
/// which gives us the ability to dynamically grow the file.
class mmap_engine_impl {
public:
    mmap_engine_impl(file& fd, u32 block_size);
    ~mmap_engine_impl();

    mmap_engine_impl(const mmap_engine_impl&) = delete;
    mmap_engine_impl& operator=(const mmap_engine_impl&) = delete;

    file& fd() const { return *m_file; }

    u64 size() const { return m_file->file_size() / m_block_size; }

    void grow(u64 n);

    u32 block_size() const { return m_block_size; }

    byte* access_block(u64 block_index) const;

    void dirty(u64 block_index);

    void flush();

private:
    void update_mappings(u64 file_size);

private:
    /// The file handle.
    file* m_file = nullptr;

    /// True if the file was opened in read-only mode.
    bool m_read_only = false;

    /// Block size associated with the file.
    u32 m_block_size = 0;

    /// Mapped size of the file. Can be out of sync with the real size of the file on disk.
    u64 m_mapped_size = 0;

    /// Chunks of memory returned by mmap().
    /// All chunks are in order and of size mmap_chunk_size.
    /// We map the underlying file in chunks because this allows us to keep
    /// stable addresses (if we had only a single mapping, we might have to
    /// relocate it in virtual memory).
    std::vector<void*> m_mappings;
};

mmap_engine_impl::mmap_engine_impl(file& fd, u32 block_size)
    : m_file(&fd)
    , m_read_only(m_file->read_only())
    , m_block_size(block_size) {
    if (mmap_chunk_size % m_block_size != 0) {
        PREQUEL_THROW(bad_argument("mmap chunk size must be a multiple of the block size."));
    }

    update_mappings(m_file->file_size());
}

mmap_engine_impl::~mmap_engine_impl() {
    try {
        flush();
    } catch (...) {
    }

    try {
        vfs& v = m_file->get_vfs();
        for (void* mapping : m_mappings)
            v.memory_unmap(mapping, mmap_chunk_size);
    } catch (...) {
    }
}

void mmap_engine_impl::grow(u64 n) {
    if (PREQUEL_UNLIKELY(m_read_only))
        PREQUEL_THROW(
            io_error("The file cannot be resized because it was opened in read-only mode."));

    u64 new_size_blocks = checked_add(size(), n);
    u64 new_size_bytes = checked_mul<u64>(new_size_blocks, m_block_size);
    m_file->truncate(new_size_bytes);
    update_mappings(new_size_bytes);
}

void mmap_engine_impl::dirty(u64 block_index) {
    unused(block_index);

    if (PREQUEL_UNLIKELY(m_read_only))
        PREQUEL_THROW(
            io_error("The file cannot be written to because it was opened in read-only mode."));
}

void mmap_engine_impl::flush() {
    vfs& v = m_file->get_vfs();
    for (void* mapping : m_mappings) {
        v.memory_sync(mapping, mmap_chunk_size);
    }
}

byte* mmap_engine_impl::access_block(u64 block_index) const {
    u64 byte_offset = checked_mul<u64>(block_index, m_block_size);

    // Check against the mapped size. The mapped size can be out of sync with the real file
    // size but checking the file size every time is likely to be too slow.
    if (byte_offset >= m_mapped_size || (m_block_size > m_mapped_size - byte_offset)) {
        PREQUEL_THROW(io_error(
            fmt::format("Failed to access a block in `{}` at index {}, beyond the end of file.",
                        m_file->name(), block_index)));
    }

    u64 index_of_chunk = byte_offset / mmap_chunk_size;
    u64 offset_in_chunk = byte_offset % mmap_chunk_size;
    PREQUEL_ASSERT(index_of_chunk < m_mappings.size(), "Chunk index out of bounds.");
    return reinterpret_cast<byte*>(m_mappings[index_of_chunk]) + offset_in_chunk;
}

void mmap_engine_impl::update_mappings(u64 file_size) {
    vfs& v = m_file->get_vfs();

    size_t required_mappings = ceil_div(file_size, mmap_chunk_size);
    if (required_mappings < m_mappings.size()) {
        while (m_mappings.size() > required_mappings) {
            void* addr = m_mappings.back();
            v.memory_unmap(addr, mmap_chunk_size);
            m_mappings.pop_back();
        }
    } else if (required_mappings > m_mappings.size()) {
        m_mappings.reserve(required_mappings);
        while (m_mappings.size() < required_mappings) {
            void* addr = v.memory_map(*m_file, m_mappings.size() * mmap_chunk_size, mmap_chunk_size);
            m_mappings.push_back(addr);
        }
    }

    m_mapped_size = file_size;
}

} // namespace detail

mmap_engine::mmap_engine(file& fd, u32 block_size)
    : engine(block_size)
    , m_impl(std::make_unique<detail::mmap_engine_impl>(fd, block_size)) {}

mmap_engine::~mmap_engine() {}

file& mmap_engine::fd() const {
    return impl().fd();
}
u64 mmap_engine::do_size() const {
    return impl().size();
}
void mmap_engine::do_grow(u64 n) {
    impl().grow(n);
}
void mmap_engine::do_flush() {
    impl().flush();
}

engine::pin_result mmap_engine::do_pin(block_index index, bool initialize) {
    unused(initialize);

    pin_result result;
    result.data = impl().access_block(index.value());
    result.cookie = 0; /* unused */
    return result;
}

void mmap_engine::do_unpin(block_index index, uintptr_t cookie) noexcept {
    unused(index, cookie);
}

void mmap_engine::do_flush(block_index index, uintptr_t cookie) {
    // TODO: Implement msync for individual blocks?
    unused(index, cookie);
}
void mmap_engine::do_dirty(block_index index, uintptr_t cookie) {
    impl().dirty(index.value());
    unused(cookie);
}

detail::mmap_engine_impl& mmap_engine::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid engine instance.");
    return *m_impl;
}

} // namespace prequel
