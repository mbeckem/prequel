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

/**
 * TODO: Existing handles should be indexed and returned again. Currently we construct
 * a new handle on every read(), returning many identical copies.
 */
struct mmap_handle final : public detail::block_handle_impl {
public:
    /// The engine this handle belongs to.
    mmap_engine_impl* const m_engine;

    /// Number of references. Will be freed or put into the pool when this becomes 0.
    u32 m_refcount = 0;

    /// Index within the file.
    u64 m_index = 0;

    /// Points to mmapped data block.
    byte* m_data = nullptr;

    /// Used by the handle pool to link unused blocks.
    boost::intrusive::list_member_hook<> m_free_list_hook;

public:
    explicit mmap_handle(mmap_engine_impl* engine)
        : m_engine(engine)
    {}

    void clear() {
        PREQUEL_ASSERT(m_refcount == 0, "Instance must not be in use.");

        m_index = 0;
        m_data = nullptr;
    }

    void set(u64 index, byte* data) {
        PREQUEL_ASSERT(m_refcount == 0, "Instance must not be in use.");
        PREQUEL_ASSERT(index != block_index::invalid_value, "Invalid index.");
        PREQUEL_ASSERT(data, "Invalid address.");

        m_index = index;
        m_data = data;
    }

public:
    // detail::block_handle interface implementation

    void handle_ref() override {
        ++m_refcount;
    }

    void handle_unref() override;

    u64 index() const noexcept override { return m_index; }

    const byte* data() const noexcept override { return m_data; }

    // TODO: Read only mmap engine?
    byte* writable_data() override { return m_data; }
};

/**
 * Allocates and frees mmap_handles.
 * Up to max_size handles are cached for reuse.
 */
class handle_pool {
public:
    struct pool_deleter {
        handle_pool* pool;

        void operator()(mmap_handle* handle) const noexcept {
            pool->free(handle);
        }
    };

    using handle_ptr = std::unique_ptr<mmap_handle, pool_deleter>;

public:
    handle_pool(u32 max_size): m_max_size(max_size) {}
    ~handle_pool();

    handle_pool(const handle_pool&) = delete;
    handle_pool& operator=(const handle_pool&) = delete;

    handle_ptr allocate(mmap_engine_impl* engine);
    void free(mmap_handle* handle);

private:
    using list_t = boost::intrusive::list<
        mmap_handle,
        boost::intrusive::member_hook<
            mmap_handle,
            boost::intrusive::list_member_hook<>,
            &mmap_handle::m_free_list_hook
        >
    >;

    /// Max number of instances kept for reuse.
    u32 m_max_size;

    /// List of reuseable handle instances.
    list_t m_list;
};

using handle_ptr = handle_pool::handle_ptr;

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

    handle_ptr access(u64 index);
    handle_ptr read(u64 index);
    handle_ptr overwrite_zero(u64 index);
    handle_ptr overwrite(u64 index, const byte* data);

    void flush();

private:
    handle_ptr get_handle();

    byte* access_block(u64 block_index) const;
    void update_mappings(u64 file_size);

private:
    friend mmap_handle;

    /// The file handle.
    file* m_file = nullptr;

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

    /// Pool for reuseable handle instances.
    handle_pool m_pool;
};

void mmap_handle::handle_unref() {
    PREQUEL_ASSERT(m_refcount > 0, "Invalid refcount.");
    if (--m_refcount == 0) {
        // possibly "delete this":
        m_engine->m_pool.free(this);
    }
}

handle_pool::~handle_pool() {
    m_list.clear_and_dispose([](mmap_handle* h) {
        delete h;
    });
}

handle_ptr handle_pool::allocate(mmap_engine_impl* engine) {
    if (!m_list.empty()) {
        mmap_handle* handle = &m_list.front();
        m_list.pop_front();
        return handle_ptr(handle, pool_deleter{this});
    }

    return handle_ptr(new mmap_handle(engine), pool_deleter{this});
}

void handle_pool::free(mmap_handle* handle) {
    if (m_list.size() < m_max_size) {
        handle->clear();
        m_list.push_back(*handle);
    } else {
        delete handle;
    }
}

mmap_engine_impl::mmap_engine_impl(file& fd, u32 block_size)
    : m_file(&fd)
    , m_block_size(block_size)
    , m_pool(1024 /* max size */)
{
    if (mmap_chunk_size % m_block_size != 0) {
        PREQUEL_THROW(bad_argument("mmap chunk size must be a multiple of the block size."));
    }

    update_mappings(m_file->file_size());
}

mmap_engine_impl::~mmap_engine_impl() {
    try {
        flush();
    } catch (...) {}

    try {
        vfs& v = m_file->get_vfs();
        for (void* mapping : m_mappings)
            v.memory_unmap(mapping, mmap_chunk_size);
    } catch (...) {}
}

void mmap_engine_impl::grow(u64 n) {
    u64 new_size_blocks = checked_add(size(), n);
    u64 new_size_bytes = checked_mul<u64>(new_size_blocks, m_block_size);
    m_file->truncate(new_size_bytes);
    update_mappings(new_size_bytes);
}

void mmap_engine_impl::flush() {
    vfs& v = m_file->get_vfs();
    for (void* mapping : m_mappings) {
        v.memory_sync(mapping, mmap_chunk_size);
    }

    // Fsync is required for metadata changes (e.g. size of file changed).
    // memsync should have taken care of the data sync already.
    m_file->sync();
}

handle_ptr mmap_engine_impl::access(u64 index) {
    // access_block() returns the block data's address but does not dereference:
    byte* data = access_block(index);

    if (m_file->get_vfs().memory_in_core(data, m_block_size)) {
        handle_ptr handle = get_handle();
        handle->set(index, data);
        return handle;
    }
    return handle_ptr();
}

handle_ptr mmap_engine_impl::read(u64 index) {
    byte* data = access_block(index);

    handle_ptr handle = get_handle();
    handle->set(index, data);
    return handle;
}

handle_ptr mmap_engine_impl::overwrite_zero(u64 index) {
    handle_ptr handle = read(index);
    std::memset(handle->m_data, 0, m_block_size);
    return handle;
}

handle_ptr mmap_engine_impl::overwrite(u64 index, const byte* data) {
    handle_ptr handle = read(index);
    std::memmove(handle->m_data, data, m_block_size);
    return handle;
}

handle_ptr mmap_engine_impl::get_handle() {
    return m_pool.allocate(this);
}

byte* mmap_engine_impl::access_block(u64 block_index) const {
    u64 byte_offset = checked_mul<u64>(block_index, m_block_size);

    // Check against the mapped size. The mapped size can be out of sync with the real file
    // size but checking the file size every time is likely to be too slow.
    if (byte_offset >= m_mapped_size || (m_block_size > m_mapped_size - byte_offset)) {
        PREQUEL_THROW(io_error(
            fmt::format("Failed to access a block in `{}` at index {}, beyond the end of file.",
                        m_file->name(), block_index)
        ));
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
    , m_impl(std::make_unique<detail::mmap_engine_impl>(fd, block_size))
{}

mmap_engine::~mmap_engine() {}

file& mmap_engine::fd() const { return impl().fd(); }
u64 mmap_engine::do_size() const { return impl().size(); }
void mmap_engine::do_grow(u64 n) { impl().grow(n); }

block_handle mmap_engine::do_access(block_index index) {
    detail::handle_ptr handle = impl().access(index.value());
    return handle ? block_handle(this, handle.release()) : block_handle();
}

block_handle mmap_engine::do_read(block_index index) {
    detail::handle_ptr handle = impl().read(index.value());
    return block_handle(this, handle.release());
}

block_handle mmap_engine::do_overwrite_zero(block_index index) {
    detail::handle_ptr handle = impl().overwrite_zero(index.value());
    return block_handle(this, handle.release());
}

block_handle mmap_engine::do_overwrite(block_index index, const byte* data) {
    detail::handle_ptr handle = impl().overwrite(index.value(), data);
    return block_handle(this, handle.release());
}

void mmap_engine::do_flush() { impl().flush(); }

detail::mmap_engine_impl& mmap_engine::impl() const {
    PREQUEL_ASSERT(m_impl, "Invalid engine instance.");
    return *m_impl;
}

} // namespace prequel
