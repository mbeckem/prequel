#include <extpp/mmap_engine.hpp>

#include <extpp/exception.hpp>
#include <extpp/io.hpp>

#include <map>

namespace extpp {
namespace detail {

static constexpr u64 mmap_region_size = 1 << 30;

/// This implementation relies on the fact that, on linux,
/// mapping beyond the end of the file is fine.
/// For reference: https://marc.info/?t=112482693600001
///
/// We map large chunks of size 1 GB into memory at once,
/// which gives us the ability to dynamically grow the file.
mmap_backend::mmap_backend(file& f, u32 block_size)
    : m_file(&f)
    , m_block_size(block_size)
{
    EXTPP_CHECK(mmap_region_size % m_block_size == 0,
                "Region size must be a multiple of the block size.");
    update();
}

mmap_backend::~mmap_backend() {
    try {
        for (void* mapping : m_maps)
            m_file->get_vfs().memory_unmap(mapping, mmap_region_size);
    } catch (...) {}
}

/// Returns the address to the block's data.
/// The block index must be within the bounds of the file.
byte* mmap_backend::access(u64 block) const {
    u64 byte_offset = checked_mul<u64>(block, m_block_size);
    if (byte_offset >= m_mapped_size || (m_block_size > m_mapped_size - byte_offset)) {
        EXTPP_THROW(
            io_error(fmt::format("Failed to access a block in `{}`: beyond the end of file.",
                                 m_file->name()))
        );
    }

    u64 region_index = byte_offset / mmap_region_size;
    u64 offset_in_region = byte_offset % mmap_region_size;
    EXTPP_ASSERT(region_index < m_maps.size(), "Map index out of bounds.");
    return reinterpret_cast<byte*>(m_maps[region_index]) + offset_in_region;
}

void mmap_backend::update() {
    u64 file_size = m_file->file_size();

    size_t required_mappings = ceil_div(file_size, mmap_region_size);
    if (required_mappings < m_maps.size()) {
        while (m_maps.size() > required_mappings) {
            void* addr = m_maps.back();
            m_file->get_vfs().memory_unmap(addr, mmap_region_size);
            m_maps.pop_back();
        }
    } else if (required_mappings > m_maps.size()) {
        m_maps.reserve(required_mappings);
        while (m_maps.size() < required_mappings) {
            void* addr = m_file->get_vfs().memory_map(*m_file, m_maps.size() * mmap_region_size, mmap_region_size);
            m_maps.push_back(addr);
        }
    }

    m_mapped_size = file_size;
}

void mmap_backend::sync() {
    // TODO
    EXTPP_UNREACHABLE("TODO");
}

mmap_engine::mmap_engine(file& fd, u32 block_size)
    : m_file(&fd)
    , m_block_size(block_size)
    , m_backend(fd, block_size)
{
    m_pool.reserve(m_max_pool_size);
}

void mmap_engine::grow(u64 n) {
    u64 new_size_blocks = checked_add(size(), n);
    u64 new_size_bytes = checked_mul<u64>(new_size_blocks, m_block_size);
    m_file->truncate(new_size_bytes);
    m_backend.update();
}

std::unique_ptr<mmap_handle> mmap_engine::read(u64 index) {
    byte* block = m_backend.access(index);

    auto handle = allocate_handle();
    handle->reset(index, block);
    return handle;
}

std::unique_ptr<mmap_handle> mmap_engine::zeroed(u64 index) {
    auto handle = read(index);
    std::memset(handle->m_data, 0, m_block_size);
    return handle;
}

std::unique_ptr<mmap_handle> mmap_engine::overwritten(u64 index, const byte* data) {
    auto handle = read(index);
    std::memmove(handle->m_data, data, m_block_size);
    return handle;
}

std::unique_ptr<mmap_handle> mmap_engine::allocate_handle() {
    if (m_pool.empty())
        return std::unique_ptr<mmap_handle>(new mmap_handle(this));

    auto handle = std::move(m_pool.back());
    m_pool.pop_back();
    return handle;
}

void mmap_engine::free_handle(std::unique_ptr<mmap_handle> handle) {
    if (m_pool.size() < m_max_pool_size)
        m_pool.push_back(std::move(handle));
}

} // namespace detail
} // namespace extpp
