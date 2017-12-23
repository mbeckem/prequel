#include <extpp/mmap_engine.hpp>

#include <extpp/exception.hpp>
#include <extpp/io.hpp>

#include <map>

namespace extpp {
namespace {

static constexpr u64 mmap_region_size = 1 << 30;

/// This implementation relies on the fact that, on linux,
/// mapping beyond the end of the file is fine.
/// For reference: https://marc.info/?t=112482693600001
struct mmap_backend {
    mmap_backend(file& f, u32 block_size)
        : m_file(&f)
        , m_block_size(block_size)
        , m_file_size(m_file->file_size())
    {
        EXTPP_CHECK(m_file_size % m_block_size == 0,
                    "File size must be a multiple of the block size.");
        EXTPP_CHECK(mmap_region_size % m_block_size == 0,
                    "Region size must be a multiple of the block size.");

        for (u64 offset = 0; offset < m_file_size; offset += mmap_region_size)
            add_region(offset);
    }

    /// Returns the address to the block's data.
    /// The block index must be within the bounds of the file.
    byte* access(u64 block) const {
        u64 byte_offset = checked_mul<u64>(block, m_block_size);
        if (byte_offset >= m_file_size || (m_block_size > m_file_size - byte_offset)) {
            EXTPP_THROW(
                io_error(fmt::format("Failed to access a block in `{}`: beyond the end of file.",
                                     m_file->name()))
            );
        }

        u64 offset_in_region = byte_offset % mmap_region_size;
        void* addr = region(byte_offset);
        return reinterpret_cast<byte*>(addr) + offset_in_region;
    }

    void grow(u64 blocks) {
        u64 additional = checked_mul(blocks, m_file_size);
        u64 new_file_size = checked_add(m_file_size, additional);
        truncate_bytes(new_file_size);
    }

private:
    /// Alters the size of the file and updates the memory mappings.
    void truncate_bytes(u64 new_file_size) {
        if (new_file_size == m_file_size) return;

        m_file->truncate(new_file_size);

        if (new_file_size > m_file_size) {
            for (u64 offset = ceil_div(m_file_size, mmap_region_size) * mmap_region_size;
                 offset < new_file_size;
                 offset += mmap_region_size)
            {
                add_region(offset);
            }
        } else {
            // Remove all regions with start offset >= new_file_size
            auto begin = m_maps.lower_bound(new_file_size);
            auto end = m_maps.end();
            for (auto i = begin; i != end; ++i) {
                m_file->get_vfs().memory_unmap(reinterpret_cast<void*>(i->second), mmap_region_size);
            }
            m_maps.erase(begin, end);
        }
        m_file_size = new_file_size;
    }

    /// Returns the region that contains the given byte.
    void* region(u64 byte_offset) const {
        auto pos = m_maps.upper_bound(byte_offset);
        EXTPP_ASSERT(pos != m_maps.begin(),
                     "Not mapped in memory.");
        --pos;
        EXTPP_ASSERT(pos->first <= byte_offset && pos->first + mmap_region_size > byte_offset,
                     "Byte belongs to this range.");
        return reinterpret_cast<void*>(pos->second);
    }

    /// Maps a new region, starting with the given byte offset.
    void add_region(u64 byte_offset) {
        u64 byte_length = mmap_region_size;

        void* addr = m_file->get_vfs().memory_map(*m_file, byte_offset, byte_length);
        m_maps.emplace(byte_offset, reinterpret_cast<uintptr_t>(addr));
    }

private:
    file* m_file;

    u32 m_block_size;

    u64 m_file_size;

    /// Tree of mapped memory regions.
    /// Key: byte offset on disk
    /// Value: start of mapped memory region.
    std::map<u64, uintptr_t> m_maps;

};

} // namespace
} // namespace extpp
