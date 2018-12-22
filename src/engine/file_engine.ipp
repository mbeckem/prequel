#ifndef PREQUEL_ENGINE_FILE_ENGINE_IPP
#define PREQUEL_ENGINE_FILE_ENGINE_IPP

#include "file_engine.hpp"

#include <prequel/math.hpp>

namespace prequel::detail::engine_impl {

file_engine::file_engine(file& fd, u32 block_size, size_t cache_blocks)
    : engine_base(block_size, cache_blocks, fd.read_only())
    , m_file(&fd) {}

file_engine::~file_engine() {
    // Make an attempt to flush pending IO.
    try {
        flush();
    } catch (...) {
    }
}

u64 file_engine::size() const {
    return m_file->file_size() >> m_block_size_log;
}

void file_engine::grow(u64 n) {
    u64 new_blocks = checked_add(size(), n);
    u64 new_bytes = checked_mul(new_blocks, u64(m_block_size));
    m_file->truncate(new_bytes);
}

void file_engine::do_read(u64 index, byte* buffer) {
    m_file->read(index << m_block_size_log, buffer, m_block_size);
}

void file_engine::do_write(u64 index, const byte* buffer) {
    m_file->write(index << m_block_size_log, buffer, m_block_size);
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_FILE_ENGINE_IPP
