#include <prequel/vfs.hpp>

#include <prequel/assert.hpp>
#include <prequel/exception.hpp>

#include <limits>
#include <memory>
#include <vector>

namespace prequel {

file::~file() {}

vfs::~vfs() {}

void* vfs::memory_map(file& f, u64 offset, u64 length) {
    unused(offset, length);

    check_vfs(f);
    PREQUEL_THROW(unsupported("mmap is not supported by this vfs."));
}

void vfs::memory_sync(void* addr, u64 length) {
    unused(addr, length);
    PREQUEL_THROW(unsupported("mmap is not supported by this vfs."));
}

void vfs::memory_unmap(void* addr, u64 length) {
    unused(addr, length);
    PREQUEL_THROW(unsupported("mmap is not supported by this vfs."));
}

bool vfs::memory_in_core(void* addr, u64 length) {
    unused(addr, length);
    PREQUEL_THROW(unsupported("mmap is not supported by this vfs."));
}

class in_memory_vfs : public vfs {
public:
    in_memory_vfs() = default;

    virtual const char* name() const noexcept override { return "memory"; }

    virtual std::unique_ptr<file> open(const char* path, access_t access, int mode) override;

    virtual std::unique_ptr<file> create_temp() override;
};

class memory_file : public file {
public:
    memory_file(in_memory_vfs& v, std::string name, bool read_only)
        : file(v)
        , m_name(std::move(name))
        , m_read_only(read_only) {}

    bool read_only() const noexcept override { return m_read_only; }
    const char* name() const noexcept override { return m_name.c_str(); }

    void read(u64 offset, void* buffer, u32 count) override;
    void write(u64 offset, const void* buffer, u32 count) override;
    u64 file_size() override;
    void truncate(u64 size) override;
    void sync() override {}
    void close() override;

private:
    void range_check(u64 offset, u32 count) const;

private:
    std::string m_name;
    std::vector<byte> m_data;
    size_t size = 0;
    bool m_read_only = false; // Doesn't make much sense..
};

void memory_file::range_check(u64 offset, u32 count) const {
    if (offset < m_data.size()) {
        if (count <= m_data.size() - offset)
            return;
    }
    PREQUEL_THROW(io_error(fmt::format("File range is out of bounds ({}, {})", offset, count)));
}

void memory_file::read(u64 offset, void* buffer, u32 count) {
    PREQUEL_ASSERT(buffer != nullptr, "Buffer null pointer");
    PREQUEL_ASSERT(count > 0, "Zero sized read");
    range_check(offset, count);

    auto begin = m_data.data() + offset;
    auto end = begin + count;
    std::copy(begin, end, reinterpret_cast<byte*>(buffer));
}

void memory_file::write(u64 offset, const void* buffer, u32 count) {
    PREQUEL_ASSERT(buffer != nullptr, "Buffer null pointer");
    PREQUEL_ASSERT(count > 0, "Zero sized write");
    if (m_read_only)
        PREQUEL_THROW(io_error("Writing to a file opened in read-only mode."));

    range_check(offset, count);
    auto begin = reinterpret_cast<const byte*>(buffer);
    auto end = begin + count;
    std::copy(begin, end, m_data.data() + offset);
}

u64 memory_file::file_size() {
    return m_data.size();
}

void memory_file::truncate(u64 sz) {
    if (sz > std::numeric_limits<size_t>::max())
        PREQUEL_THROW(io_error(fmt::format("File size too large ({} byte)", sz)));
    m_data.resize(sz);
}

void memory_file::close() {
    m_data.clear();
}

std::unique_ptr<file> in_memory_vfs::open(const char* path, access_t access, int mode) {
    unused(access, mode);
    return std::make_unique<memory_file>(*this, path, access == read_only);
}

std::unique_ptr<file> in_memory_vfs::create_temp() {
    return std::make_unique<memory_file>(*this, "unnamed-temporary", false);
}

vfs& memory_vfs() {
    static in_memory_vfs v;
    return v;
}

} // namespace prequel
