#include <extpp/io.hpp>

#include <extpp/assert.hpp>
#include <extpp/exception.hpp>

#include <limits>
#include <memory>
#include <vector>

namespace extpp {

file::~file() {

}

vfs::~vfs() {

}

void* vfs::memory_map(file& f, u64 offset, u64 length) {
    unused(offset, length);

    check_vfs(f);
    EXTPP_THROW(unsupported("mmap is not supported by this vfs."));
}

void vfs::memory_sync(void* addr, u64 length) {
    unused(addr, length);
    EXTPP_THROW(unsupported("mmap is not supported by this vfs."));
}

void vfs::memory_unmap(void* addr, u64 length) {
    unused(addr, length);
    EXTPP_THROW(unsupported("mmap is not supported by this vfs."));
}

class in_memory_vfs : public vfs {
public:
    in_memory_vfs() = default;

    virtual const char* name() const noexcept override { return "memory"; }

    virtual std::unique_ptr<file> open(const char* path,
                                       access_t access = read_only,
                                       flags_t mode = open_normal) override;
};

class memory_file : public file {
    std::string m_name;
    std::vector<byte> m_data;

public:
    memory_file(in_memory_vfs& v, std::string name)
        : file(v)
        , m_name(std::move(name)) {}

    const char* name() const noexcept override { return m_name.c_str(); }

    void read(u64 offset, void* buffer, u32 count) override;
    void write(u64 offset, const void* buffer, u32 count) override;
    u64 file_size() override;
    void truncate(u64 size) override;
    void sync() override {}
    void close() override;

private:
    void range_check(u64 offset, u32 count) const;
};

void memory_file::range_check(u64 offset, u32 count) const
{
    if (offset < m_data.size()) {
        if (count <= m_data.size() - offset)
            return;
    }
    EXTPP_THROW(io_error(fmt::format("File range is out of bounds ({}, {})", offset, count)));
}

void memory_file::read(u64 offset, void* buffer, u32 count)
{
    EXTPP_ASSERT(buffer != nullptr, "Buffer null pointer");
    EXTPP_ASSERT(count > 0, "Zero sized read");
    range_check(offset, count);

    auto begin = m_data.data() + offset;
    auto end = begin + count;
    std::copy(begin, end, reinterpret_cast<byte*>(buffer));
}

void memory_file::write(u64 offset, const void* buffer, u32 count)
{
    EXTPP_ASSERT(buffer != nullptr, "Buffer null pointer");
    EXTPP_ASSERT(count > 0, "Zero sized write");
    range_check(offset, count);

    auto begin = reinterpret_cast<const byte*>(buffer);
    auto end = begin + count;
    std::copy(begin, end, m_data.data() + offset);
}

u64 memory_file::file_size()
{
    return m_data.size();
}

void memory_file::truncate(u64 size)
{
    if (size > std::numeric_limits<size_t>::max())
        EXTPP_THROW(io_error(fmt::format("File size too large ({} B)", size)));
    m_data.resize(size);
}

void memory_file::close()
{
    m_data.clear();
}

std::unique_ptr<file> in_memory_vfs::open(const char* path,
                                          access_t access,
                                          flags_t mode)
{
    // TODO: Respect flags
    unused(access, mode);
    return std::make_unique<memory_file>(*this, path);
}

vfs& memory_vfs() {
    static in_memory_vfs v;
    return v;
}

} // namespace extpp
