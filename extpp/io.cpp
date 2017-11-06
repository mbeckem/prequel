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

class memory_file : public file {
    std::string m_name;
    std::vector<byte> m_data;

public:
    memory_file(std::string name)
        : m_name(std::move(name)) {}

    const char* name() const noexcept override { return m_name.c_str(); }

    void read(u64 offset, void* buffer, u32 count) override;
    void write(u64 offset, const void* buffer, u32 count) override;
    u64 file_size() override;
    void truncate(u64 size) override;
    void close() override;

private:
    size_t truncate_offset(u64 offset) const;
    void range_check(u64 offset, u32 count) const;
};

size_t memory_file::truncate_offset(u64 offset) const
{
    using limits = std::numeric_limits<size_t>;
    if (offset > limits::max()) {
        // TODO: Own exception class
        throw std::invalid_argument("offset too large.");
    }
    return static_cast<size_t>(offset);
}

void memory_file::range_check(u64 offset, u32 count) const
{
    size_t begin = truncate_offset(offset);
    if (begin >= m_data.size()) {
        throw std::range_error("range out of bounds");
    }
    if (count > m_data.size() - begin) {
        throw std::range_error("range out of bounds");
    }
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
    m_data.resize(truncate_offset(size));
}

void memory_file::close()
{
    m_data.clear();
}

std::unique_ptr<file> create_memory_file(std::string name)
{
    return std::make_unique<memory_file>(std::move(name));
}

} // namespace extpp
