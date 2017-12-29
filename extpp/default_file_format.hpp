#ifndef EXTPP_DEFAULT_FILE_FORMAT_HPP
#define EXTPP_DEFAULT_FILE_FORMAT_HPP

#include <extpp/block.hpp>
#include <extpp/io.hpp>
#include <extpp/file_engine.hpp>
#include <extpp/default_allocator.hpp>

#include <optional>

namespace extpp {

template<typename UserData, u32 BlockSize>
class default_file_format {
public:
    explicit default_file_format(file& f)
        : m_file(f)
        , m_engine(m_file)
    {
        init();
    }

    default_file_format(file& f, u32 cache_size)
        : m_file(f)
        , m_engine(m_file, cache_size)
    {
        init();
    }

    file_engine<BlockSize>& get_engine() {
        return m_engine;
    }

    default_allocator<BlockSize>& get_allocator() {
        EXTPP_ASSERT(m_allocator, "Invalid state.");
        return *m_allocator;
    }

    handle<UserData> user_data() {
        return m_handle.member(&block::user);
    }

    void flush() {
        m_engine.flush();
        m_file.sync();
    }

private:
    struct block_data {
        UserData user;
        typename default_allocator<BlockSize>::anchor alloc;
    };

    struct block : make_block_t<block_data, BlockSize> {};

private:
    void init() {
        // TODO: Verify file format, check magic bytes...
        if (m_file.file_size() == 0) {
            m_file.truncate(BlockSize);
            m_handle = construct<block>(m_engine, raw_address::block_address(block_index(0), BlockSize));
            m_engine.flush();
        } else {
            m_handle = access(m_engine, raw_address_cast<block>(raw_address::block_address(block_index(0), BlockSize)));
        }

        m_allocator.emplace(m_handle.member(&block::alloc), m_engine);
    }

private:
    file& m_file;
    file_engine<BlockSize> m_engine;
    handle<block> m_handle;
    std::optional<default_allocator<BlockSize>> m_allocator;
};

} // namespace extpp

#endif // EXTPP_DEFAULT_FILE_FORMAT_HPP
