#ifndef EXTPP_DEFAULT_FILE_FORMAT_HPP
#define EXTPP_DEFAULT_FILE_FORMAT_HPP

#include <extpp/block.hpp>
#include <extpp/io.hpp>
#include <extpp/engine.hpp>
#include <extpp/default_allocator.hpp>

#include <optional>

namespace extpp {

template<typename UserData, u32 BlockSize>
class default_file_format {
public:
    explicit default_file_format(std::unique_ptr<file> f)
        : m_file(std::move(f))
        , m_engine(*m_file)
    {
        init();
    }

    default_file_format(std::unique_ptr<file> f, u32 cache_size)
        : m_file(std::move(f))
        , m_engine(*m_file, cache_size)
    {
        init();
    }

    extpp::engine<BlockSize>& engine() {
        return m_engine;
    }

    default_allocator<BlockSize>& allocator() {
        EXTPP_ASSERT(m_allocator, "Invalid state.");
        return *m_allocator;
    }

    handle<UserData, BlockSize> user_data() {
        return m_handle.member(&block::user);
    }

    void flush() {
        m_engine.flush();
        // TODO: m_file->flush();
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
        if (m_file->file_size() == 0) {
            m_file->truncate(BlockSize);
            m_handle = construct<block>(m_engine, raw_address<BlockSize>::from_block(0));
            m_engine.flush();
        } else {
            m_handle = access(m_engine, address_cast<block>(raw_address<BlockSize>::from_block(0)));
        }

        m_allocator.emplace(m_handle.member(&block::alloc), m_engine);
    }

private:
    std::unique_ptr<file> m_file;
    extpp::engine<BlockSize> m_engine;
    handle<block, BlockSize> m_handle;
    std::optional<extpp::default_allocator<BlockSize>> m_allocator;
};

} // namespace extpp

#endif // EXTPP_DEFAULT_FILE_FORMAT_HPP
