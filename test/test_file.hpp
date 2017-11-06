#ifndef TEST_FILE_HPP
#define TEST_FILE_HPP

#include <extpp/default_allocator.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/io.hpp>

#include <extpp/detail/rollback.hpp>

#include <boost/optional.hpp>
#include <boost/noncopyable.hpp>

namespace extpp {

template<typename Anchor, u32 BlockSize>
class test_file : boost::noncopyable {
private:
    struct block_proto {
        typename default_allocator<BlockSize>::anchor alloc_anchor;
        Anchor anchor;
    };

    struct block : make_block_t<block_proto, BlockSize> {};

    struct state {
        handle<Anchor, BlockSize> anchor;
        default_allocator<BlockSize> alloc;

        state(handle<block, BlockSize> hnd, extpp::engine<BlockSize>& eng, u32 chunk)
            : anchor(hnd.neighbor(&hnd->anchor))
            , alloc(hnd.neighbor(&hnd->alloc_anchor), eng, chunk)
        {}
    };

public:
    explicit test_file()
        : m_file(create_memory_file("test-file"))
        , m_block_size(BlockSize)
    {
        m_file->truncate(m_block_size);

        extpp::engine<BlockSize> be(*m_file, 1);
        construct<block>(be.read(0));
        be.flush();
    }

    void open() {
        if (m_engine)
            throw std::logic_error("already open");

        auto rb = detail::rollback([&]{
            m_state.reset();
            m_engine.reset();
        });

        m_engine.emplace(*m_file, 8);
        {
            auto first_block = cast<block>(m_engine->read(0));
            m_state.emplace(first_block, *m_engine, 16);
        }

        rb.commit();
    }

    void close() {
        if (m_engine) {
            m_state.reset();
            m_engine->flush();
            m_engine.reset();
        }
    }

    extpp::engine<BlockSize>& engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

    const handle<Anchor, BlockSize>& anchor() const {
        if (!m_engine)
            throw std::logic_error("not open");
        return m_state->anchor;
    }

    default_allocator<BlockSize>& alloc() {
        if (!m_engine)
            throw std::logic_error("not open");
        return m_state->alloc;
    }

private:
    std::unique_ptr<file> m_file;
    u32 m_block_size = 0;
    boost::optional<extpp::engine<BlockSize>> m_engine;
    boost::optional<state> m_state;
};

} // namespace extpp

#endif // TEST_FILE_HPP
