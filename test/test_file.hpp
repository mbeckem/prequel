#ifndef TEST_FILE_HPP
#define TEST_FILE_HPP

#include <extpp/default_allocator.hpp>
#include <extpp/file_engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/io.hpp>
#include <extpp/mmap_engine.hpp>

#include <extpp/detail/rollback.hpp>

#include <boost/optional.hpp>
#include <boost/noncopyable.hpp>

namespace extpp {

inline std::unique_ptr<file> get_test_file() {
    static constexpr bool fs_test = false;

    if constexpr (fs_test) {
        return system_vfs().create_temp();
    } else {
        return memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    }
}

template<u32 BlockSize>
inline std::unique_ptr<engine<BlockSize>> get_test_engine(file& f) {
    static constexpr bool mmap_test = false;

    if constexpr (mmap_test) {
        return std::make_unique<mmap_engine<BlockSize>>(f);
    } else {
        return std::make_unique<file_engine<BlockSize>>(f, 16);
    }
}

template<typename Anchor, u32 BlockSize>
class test_file : boost::noncopyable {
private:
    struct block_proto {
        typename default_allocator<BlockSize>::anchor alloc_anchor;
        Anchor anchor;
    };

    struct block : make_block_t<block_proto, BlockSize> {};

    struct state {
        handle<Anchor> anchor;
        default_allocator<BlockSize> alloc;

        state(handle<block> hnd, engine<BlockSize>& eng)
            : anchor(hnd.member(&block::anchor))
            , alloc(hnd.member(&block::alloc_anchor), eng)
        {}
    };

public:
    using allocator_type = default_allocator<BlockSize>;

public:
    test_file()
        : m_file(get_test_file())
        , m_block_size(BlockSize)
    {
        init();
    }

    void open() {
        if (m_engine)
            throw std::logic_error("already open");

        detail::rollback rb = [&]{
            m_state.reset();
            m_engine.reset();
        };

        m_engine = get_test_engine<BlockSize>(*m_file);
        {
            auto first_block = cast<block>(m_engine->read(block_index(0)));
            m_state.emplace(first_block, *m_engine);
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

    engine<BlockSize>& get_engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

    const handle<Anchor>& get_anchor() const {
        if (!m_engine)
            throw std::logic_error("not open");
        return m_state->anchor;
    }

    default_allocator<BlockSize>& get_allocator() {
        if (!m_engine)
            throw std::logic_error("not open");
        return m_state->alloc;
    }

private:
    void init() {
        if (m_file->file_size() == 0) {
            m_file->truncate(m_block_size);
            extpp::file_engine<BlockSize> be(*m_file, 1);
            construct<block>(be.read(block_index(0)));
            be.flush();
        }
    }

private:
    std::unique_ptr<file> m_file;
    std::unique_ptr<engine<BlockSize>> m_engine;
    u32 m_block_size = 0;
    boost::optional<state> m_state;
};

} // namespace extpp

#endif // TEST_FILE_HPP
