#ifndef TEST_FILE_HPP
#define TEST_FILE_HPP

#include <extpp/file_engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/io.hpp>

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

inline std::unique_ptr<engine> get_test_engine(file& f, u32 block_size) {
    static constexpr bool mmap_test = false;

// TODO
//    if constexpr (mmap_test) {
//        return std::make_unique<mmap_engine>(f, block_size);
//    } else {
        return std::make_unique<file_engine>(f, block_size, 16);
//    }
}

template<typename Anchor>
class test_file : boost::noncopyable {
public:
    explicit test_file(u32 block_size)
        : m_file(get_test_file())
        , m_block_size(block_size)
    {
        init();
    }

    void open() {
        if (m_engine)
            throw std::logic_error("already open");

        detail::rollback rb = [&]{
            m_anchor = handle<Anchor>();
            m_engine.reset();
        };

        m_engine = get_test_engine(*m_file, m_block_size);
        m_anchor = handle<Anchor>(m_engine->read(block_index(0)), 0);
        rb.commit();
    }

    void close() {
        if (m_engine) {
            m_anchor = handle<Anchor>();
            m_engine->flush();
            m_engine.reset();
        }
    }

    engine& get_engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

    const handle<Anchor>& get_anchor() const {
        if (!m_engine)
            throw std::logic_error("not open");
        return m_anchor;
    }

private:
    void init() {
        if (m_file->file_size() == 0) {
            m_file->truncate(m_block_size);
            extpp::file_engine be(*m_file, m_block_size, 1);
            handle<Anchor> handle(be.zeroed(block_index(0)), 0);
            handle.set(Anchor());
            be.flush();
        }
    }

private:
    std::unique_ptr<file> m_file;
    std::unique_ptr<engine> m_engine;
    u32 m_block_size = 0;
    handle<Anchor> m_anchor;
};

} // namespace extpp

#endif // TEST_FILE_HPP
