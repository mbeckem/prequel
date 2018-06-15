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
        return std::make_unique<file_engine>(f, block_size, 4);
//    }
}

class test_file : boost::noncopyable {
public:
    explicit test_file(u32 block_size)
        : m_file(get_test_file())
        , m_block_size(block_size)
    {}

    void open() {
        if (m_engine)
            throw std::logic_error("already open");

        m_engine = get_test_engine(*m_file, m_block_size);
    }

    void close() {
        if (m_engine) {
            m_engine->flush();
            m_engine.reset();
        }
    }

    engine& get_engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

private:
    std::unique_ptr<file> m_file;
    std::unique_ptr<engine> m_engine;
    u32 m_block_size = 0;
};

} // namespace extpp

#endif // TEST_FILE_HPP
