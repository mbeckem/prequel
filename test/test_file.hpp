#ifndef TEST_FILE_HPP
#define TEST_FILE_HPP

#include <prequel/file_engine.hpp>
#include <prequel/handle.hpp>
#include <prequel/mmap_engine.hpp>
#include <prequel/memory_engine.hpp>
#include <prequel/vfs.hpp>

#include <prequel/detail/deferred.hpp>

#include <boost/optional.hpp>
#include <boost/noncopyable.hpp>

namespace prequel {

class test_file : boost::noncopyable {
public:
    explicit test_file(u32 block_size)
    {
        // TODO test mmap engine
        constexpr bool fs_test = true;
        if (fs_test) {
            m_file = system_vfs().create_temp();
            m_engine = std::make_unique<file_engine>(*m_file, block_size, 64);
        } else {
            m_engine = std::make_unique<memory_engine>(block_size);
        }
    }

    engine& get_engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

private:
    std::unique_ptr<file> m_file; // Can be unused
    std::unique_ptr<engine> m_engine;
};

} // namespace prequel

#endif // TEST_FILE_HPP
