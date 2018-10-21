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

inline std::unique_ptr<file> get_test_file() {
    // TODO: Make these defines or runtime options.
    static constexpr bool fs_test = false;

    if constexpr (fs_test) {
        return system_vfs().create_temp();
    } else {
        return memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    }
}

inline std::unique_ptr<engine> get_test_engine(file& f, u32 block_size) {
    // TODO: Make these defines or runtime options.
    static constexpr bool mmap_test = false;

    if constexpr (mmap_test) {
        return std::make_unique<mmap_engine>(f, block_size);
    } else {
        return std::make_unique<file_engine>(f, block_size, 16 * 1024);
    }
}

// TODO: Configuration option for file engine or mmap engine.
class test_file : boost::noncopyable {
public:
    explicit test_file(u32 block_size)
        : m_engine(std::make_unique<memory_engine>(block_size))
    {}

    engine& get_engine() {
        if (!m_engine)
            throw std::logic_error("not open");
        return *m_engine;
    }

private:
    std::unique_ptr<engine> m_engine;
};

} // namespace prequel

#endif // TEST_FILE_HPP
