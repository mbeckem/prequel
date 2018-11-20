#include "./database.hpp"

#include <prequel/default_file_format.hpp>
#include <prequel/vfs.hpp>

#include <fmt/format.h>

namespace keyvaluedb {

struct database_header {
    database::anchor db;

    static constexpr auto get_binary_format() {
        return prequel::make_binary_format(&database_header::db);
    }
};

struct settings {
    // Print stats on exit?
    bool print_stats = true;

    // Number of cached blocks in memory.
    uint32_t cache_blocks = 1024;
};

}

int main() {
    using namespace keyvaluedb;

    settings s;

    auto file = prequel::memory_vfs().open("tempfile", prequel::vfs::read_write, prequel::vfs::open_create);
    prequel::default_file_format<database_header> format(*file, 4096, s.cache_blocks);

    {
        database db(format.get_user_data().member<&database_header::db>(), format.get_allocator());

        for (int i = 1; i <= 1024; ++i) {
            std::string key = fmt::format("hello {}", i);
            std::string value = fmt::format("world {}", i);
            bool inserted = db.insert(key, value);
            if (!inserted)
                throw std::logic_error("Insertion failed.");
        }

        db.dump(std::cout);
    }
    format.flush();

    if (s.print_stats) {
        prequel::file_engine_stats stats;
        if (prequel::file_engine* fe = dynamic_cast<prequel::file_engine*>(&format.get_engine()))
            stats = fe->stats();

        std::cout << "\n"
                  << "I/O statistics:\n"
                  << "  Reads:      " << stats.reads << "\n"
                  << "  Writes:     " << stats.writes << "\n"
                  << "  Cache hits: " << stats.cache_hits << "\n"
                  << std::flush;
    }
}
