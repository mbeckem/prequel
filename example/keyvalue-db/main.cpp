#include "./database.hpp"

#include <prequel/simple_file_format.hpp>
#include <prequel/vfs.hpp>

#include <fmt/format.h>

namespace keyvaluedb {

struct database_header {
    database_header() = default;

    database::anchor db;

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&database_header::db);
    }
};

struct settings {
    // Print stats on exit?
    bool print_stats = true;

    // Number of megabytes (approx) cached in memory.
    uint32_t cache_megabytes = 1;
};

} // namespace keyvaluedb

static const prequel::magic_header magic("example-keyvaluedb");
static const uint32_t version = 1;
static const uint32_t block_size = 4096;

int main() {
    using namespace keyvaluedb;

    settings s;

    prequel::simple_file_format<database_header> format(magic, version, block_size);
    format.cache_size(uint64_t(s.cache_megabytes) << 20);
    format.engine_type(format.file_engine);

    format.open_or_create("keyvalue.db", [&] { return database_header(); });

    {
        database_header header = format.get_user_data();
        prequel::anchor_flag header_changed;

        database db(prequel::make_anchor_handle(header.db, header_changed), format.get_allocator());

        for (int i = 1; i <= 1024; ++i) {
            std::string key = fmt::format("hello {}", i);
            std::string value = fmt::format("world {}", i);
            bool inserted = db.insert(key, value);
            if (!inserted)
                throw std::logic_error("Insertion failed.");
        }

        db.dump(std::cout);

        if (header_changed)
            format.set_user_data(header);
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
