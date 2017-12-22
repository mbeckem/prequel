#include <extpp/btree.hpp>
#include <extpp/default_file_format.hpp>
#include <extpp/identity_key.hpp>

#include <clipp.h>
#include <fmt/format.h>

#include <cstdint>
#include <chrono>
#include <iostream>
#include <random>

enum class program_mode {
    stats, query, insert, verify, dump
};

struct options {
    program_mode mode = program_mode::stats;
    std::string file;
    std::uint32_t cache_size = 128;
    std::uint64_t iterations = 10'000'000;

    struct insert_t {
        bool linear = false;
    } insert;
};

static void parse_options(int argc, char** argv, options& o) {
    using namespace clipp;

    bool show_help = false;

    auto general = "General options:" % (
        (required("-f", "--file") & value("file", o.file))          % "database file",
        (option("-m", "--cache-size") & value("M", o.cache_size))   % fmt::format("cache size in blocks (default: {})", o.cache_size),
        (option("-n", "--iterations") & value("N", o.iterations))   % fmt::format("number of iterations (default: {})", o.iterations),
        (option("-h", "--help").set(show_help))                     % "display this text"
    );

    auto cmd_insert = (
        command("insert").set(o.mode, program_mode::insert)         % "insert elements into the tree",
        "Subcommand insert:" % group(
            (option("--linear").set(o.insert.linear, true)          % "Perform linear insertion instead of random insertion.")
        )
    );

    auto cmd_query = command("query").set(o.mode, program_mode::query) % "query the tree (using random values)";

    auto cmd_verify = command("verify").set(o.mode, program_mode::verify) % "run the verification function";

    auto cmd_dump = command("dump").set(o.mode, program_mode::dump) % "dump tree contents to stdout";

    auto cmd_stats = command("stats").set(o.mode, program_mode::stats) % "print tree statistics";

    auto cli = (general, (cmd_insert | cmd_query | cmd_verify | cmd_dump | cmd_stats));

    auto print_usage = [&](bool include_help){
        auto fmt = doc_formatting()
                .merge_alternative_flags_with_common_prefix(false)
                .merge_joinable_with_common_prefix(false)
                .start_column(0)
                .doc_column(30)
                .max_flags_per_param_in_usage(1);

        std::cout << "Usage:\n"
                  << usage_lines(cli, argv[0], doc_formatting(fmt).start_column(4))
                  << "\n";
        if (include_help) {
            std::cout << "\n"
                      << documentation(cli, fmt)
                      << "\n";
        }
    };

    parsing_result result = parse(argc, argv, cli);
    if (show_help) {
        print_usage(true);
        std::exit(1);
    }

    if (!result) {
        if (!result.missing().empty()) {
            std::cout << "Required parameters are missing.\n\n";
        }
        print_usage(false);
        std::exit(1);
    }
}

static constexpr std::uint32_t block_size = 4096;

using value_type = int;
using tree_type = extpp::btree<value_type, extpp::identity_key, std::less<>, block_size>;
using format_type = extpp::default_file_format<tree_type::anchor, block_size>;

static void run_insert(tree_type& tree, std::uint64_t n, bool linear) {
    std::default_random_engine engine(std::random_device{}());
    std::uniform_int_distribution<value_type> dist;

    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "Inserting " << n << " random numbers\n\n" << std::flush;

    std::uint64_t insertions = 0;
    for (std::uint64_t i = 0; i < n; ++i) {
        value_type v;
        if (linear) {
            v = i;
        } else {
            v = dist(engine);
        }

        auto [pos, inserted] = tree.insert(v);
        insertions += inserted;
        (void) pos;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();

    std::cout << "Done. " << insertions << " values were actually inserted (the rest were collisions).\n"
              << "Time taken: " << elapsed << " Seconds.\n"
              << "\n"
              << std::flush;
}

static void run_query(tree_type& tree, std::uint64_t n) {
    std::default_random_engine engine(std::random_device{}());
    std::uniform_int_distribution<value_type> dist;

    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "Querying " << n << " random numbers\n\n" << std::flush;

    for (std::uint64_t i = 0; i < n; ++i) {
        value_type v = dist(engine);

        auto pos = tree.find(v);
        (void) pos;
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();

    std::cout << "Done.\n"
              << "Time taken: " << elapsed << " Seconds.\n"
              << "\n"
              << std::flush;
}

int main(int argc, char** argv) {
    options o;
    parse_options(argc, argv, o);

    std::cout << "Opening file " << o.file << "\n"
              << "Caching " << o.cache_size << " Blocks\n"
              << "\n"
              << std::flush;

    auto file = extpp::system_vfs().open(o.file.c_str(), extpp::vfs::read_write, extpp::vfs::open_create);
    format_type format(*file, o.cache_size);
    tree_type tree(format.user_data(), format.get_allocator());

    {
        switch (o.mode) {
        case program_mode::stats:
            std::cout << "Tree attributes:\n"
                      << "  Height:          " << tree.height() << "\n"
                      << "  Size:            " << tree.size() << "\n"
                      << "  Fill factor:     " << tree.fill_factor() << "\n"
                      << "  Internal fanout: " << tree.internal_fanout() << "\n"
                      << "  Leaf fanout:     " << tree.leaf_fanout() << "\n"
                      << "  Iternal nodes:   " << tree.internal_nodes() << "\n"
                      << "  Leaf nodes:      " << tree.leaf_nodes() << "\n"
                      << "\n"
                      << std::flush;
            break;
        case program_mode::verify:
            try {
                tree.verify();
                std::cout << "Verification successful." << "\n";
            } catch (const std::exception& e) {
                std::cout << "Verification failed: " << e.what() << "\n";
            }
            break;
        case program_mode::dump:
            std::cout << "Tree content:\n";
            for (const value_type& v : tree) {
                std::cout << v << "\n";
            }
            break;
        case program_mode::insert:
            run_insert(tree, o.iterations, o.insert.linear);
            break;
        case program_mode::query:
            run_query(tree, o.iterations);
            break;
        }
    }

    format.flush();
    extpp::engine_stats stats = format.get_engine().stats();

    std::cout << "I/O statistics:\n"
              << "  Reads:      " << stats.reads << "\n"
              << "  Writes:     " << stats.writes << "\n"
              << "  Cache hits: " << stats.cache_hits << "\n"
              << std::flush;
    return 0;
}
