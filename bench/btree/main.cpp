#include <boost/program_options.hpp>
#include <extpp/btree.hpp>
#include <extpp/default_file_format.hpp>
#include <extpp/identity_key.hpp>

#include <cstdint>
#include <chrono>
#include <iostream>
#include <random>

namespace po = boost::program_options;

enum class program_mode {
    stats, query, insert
};

static std::istream& operator>>(std::istream& is, program_mode& m) {
    std::string token;
    is >> token;
    if (token == "stats")
        m = program_mode::stats;
    else if (token == "query")
        m = program_mode::query;
    else if (token == "insert")
        m = program_mode::insert;
    else
        is.setstate(std::ios_base::failbit);
    return is;
}

struct options {
    program_mode mode = program_mode::query;
    std::string file;
    std::uint32_t cache_size = 0;
    std::uint64_t iterations = 0;
};

static options parse_options(int argc, char** argv) {
    options result;

    try {
        po::options_description opt_visible("Allowed options");
        opt_visible.add_options()
                ("help,h", "Print this message.")
                (",c", po::value(&result.cache_size)->value_name("M")->default_value(1024),
                 "The number of blocks to cache in memory.")
                (",n", po::value(&result.iterations)->value_name("N")->default_value(1000000L),
                 "The number of iterations to run.")
        ;

        po::options_description opt_hidden("Hidden options");
        opt_hidden.add_options()
                ("MODE", po::value(&result.mode)->required())
                ("FILE", po::value(&result.file)->required())
        ;

        po::options_description opt_cli;
        opt_cli.add(opt_visible).add(opt_hidden);

        po::positional_options_description p;
        p.add("MODE", 1);
        p.add("FILE", 1);

        po::variables_map vm;
        po::store(po::command_line_parser(argc, argv)
                  .options(opt_cli).positional(p).run(),
                  vm);
        if (vm.count("help")) {
            std::cout << "Usage: " << argv[0] << " [options] MODE FILE\n"
                         "\n"
                         "Possible modes:\n"
                         "  stats       Print statistics about the tree and exit.\n"
                         "  insert      Generate N random numbers and insert them into the tree.\n"
                         "  query       Query the tree for N random numbers.\n"
                         "\n";
            std::cout << opt_visible << std::flush;
            std::exit(1);
        }

        po::notify(vm);
    } catch(const po::error& e) {
        std::cerr << "Error: " << e.what() << "\n";
        std::exit(1);
    }
    return result;
}

static constexpr std::uint32_t block_size = 4096;

using value_type = int;
using tree_type = extpp::btree<value_type, extpp::identity_key, std::less<>, block_size>;
using format_type = extpp::default_file_format<tree_type::anchor, block_size>;

static void run_insert(tree_type& tree, std::uint64_t n) {
    std::default_random_engine engine(std::random_device{}());
    std::uniform_int_distribution<value_type> dist;

    auto start_time = std::chrono::high_resolution_clock::now();

    std::cout << "Inserting " << n << " random numbers\n\n" << std::flush;

    std::uint64_t insertions = 0;
    for (std::uint64_t i = 0; i < n; ++i) {
        value_type v = dist(engine);
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
    (void) tree;
    (void) n;
    std::cout << "Not implemented" << std::endl;
}

int main(int argc, char** argv) {
    options opt = parse_options(argc, argv);

    std::cout << "Opening file " << opt.file << "\n"
              << "Caching " << opt.cache_size << " Blocks\n"
              << "\n"
              << std::flush;

    auto file = extpp::system_vfs().open(opt.file.c_str(), extpp::vfs::read_write, extpp::vfs::open_create);
    format_type format(*file, opt.cache_size);
    {
        tree_type tree(format.user_data(), format.get_allocator());
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

        switch (opt.mode) {
        case program_mode::stats:
            break;
        case program_mode::insert:
            run_insert(tree, opt.iterations);
            break;
        case program_mode::query:
            run_query(tree, opt.iterations);
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
