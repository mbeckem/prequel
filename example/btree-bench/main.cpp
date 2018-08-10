#include <extpp/btree.hpp>
#include <extpp/default_file_format.hpp>

#include <clipp.h>
#include <fmt/format.h>

#include <chrono>
#include <random>

using namespace extpp;

using std::chrono::high_resolution_clock;

enum class mode {
    init,
    stats,
    dump,
    validate,
    insert,
    bulk_load,
    query,
};

struct options {
    mode action = mode::init;
    std::string file;
    bool write_mode = false;
    bool create_mode = false;
    u32 block_size_bytes = 0;
    u32 cache_size_megabytes = 1;

    struct init_t {
        enum which_t {
            small, large
        };
        which_t which = small;
    } init;

    struct insert_t {
        enum which_t {
            linear, random
        };
        which_t which = linear;
        u64 count = 0;
    } insert;

    struct bulk_load_t {
        u64 count = 0;
    } bulk_load;

    struct query_t {
        u64 count = 0;
    } query;
};

// For printing of composite key below.
template<typename T1, typename T2>
std::ostream& operator<<(std::ostream& os, const std::pair<T1, T2>& v) {
    return os << "(" << v.first << ", " << v.second << ")";
}

// A large value (128 bytes), indexed by a composite key.
struct large_value {
    u64 key1 = 0;                               // +  8 Byte
    u64 key2 = 0;                               // +  8 Byte
    u16 ignored1[16]{1, 2, 4, 5, 9, 11};        // + 32 Byte
    std::tuple<u64, u64> ignored2{44, 12332};   // + 16 Byte
    std::array<byte, 60> ignored3{};            // + 60 Byte
    u32 ignored4 = 7;                           // +  4 Byte
                                                // =========
                                                //  128 Byte

    static constexpr auto get_binary_format() {
        return make_binary_format(&large_value::key1, &large_value::key2,
                                  &large_value::ignored1, &large_value::ignored2,
                                  &large_value::ignored3, &large_value::ignored4);
    }

    // Values are indexed by both keys (lexicographically).
    // The pair comparison operator does that job for us.
    struct key_extract {
        std::pair<u64, u64> operator()(const large_value& value) const {
            return std::pair(value.key1, value.key2);
        }
    };

    friend std::ostream& operator<<(std::ostream& os, const large_value& v) {
        return os << key_extract()(v);
    }
};

static_assert(serialized_size<large_value>() == 128);

// The values inside are indexed by itself, i.e. the tree implements a set.
using small_value_tree = btree<i64>;
using large_value_tree = btree<large_value, large_value::key_extract>;

// This value is stored inside the first block on disk.
// The state must be initialized first (it has to contain
// either a small tree or large tree in order to be useful).
struct anchor {
    std::variant<
        std::monostate,
        small_value_tree::anchor,
        large_value_tree::anchor
    > tree;

    static constexpr auto get_binary_format() {
        return make_binary_format(&anchor::tree);
    }
};

options parse_options(int argc, char** argv) {
    using namespace clipp;

    options opts;
    bool show_help = false;

    auto general = "General options:" % (
        (option("-h", "--help").set(show_help))
                % "Show help",
        (required("-f", "--file") & value("file", opts.file))
                % "Input file",
        (required("-b", "--block-size") & value("B", opts.block_size_bytes))
                % "Block size (in Byte)",
        (required("-m", "--cache-size") & value("MB", opts.cache_size_megabytes))
                % "Cache size (in Megabyte)"
    );

    auto cli = (
        general,
        (
            (
                command("init")
                        .set(opts.action, mode::init)
                        .set(opts.write_mode, true)
                        .set(opts.create_mode, true),
                one_of(
                    required("small").set(opts.init.which, options::init_t::small),
                    required("large").set(opts.init.which, options::init_t::large))
            ) % "Initialize the tree datastructure with small or large values" |

            command("stats").set(opts.action, mode::stats)
                    % "Print tree statistics" |

            command("dump").set(opts.action, mode::dump)
                    % "Print the entire content of the tree" |

            command("validate").set(opts.action, mode::validate)
                    % "Check the integrity of the tree" |

            (
                command("insert").set(opts.action, mode::insert).set(opts.write_mode, true),
                one_of(
                    required("linear").set(opts.insert.which, options::insert_t::linear),
                    required("random").set(opts.insert.which, options::insert_t::random)),
                value("N").set(opts.insert.count)
            ) % "Insert N elements into the tree, either random or in linear (ascending) order" |

            (
                command("bulk_load").set(opts.action, mode::bulk_load).set(opts.write_mode, true),
                value("N").set(opts.bulk_load.count)
            ) % "Insert N elements into an empty tree, in ascending linear order" |

            (
                command("query").set(opts.action, mode::query),
                value("N").set(opts.query.count)
            ) % "Query for N random values in the tree (between min and max)"
        )
    );

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
            std::cerr << "Required parameters are missing.\n\n";
        }
        print_usage(false);
        std::exit(1);
    }

    return opts;
}

// Open the appropriate tree, then run the function.
template<typename Function>
void tree_operation(anchor_handle<anchor> tree_anchor, allocator& alloc, Function&& fn) {
    struct variant_visitor {
        anchor_flag* changed;
        allocator* alloc;
        Function* fn;

        void operator()(std::monostate) const {
            throw std::runtime_error("Tree was not initialized.");
        }

        void operator()(small_value_tree::anchor& state) const {
            small_value_tree tree(make_anchor_handle(state, *changed), *alloc);
            (*fn)(tree);
        }

        void operator()(large_value_tree::anchor& state) const {
            large_value_tree tree(make_anchor_handle(state, *changed), *alloc);
            (*fn)(tree);
        }
    };

    anchor_flag changed;
    auto current_state = tree_anchor.get<&anchor::tree>();
    std::visit(variant_visitor{&changed, &alloc, &fn}, current_state);
    if (changed) {
        tree_anchor.set<&anchor::tree>(current_state);
    }
}

template<typename Tree>
void tree_stats(Tree& tree) {
    std::cout << "Static properties:\n"
              << "  Value size:      " << tree.value_size() << "\n"
              << "  Key size:        " << tree.key_size() << "\n"
              << "  Internal fanout: " << tree.internal_node_capacity() << "\n"
              << "  Leaf fanout:     " << tree.leaf_node_capacity() << "\n"
              << "\n"
              << "Dynamic properties:\n"
              << "  Height:          " << tree.height() << "\n"
              << "  Size:            " << tree.size() << "\n"
              << "  Internal nodes:  " << tree.internal_nodes() << "\n"
              << "  Leaf nodes:      " << tree.leaf_nodes() << "\n"
              << "  Byte size:       " << tree.byte_size() << "\n"
              << "  Fill factor:     " << tree.fill_factor() << "\n"
              << "  Overhead:        " << tree.overhead() << "\n"
              << "\n"
              << std::flush;
}

static std::mt19937 rng{std::random_device{}()};

auto random_values(small_value_tree&) {
    return []() {
        return std::uniform_int_distribution<i64>()(rng);
    };
}

auto random_values(large_value_tree&) {
    return []() {
        std::uniform_int_distribution<u64> dist;

        large_value v;
        v.key1 = dist(rng);
        v.key2 = dist(rng);
        return v;
    };
}

auto linear_values(small_value_tree& tree) {
    i64 v = 0;
    if (!tree.empty()) {
        v = tree.create_cursor(tree.seek_max).get() + 1;
    }

    return [v]() mutable {
        return v++;
    };
}

auto linear_values(large_value_tree& tree) {
    std::pair<u64, u64> v{};
    if (!tree.empty()) {
        v = tree.derive_key(tree.create_cursor(tree.seek_max).get());
    }

    return [v]() mutable {
        v.second += 1;
        if (v.second >= 100) {
            v.first += 1;
            v.second = 0;
        }

        large_value l;
        l.key1 = v.first;
        l.key2 = v.second;
        return l;
    };
}

u64 random_key(small_value_tree&, u64 min, u64 max) {
    std::uniform_int_distribution<u64> dist(min, max);
    return dist(rng);
}

std::pair<u64, u64> random_key(large_value_tree&,
                               const std::pair<u64, u64>& min,
                               const std::pair<u64, u64>& max)
{
    std::uniform_int_distribution<u64> d1(min.first, max.first);
    std::uniform_int_distribution<u64> d2(min.second, max.second);
    return std::make_pair(d1(rng), d2(rng));
}

template<typename Func>
void measure(file_engine& engine, Func&& fn) {
    high_resolution_clock::time_point start = high_resolution_clock::now();
    u64 ops = 0;
    {
        ops = fn();
        std::cout << "Flushing cached buffers." << std::endl;
        engine.flush();
        engine.fd().sync();
    }
    high_resolution_clock::time_point end = high_resolution_clock::now();

    file_engine_stats stats = engine.stats();
    double seconds = std::chrono::duration<double>(end - start).count();
    double megabytes_read = (stats.reads * engine.block_size()) / double(1 << 20);
    double megabytes_written = (stats.writes * engine.block_size()) / double(1 << 20);

    fmt::print(std::cout,
               "Operation complete.\n"
               "  Time taken:      {:12.3f} seconds\n"
               "  Operating speed: {:12.3f} ops/s\n"
               "  Blocks read:     {:12} ({:9.3f} MB)\n"
               "  Blocks written:  {:12} ({:9.3f} MB)\n"
               "  Cache hits:      {:12}\n"
               "  Read bandwidth:  {:12.3f} MB/s\n"
               "  Write bandwidth: {:12.3f} MB/s\n",
               seconds, (double) ops / seconds,
               stats.reads, megabytes_read,
               stats.writes, megabytes_written,
               stats.cache_hits,
               megabytes_read / seconds,
               megabytes_written / seconds
    );
    std::cout << std::flush;
}

template<typename Tree, typename ItemGenerator>
void run_tree_insert(file_engine& engine, Tree& tree, ItemGenerator&& gen, u64 count) {
    measure(engine, [&]() {
        const u64 reporting_interval = std::max(count / 100, u64(1));

        std::cout << "Beginning to insert " << count << " elements." << std::endl;

        auto cursor = tree.create_cursor();
        for (u64 i = 0; i < count; ++i) {
            auto item = gen();
            cursor.insert_or_update(item);

            if ((i + 1) % reporting_interval == 0) {
                std::cout << "Inserted " << (i + 1) << " elements." << std::endl;
            }
        }
        return count;
    });
}

template<typename Tree>
void tree_insert(file_engine& engine, Tree& tree, options::insert_t::which_t mode, u64 count) {
    switch (mode) {
    case options::insert_t::random:
        run_tree_insert(engine, tree, random_values(tree), count);
        break;
    case options::insert_t::linear:
        run_tree_insert(engine, tree, linear_values(tree), count);
        break;
    }
}

template<typename Tree>
void tree_bulk_load(file_engine& engine, Tree& tree, u64 count) {
    measure(engine, [&]() {
        const u64 reporting_interval = std::max(count / 100, u64(1));

        auto gen = linear_values(tree);

        std::cout << "Beginning to insert " << count << " elements." << std::endl;

        auto loader = tree.bulk_load();
        for (u64 i = 0; i < count; ++i) {
            auto item = gen();
            loader.insert(item);

            if ((i + 1) % reporting_interval == 0) {
                std::cout << "Inserted " << (i + 1) << " elements." << std::endl;
            }
        }
        loader.finish();
        return count;
    });
}

template<typename Tree>
void tree_query(file_engine& engine, Tree& tree, u64 count) {
    measure(engine, [&]() {
        if (tree.empty()) {
            throw std::runtime_error("The tree is empty.");
        }

        std::cout << "Beginning to query for " << count << " values." << std::endl;

        const u64 reporting_interval = std::max(count / 100, u64(1));
        u64 found = 0;

        auto min_key = tree.derive_key(tree.create_cursor(tree.seek_min).get());
        auto max_key = tree.derive_key(tree.create_cursor(tree.seek_max).get());

        auto cursor = tree.create_cursor();
        for (u64 i = 0; i < count; ++i) {
            auto key = random_key(tree, min_key, max_key);
            found += cursor.find(key);

            if ((i + 1) % reporting_interval == 0) {
                std::cout << "Queried for " << (i + 1) << " elements (" << found << " were found)." << std::endl;
            }
        }

        return count;
    });
}

void run(file_engine& engine, anchor_handle<anchor> tree_anchor, allocator& alloc, const options& opts) {
    switch (opts.action) {
    case mode::init:
    {
        auto tree_state = tree_anchor.get();
        if (!std::holds_alternative<std::monostate>(tree_state.tree)) {
            throw std::runtime_error("Tree is already initialized.");
        }

        switch (opts.init.which) {
        case options::init_t::small:
            tree_state.tree = small_value_tree::anchor();
            std::cout << "Initialized with small value size." << std::endl;
            break;
        case options::init_t::large:
            tree_state.tree = large_value_tree::anchor();
            std::cout << "Initialized with large value size." << std::endl;
            break;
        }
        tree_anchor.set(tree_state);
        break;
    }
    case mode::stats:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree_stats(tree);
        });
        break;
    case mode::dump:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree.dump(std::cout);
            std::cout << std::flush;
        });
        break;
    case mode::validate:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree.validate();
        });
        break;
    case mode::insert:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree_insert(engine, tree, opts.insert.which, opts.insert.count);
        });
        break;
    case mode::bulk_load:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree_bulk_load(engine, tree, opts.bulk_load.count);
        });
        break;
    case mode::query:
        tree_operation(tree_anchor, alloc, [&](auto& tree) {
            tree_query(engine, tree, opts.query.count);
        });
        break;
    }
}

int main(int argc, char** argv) {
    // Parse command line options.
    options opts;
    try {
        opts = parse_options(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    // Open the file in the correct mode and maybe create it.
    std::unique_ptr<file> input;
    try {
        input = system_vfs().open(
                opts.file.c_str(),
                opts.write_mode ? vfs::read_write : vfs::read_only,
                opts.create_mode ? vfs::open_create : vfs::open_normal
        );
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    try {
        // The number of blocks cached in memory.
        const u32 block_size = opts.block_size_bytes;
        const u32 cache_blocks = (u64(opts.cache_size_megabytes) * u64(1 << 20)) / block_size;

        default_file_format<anchor> file_format(*input, block_size, cache_blocks);
        file_format.get_allocator().min_chunk(4096);
        file_format.get_allocator().min_meta_chunk(64);

        run(file_format.get_engine(),
            file_format.get_user_data(),
            file_format.get_allocator(), opts);

        file_format.flush();
        input->sync();
    } catch (const std::exception& e) {
        std::cerr << "Failed to run: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
