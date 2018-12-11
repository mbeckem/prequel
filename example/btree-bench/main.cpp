#include <prequel/container/btree.hpp>
#include <prequel/container/hash_table.hpp>
#include <prequel/simple_file_format.hpp>

#include <clipp.h>
#include <fmt/format.h>

#include <chrono>
#include <random>

using namespace prequel;

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
    bool mmap = false;
    u32 block_size_bytes = 0;
    u32 cache_size_megabytes = 1;

    struct init_t {
        enum which_t { small_tree, large_tree, small_hash, large_hash };
        which_t which = small_tree;
    } init;

    struct insert_t {
        enum which_t { linear, random };
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
    u64 key1 = 0;                             // +  8 Byte
    u64 key2 = 0;                             // +  8 Byte
    u16 ignored1[16]{1, 2, 4, 5, 9, 11};      // + 32 Byte
    std::tuple<u64, u64> ignored2{44, 12332}; // + 16 Byte
    std::array<byte, 60> ignored3{};          // + 60 Byte
    u32 ignored4 = 7;                         // +  4 Byte
                                              // =========
                                              //  128 Byte

    static constexpr auto get_binary_format() {
        return binary_format(&large_value::key1, &large_value::key2, &large_value::ignored1,
                             &large_value::ignored2, &large_value::ignored3, &large_value::ignored4);
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

using small_value_hash = hash_table<i64>;
using large_value_hash = hash_table<large_value, large_value::key_extract>;

template<typename Container>
constexpr bool is_tree() {
    return std::is_same_v<Container, small_value_tree> || std::is_same_v<Container, large_value_tree>;
}

template<typename Container>
constexpr bool is_hash() {
    return std::is_same_v<Container, small_value_hash> || std::is_same_v<Container, large_value_hash>;
}

// This value is stored inside the first block on disk.
// The state must be initialized first (it has to contain
// either a small tree or large tree in order to be useful).
struct anchor {
    std::variant<std::monostate, small_value_tree::anchor, large_value_tree::anchor,
                 small_value_hash::anchor, large_value_hash::anchor>
        container;

    static constexpr auto get_binary_format() { return binary_format(&anchor::container); }
};

using format_type = simple_file_format<anchor>;

options parse_options(int argc, char** argv) {
    using namespace clipp;

    options opts;
    bool show_help = false;

    auto general_options =
        "General options:"
        % ((option("-h", "--help").set(show_help)) % "Show help",
           (option("-m", "--cache-size") & value("MB", opts.cache_size_megabytes))
               % "Cache size (in Megabyte)",
           (option("--mmap").set(opts.mmap, true)
            % "Use mmap instead of normal file I/O (cache size will be ignored)."));

    auto required_options = "Required options:"
                            % ((required("-f", "--file") & value("file", opts.file)) % "Input file",
                               (required("-b", "--block-size") & value("B", opts.block_size_bytes))
                                   % "Block size (in Byte)");

    auto cmd_init =
        (command("init")
             .set(opts.action, mode::init)
             .set(opts.write_mode, true)
             .set(opts.create_mode, true),
         one_of(required("small-tree").set(opts.init.which, options::init_t::small_tree),
                required("large-tree").set(opts.init.which, options::init_t::large_tree),
                required("small-hash").set(opts.init.which, options::init_t::small_hash),
                required("large-hash").set(opts.init.which, options::init_t::large_hash)))
        % "Initialize a container";

    auto cmd_stats =
        group(command("stats").set(opts.action, mode::stats)) % "Print container statistics";

    auto cmd_dump = group(command("dump").set(opts.action, mode::dump))
                    % "Print the entire content of the container";

    auto cmd_validate = group(command("validate").set(opts.action, mode::validate))
                        % "Check the integrity of the container";

    auto cmd_insert =
        (command("insert").set(opts.action, mode::insert).set(opts.write_mode, true),
         one_of(required("linear").set(opts.insert.which, options::insert_t::linear),
                required("random").set(opts.insert.which, options::insert_t::random)),
         value("N").set(opts.insert.count))
        % "Insert N elements into the container, either random or in linear (ascending) order";

    auto cmd_bulk_load =
        (command("bulk_load").set(opts.action, mode::bulk_load).set(opts.write_mode, true),
         value("N").set(opts.bulk_load.count))
        % "Insert N elements into an empty container, in ascending linear order";

    auto cmd_query =
        (command("query").set(opts.action, mode::query), value("N").set(opts.query.count))
        % "Query for N random values in the container (between min and max)";

    std::vector<clipp::group> subcommands{cmd_init,   cmd_stats,     cmd_dump, cmd_validate,
                                          cmd_insert, cmd_bulk_load, cmd_query};

    clipp::group subcommands_group;
    subcommands_group.exclusive(true);
    for (auto& cmd : subcommands)
        subcommands_group.push_back(cmd);

    auto print_usage = [&](bool include_help) {
        auto fmt = doc_formatting()
                       .merge_alternative_flags_with_common_prefix(false)
                       .merge_joinable_with_common_prefix(false)
                       .start_column(0)
                       .doc_column(30)
                       .max_flags_per_param_in_usage(1);

        std::cout << "Usage:\n"
                  << "    " << argv[0] << " REQUIRED_OPTIONS... [OPTIONS...] SUBCOMMAND ARGS..."
                  << "\n"
                  << "    " << argv[0] << " -h"
                  << "\n";
        if (include_help) {
            std::cout << "\n"
                      << documentation(required_options, fmt) << "\n\n"
                      << documentation(general_options, fmt) << "\n\n";

            std::cout << "Subcommands "
                      << "\n"
                      << "-----------"
                      << "\n\n";
            for (const auto& cmd : subcommands)
                std::cout << documentation(cmd, fmt) << "\n\n";
        }
    };

    auto cli = (required_options, general_options, subcommands_group);

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

// Open the appropriate container, then run the function.
template<typename Function>
void container_operation(anchor_handle<anchor> container_anchor, allocator& alloc, Function&& fn) {
    struct variant_visitor {
        anchor_flag* changed;
        allocator* alloc;
        Function* fn;

        void operator()(std::monostate) const {
            throw std::runtime_error("Container was not initialized.");
        }

        void operator()(small_value_tree::anchor& state) const {
            small_value_tree tree(make_anchor_handle(state, *changed), *alloc);
            (*fn)(tree);
        }

        void operator()(large_value_tree::anchor& state) const {
            large_value_tree tree(make_anchor_handle(state, *changed), *alloc);
            (*fn)(tree);
        }

        void operator()(small_value_hash::anchor& state) const {
            small_value_hash hash(make_anchor_handle(state, *changed), *alloc);
            (*fn)(hash);
        }

        void operator()(large_value_hash::anchor& state) const {
            large_value_hash hash(make_anchor_handle(state, *changed), *alloc);
            (*fn)(hash);
        }
    };

    anchor_flag changed;
    auto current_state = container_anchor.get<&anchor::container>();
    std::visit(variant_visitor{&changed, &alloc, &fn}, current_state);
    if (changed) {
        container_anchor.set<&anchor::container>(current_state);
    }
}

template<typename... Args>
void container_stats(btree<Args...>& tree) {
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

template<typename... Args>
void container_stats(hash_table<Args...>& hash) {
    std::cout << "Static properties:\n"
              << "  Value size:        " << hash.value_size() << "\n"
              << "  Key size:          " << hash.key_size() << "\n"
              << "  Bucket capacity:   " << hash.bucket_capacity() << "\n"
              << "\n"
              << "Dynamic properties:\n"
              << "  Size:              " << hash.size() << "\n"
              << "  Primary buckets:   " << hash.primary_buckets() << "\n"
              << "  Overflow buckets:  " << hash.overflow_buckets() << "\n"
              << "  Allocated buckets: " << hash.allocated_buckets() << "\n"
              << "  Byte size:         " << hash.byte_size() << "\n"
              << "  Fill factor:       " << hash.fill_factor() << "\n"
              << "  Overhead:          " << hash.overhead() << "\n"
              << "\n"
              << std::flush;
}

static std::mt19937 rng{std::random_device{}()};

template<typename Container>
auto random_values(Container& container) {
    using value_type = typename Container::value_type;

    unused(container);

    if constexpr (std::is_same_v<value_type, i64>) {
        return []() { return std::uniform_int_distribution<i64>()(rng); };
    } else if constexpr (std::is_same_v<value_type, large_value>) {
        return []() {
            std::uniform_int_distribution<u64> dist;

            large_value v;
            v.key1 = dist(rng);
            v.key2 = dist(rng);
            return v;
        };
    } else {
        static_assert(always_false<value_type>::value, "Not implemented.");
    }
}

template<typename Container>
auto linear_values(Container& container) {
    using value_type = typename Container::value_type;

    static_assert(is_tree<Container>(), "Unsupported container.");

    if constexpr (std::is_same_v<value_type, i64>) {
        i64 v = 0;
        if (!container.empty()) {
            v = container.create_cursor(container.seek_max).get() + 1;
        }

        return [v]() mutable { return v++; };
    } else if constexpr (std::is_same_v<value_type, large_value>) {
        std::pair<u64, u64> v{};
        if (!container.empty()) {
            v = container.derive_key(container.create_cursor(container.seek_max).get());
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
    } else {
        static_assert(always_false<value_type>::value, "Not implemented.");
    }
}

i64 random_key(small_value_tree&, i64 min, i64 max) {
    std::uniform_int_distribution<i64> dist(min, max);
    return dist(rng);
}

std::pair<u64, u64>
random_key(large_value_tree&, const std::pair<u64, u64>& min, const std::pair<u64, u64>& max) {
    std::uniform_int_distribution<u64> d1(min.first, max.first);
    std::uniform_int_distribution<u64> d2(min.second, max.second);
    return std::make_pair(d1(rng), d2(rng));
}

template<typename Func>
void measure(format_type& format, Func&& fn) {
    high_resolution_clock::time_point start = high_resolution_clock::now();
    u64 ops = 0;
    {
        ops = fn();
        std::cout << "Flushing cached buffers." << std::endl;
        format.flush();
    }
    high_resolution_clock::time_point end = high_resolution_clock::now();

    // Mmap engine does not support read/write stats.
    file_engine_stats stats;
    if (file_engine* fe = dynamic_cast<file_engine*>(&format.get_engine()))
        stats = fe->stats();
    double seconds = std::chrono::duration<double>(end - start).count();
    double megabytes_read = (stats.reads * format.block_size()) / double(1 << 20);
    double megabytes_written = (stats.writes * format.block_size()) / double(1 << 20);

    fmt::print(std::cout,
               "Operation complete.\n"
               "  Time taken:      {:12.3f} seconds\n"
               "  Operating speed: {:12.3f} ops/s\n"
               "  Blocks read:     {:12} ({:9.3f} MB)\n"
               "  Blocks written:  {:12} ({:9.3f} MB)\n"
               "  Cache hits:      {:12}\n"
               "  Read bandwidth:  {:12.3f} MB/s\n"
               "  Write bandwidth: {:12.3f} MB/s\n",
               seconds, (double) ops / seconds, stats.reads, megabytes_read, stats.writes,
               megabytes_written, stats.cache_hits, megabytes_read / seconds,
               megabytes_written / seconds);
    std::cout << std::flush;
}

template<typename Container, typename ItemGenerator>
void run_container_insert(format_type& format, Container& container, ItemGenerator&& gen, u64 count) {
    measure(format, [&]() {
        const u64 reporting_interval = std::max(count / 100, u64(1));

        if constexpr (std::is_same_v<large_value_tree,
                                     Container> || std::is_same_v<small_value_tree, Container>) {
            std::cout << "Beginning to insert " << count << " elements." << std::endl;

            auto cursor = container.create_cursor();
            for (u64 i = 0; i < count; ++i) {
                auto item = gen();
                cursor.insert_or_update(item);

                if ((i + 1) % reporting_interval == 0) {
                    std::cout << "Inserted " << (i + 1) << " elements." << std::endl;
                }
            }
            return count;
        } else if constexpr (std::is_same_v<large_value_hash,
                                            Container> || std::is_same_v<small_value_hash, Container>) {
            std::cout << "Beginning to insert " << count << " elements." << std::endl;

            for (u64 i = 0; i < count; ++i) {
                auto item = gen();
                container.insert_or_update(item);

                if ((i + 1) % reporting_interval == 0) {
                    std::cout << "Inserted " << (i + 1) << " elements." << std::endl;
                }
            }
            return count;
        } else {
            static_assert(prequel::always_false<Container>::value, "Not implemented.");
        }
    });
}

template<typename Container>
void container_insert(format_type& format, Container& container, options::insert_t::which_t mode,
                      u64 count) {
    switch (mode) {
    case options::insert_t::random:
        run_container_insert(format, container, random_values(container), count);
        break;
    case options::insert_t::linear:
        if constexpr (is_tree<Container>()) {
            run_container_insert(format, container, linear_values(container), count);
        } else {
            throw std::runtime_error("Only trees are supported for linear insertion benchmarks.");
        }
        break;
    }
}

template<typename Container>
void container_bulk_load(format_type& format, Container& container, u64 count) {
    if constexpr (is_tree<Container>()) {
        measure(format, [&]() {
            const u64 reporting_interval = std::max(count / 100, u64(1));

            auto gen = linear_values(container);

            std::cout << "Beginning to insert " << count << " elements." << std::endl;

            auto loader = container.bulk_load();
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
    } else {
        unused(format, container, count);
        throw std::runtime_error("Only trees are supported for bulk insertion benchmarks.");
    }
}

template<typename Container>
void container_query(format_type& format, Container& container, u64 count) {
    if constexpr (is_tree<Container>()) {
        measure(format, [&]() {
            if (container.empty()) {
                throw std::runtime_error("The container is empty.");
            }

            std::cout << "Beginning to query for " << count << " values." << std::endl;

            const u64 reporting_interval = std::max(count / 100, u64(1));
            u64 found = 0;

            auto min_key = container.derive_key(container.create_cursor(container.seek_min).get());
            auto max_key = container.derive_key(container.create_cursor(container.seek_max).get());

            auto cursor = container.create_cursor();
            for (u64 i = 0; i < count; ++i) {
                auto key = random_key(container, min_key, max_key);
                found += cursor.find(key);

                if ((i + 1) % reporting_interval == 0) {
                    std::cout << "Queried for " << (i + 1) << " elements (" << found
                              << " were found)." << std::endl;
                }
            }

            return count;
        });
    } else {
        unused(format, container, count);
        throw std::runtime_error("Query benchmark has not yet been implemented for hash tables.");
    }
}

void run(format_type& format, anchor_handle<anchor> container_anchor, allocator& alloc,
         const options& opts) {
    switch (opts.action) {
    case mode::init: {
        auto container_state = container_anchor.get();
        if (!std::holds_alternative<std::monostate>(container_state.container)) {
            throw std::runtime_error("Container is already initialized.");
        }

        switch (opts.init.which) {
        case options::init_t::small_tree:
            container_state.container = small_value_tree::anchor();
            std::cout << "Initialized tree with small value size." << std::endl;
            break;
        case options::init_t::large_tree:
            container_state.container = large_value_tree::anchor();
            std::cout << "Initialized tree with large value size." << std::endl;
            break;
        case options::init_t::small_hash:
            container_state.container = small_value_hash::anchor();
            std::cout << "Initialized hash table with small value size." << std::endl;
            break;
        case options::init_t::large_hash:
            container_state.container = large_value_hash::anchor();
            std::cout << "Initialized hash table with large value size." << std::endl;
            break;
        }
        container_anchor.set(container_state);
        break;
    }
    case mode::stats:
        container_operation(container_anchor, alloc,
                            [&](auto& container) { container_stats(container); });
        break;
    case mode::dump:
        container_operation(container_anchor, alloc, [&](auto& container) {
            // FIXME implement dump for hash_table<T> so we can erase the raw()
            container.raw().dump(std::cout);
            std::cout << std::flush;
        });
        break;
    case mode::validate:
        container_operation(container_anchor, alloc, [&](auto& container) { container.validate(); });
        break;
    case mode::insert:
        container_operation(container_anchor, alloc, [&](auto& container) {
            container_insert(format, container, opts.insert.which, opts.insert.count);
        });
        break;
    case mode::bulk_load:
        container_operation(container_anchor, alloc, [&](auto& container) {
            container_bulk_load(format, container, opts.bulk_load.count);
        });
        break;
    case mode::query:
        container_operation(container_anchor, alloc, [&](auto& container) {
            container_query(format, container, opts.query.count);
        });
        break;
    }
}

static const prequel::magic_header magic("btree-bench");
static const uint32_t version = 1;

int main(int argc, char** argv) {
    // Parse command line options.
    options opts;
    try {
        opts = parse_options(argc, argv);
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    // Define the file format.
    format_type file_format(magic, version, opts.block_size_bytes);
    file_format.cache_size(u64(opts.cache_size_megabytes) * u64(1 << 20));
    file_format.engine_type(opts.mmap ? file_format.mmap_engine : file_format.file_engine);

    // Open the file in the correct mode and maybe create it.
    try {
        if (opts.create_mode) {
            file_format.create(opts.file.c_str(), anchor());
        } else {
            file_format.open(opts.file.c_str(), !opts.write_mode);
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
        return 1;
    }

    fmt::print("Using mmap: {}\n", opts.mmap);

    try {
        anchor anchor_value = file_format.get_user_data();
        prequel::anchor_flag anchor_changed;

        file_format.get_allocator().min_chunk(4096);
        run(file_format, prequel::make_anchor_handle(anchor_value, anchor_changed),
            file_format.get_allocator(), opts);

        if (anchor_changed)
            file_format.set_user_data(anchor_value);

        file_format.flush();
    } catch (const std::exception& e) {
        std::cerr << "Failed to run: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
