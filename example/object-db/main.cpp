#include <extpp/btree.hpp>
#include <extpp/default_file_format.hpp>
#include <extpp/heap.hpp>
#include <extpp/id_generator.hpp>

#include <clipp.h>

#include <cassert>
#include <cstdint>
#include <functional>
#include <iostream>
#include <map>
#include <tuple>

namespace objectdb {

static constexpr uint32_t block_size = 4096;

// fnv1-a hash.
inline uint64_t fnv_hash(const uint8_t* begin, const uint8_t* end)
{
    static const uint64_t magic_prime = 0x00000100000001b3;

    uint64_t hash = 0xcbf29ce484222325;
    for ( ; begin != end; ++begin) {
        hash = hash ^ *begin;
        hash = hash * magic_prime;
    }

    return hash;
}

static void load(const extpp::heap& heap, extpp::heap_reference ref, std::string& buffer) {
    buffer.resize(heap.size(ref));
    heap.load(ref, (extpp::byte*) &buffer[0], buffer.size());
}

static extpp::heap_reference save(extpp::heap& heap, const std::string& buffer) {
    return heap.allocate((const extpp::byte*) buffer.data(), buffer.size());
}

class node_id {
private:
    uint64_t m_value = 0;

public:
    node_id() = default;
    node_id(uint64_t value): m_value(value) {}

    uint64_t value() const { return m_value; }

    bool operator<(const node_id& other) const { return m_value < other.m_value; }
    bool operator==(const node_id& other) const { return m_value == other.m_value; }
    bool operator!=(const node_id& other) const { return m_value != other.m_value; }

    static constexpr auto get_binary_format() {
        return extpp::make_binary_format(&node_id::m_value);
    }
};

class interned_strings {
    // If a string is interned, it can be looked up using its
    // hash and contents. Only one copy of such a string exists
    // in the entire database. This saves space for frequently used
    // strings and makes comparisons faster (interned strings can be compared
    // by checking their references for equality).
    //
    // We could use reference counting or garbage collection to collect
    // unused interned keys. But currently we just store them forever (only keys
    // are interned).
    struct entry {
        extpp::heap_reference string;   // Reference to the string.
        uint64_t hash = 0;              // The hash of the string.

        // Entries are indexed by their hash (and the location in the heap as a second
        // key part to make them unique).
        struct derive_key {
            std::tuple<uint64_t, uint64_t> operator()(const entry& s) const {
                return std::tuple<uint64_t, uint64_t>(s.hash, s.string.value());
            }
        };

        static constexpr auto get_binary_format() {
            return extpp::make_binary_format(&entry::string, &entry::hash);
        }
    };

    // Contains one entry for every interned string.
    // Note that this table contains *weak* references, i.e.
    // references to strings in this tree do not count towards
    // the root set. Entries are removed whenever the garbage collector
    // destroys as string.
    using tree_type = extpp::btree<entry, entry::derive_key>;

public:
    using anchor = typename tree_type::anchor;

public:
    interned_strings(extpp::anchor_handle<anchor> anc, extpp::allocator& alloc, extpp::heap& storage)
        : m_tree(std::move(anc), alloc)
        , m_heap(storage)
    {}

    // Returns the reference to the interned copy of that string, if it exists.
    extpp::heap_reference find(const std::string& str) const {
        return find_impl(str, hash(str));
    }

    // Interns the given string. Either returns a reference to some existing copy
    // of that string or inserts a new copy into the heap.
    extpp::heap_reference intern(const std::string& str) {
        const uint64_t h = hash(str);
        if (auto ref = find_impl(str, h))
            return ref;

        entry ent;
        ent.hash = h;
        ent.string = save(m_heap, str);

        auto result = m_tree.insert(ent);
        assert(result.inserted); // Entry must be unique, even when hashes collide.
        (void) result;
        return ent.string;
    }

private:
    extpp::heap_reference find_impl(const std::string& str, uint64_t hash) const {
        std::tuple<uint64_t, uint64_t> key(hash, 0);

        // Loop over all collisions. We have to test the real strings for equality.
        std::string value;
        for (auto pos = m_tree.lower_bound(key); pos; pos.move_next()) {
            entry ent = pos.get();
            if (ent.hash != hash)
                break;

            load(m_heap, ent.string, value);
            if (value == str)
                return ent.string;
        }
        return {};
    }

    static uint64_t hash(const std::string& str) {
        const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
        return fnv_hash(data, data + str.size());
    }

private:
    tree_type m_tree;
    extpp::heap& m_heap;
};

class property_map {
    struct property {
        node_id node;                   // owner of the property
        extpp::heap_reference name;     // name of the property (string, interned)
        extpp::heap_reference value;    // value of the property (string)

        // Indexed by node id value and the key.
        struct derive_key {
            std::tuple<uint64_t, uint64_t> operator()(const property& p) const {
                return std::tuple<uint64_t, uint64_t>(p.node.value(), p.name.value());
            }
        };

        static constexpr auto get_binary_format() {
            return make_binary_format(&property::node, &property::name, &property::value);
        }
    };

    // Contains one entry for every node's property.
    using tree_type = extpp::btree<property, property::derive_key>;

public:
    using anchor = tree_type::anchor;

public:
    property_map(extpp::anchor_handle<anchor> anc, extpp::heap& heap, extpp::allocator& alloc)
        : m_tree(std::move(anc), alloc)
        , m_heap(heap)
    {}

    template<typename Callback>
    void iterate_properties(node_id node, Callback&& cb) const {
        node_range(node, [&](const property& prop, const auto& cursor) {
            (void) cursor;
            cb(prop.name, prop.value);
        });
    }

    // Returns the value of the property `name` in the given node.
    // Returns a null reference if no such property exists.
    extpp::heap_reference get(node_id node, extpp::heap_reference name) const {
        const std::tuple<uint64_t, uint64_t> key(node.value(), name.value());

        auto cursor = m_tree.find(key);
        if (cursor) {
            return cursor.get().value;
        }
        return {};
    }

    // Sets the property `name` of the given node to `value`.
    void set(node_id node, extpp::heap_reference name, extpp::heap_reference value) {
        property p;
        p.node = node;
        p.name = name;
        p.value = value;

        auto result = m_tree.insert(p);
        if (!result.inserted) {
            // The property already existed.
            property old = result.position.get();
            m_heap.free(old.value);
            result.position.set(p);
        }
    }

    // Remove a single property from a node.
    void remove(node_id node, extpp::heap_reference name) {
        const std::tuple<uint64_t, uint64_t> key(node.value(), name.value());

        auto cursor = m_tree.find(key);
        if (!cursor)
            return;

        property prop = cursor.get();
        m_heap.free(prop.value);
        cursor.erase();
    }

    // Remove all properties for that node.
    void remove(node_id node) {
        node_range(node, [&](auto& prop, auto& cursor) {
            (void) prop;
            cursor.erase();
        });
    }

private:
    template<typename CursorCallback>
    void node_range(node_id node, CursorCallback&& cb) const
    {
        const std::tuple<uint64_t, uint64_t> key(node.value(), 0);

        auto cursor = m_tree.create_cursor();
        cursor.lower_bound(key);
        while (cursor) {
            property prop = cursor.get();
            if (prop.node != node)
                break;

            cb(prop, cursor);
            cursor.move_next();
        }
    }

private:
    tree_type m_tree;
    extpp::heap& m_heap;
};

class edge_map {
    struct edge {
        node_id source;
        extpp::heap_reference label; // Interned string.
        node_id destination;

        struct key {
            // Derived from the fields above.
            uint64_t source, destination, label;

            bool operator<(const key& other) const {
                return std::tie(source, label, destination)
                        < std::tie(other.source, other.label, other.destination);
            }

            bool operator==(const key& other) const {
                return source == other.source
                        && label == other.label
                        && destination == other.destination;
            }

            static constexpr auto get_binary_format() {
                return extpp::make_binary_format(&key::source, &key::label, &key::destination);
            }
        };

        struct derive_key {
            key operator()(const edge& e) const {
                return key{e.source.value(), e.label.value(), e.destination.value()};
            }
        };

        static constexpr auto get_binary_format() {
            return extpp::make_binary_format(&edge::source, &edge::label, &edge::destination);
        }
    };

    // Contains one entry for every node's property.
    using tree_type = extpp::btree<edge, edge::derive_key>;

public:
    class anchor {
        tree_type::anchor map, reverse_map;

        friend edge_map;
        friend extpp::binary_format_access;

        static constexpr auto get_binary_format() {
            return extpp::make_binary_format(&anchor::map, &anchor::reverse_map);
        }
    };

public:
    edge_map(extpp::anchor_handle<anchor> anc, extpp::allocator& alloc)
        : m_map(anc.member<&anchor::map>(), alloc)
        , m_reverse_map(anc.member<&anchor::reverse_map>(), alloc)
    {}

    template<typename Callback>
    void iterate_edges(node_id node, Callback&& cb) const {
        edge::key key;
        key.source = node.value();
        key.label = 0;
        key.destination = 0;

        auto cursor = m_map.lower_bound(key);
        while (cursor) {
            edge e = cursor.get();
            if (e.source != node)
                break;

            cb(e.label, e.destination);
            cursor.move_next();
        }
    }

    // True if the node has either incoming or outgoing edges.
    bool has_edges(node_id node) const {
        edge::key key;
        key.source = node.value();
        key.destination = 0;
        key.label = 0;

        // Edges are bi-directional; one lookup is sufficient.
        auto cursor = m_map.lower_bound(key);
        return cursor && cursor.get().source == node;
    }

    // Links the two nodes together with a directed edge and the given label.
    // Returns true if the ege was actually inserted, i.e. if it didn't exist.
    bool link(node_id source, extpp::heap_reference label, node_id destination) {
        edge e;
        e.source = source;
        e.label = label;
        e.destination = destination;

        auto result = m_map.insert(e);
        if (result.inserted) {
            bool reverse_inserted = m_reverse_map.insert(reversed(e)).inserted;
            assert(reverse_inserted);
            (void) reverse_inserted;
        }
        return result.inserted;
    }

    // Removes the edge (source, label, destination) and returns true if it existed.
    bool unlink(node_id source, extpp::heap_reference label, node_id destination) {
        edge e;
        e.source = source;
        e.label = label;
        e.destination = destination;

        auto c1 = m_map.find(key(e));
        auto c2 = m_reverse_map.find(key(reversed(e)));
        if (c1 && c2) {
            c1.erase();
            c2.erase();
            return true;
        }

        // Either both or none:
        assert(c1.at_end() == c2.at_end());
        return false;
    }

    // Remove all edges that begin or end at this node.
    void remove(node_id node) {
        auto c1 = m_map.create_cursor();
        auto c2 = m_reverse_map.create_cursor();

        edge::key node_key;
        node_key.source = node.value();
        node_key.label = 0;
        node_key.destination = 0;

        c1.lower_bound(node_key);
        while (c1) {
            edge e = c1.get();
            if (e.source != node)
                break;

            c2.find(key(reversed(e)));
            assert(c2); // Must find reverse entry
            c2.erase();

            c1.move_next();
        }
    }

private:
    edge reversed(const edge& e) const {
        edge r;
        r.source = e.destination;
        r.destination = e.source;
        r.label = e.label;
        return r;
    }

    edge::key key(const edge& e) const {
        return edge::derive_key()(e);
    }

private:
    tree_type m_map, m_reverse_map;
};

// Contains one entry for every existing graph node.
class node_index {
    using tree_type = extpp::btree<node_id>;

public:
    using anchor = tree_type::anchor;

public:
    node_index(extpp::anchor_handle<anchor> anc, extpp::allocator& alloc)
        : m_tree(std::move(anc), alloc)
    {}

    template<typename Callback>
    void iterate_nodes(Callback&& cb) const {
        auto cursor = m_tree.create_cursor(m_tree.seek_min);
        for (; cursor; cursor.move_next()) {
            cb(cursor.get());
        }
    }

    uint64_t size() const { return m_tree.size(); }

    bool find(node_id id) const {
        return static_cast<bool>(m_tree.find(id));
    }

    bool insert(node_id id) {
        auto result = m_tree.insert(id);
        return result.inserted;
    }

    bool remove(node_id id) {
        auto cursor = m_tree.find(id);
        if (!cursor)
            return false;

        cursor.erase();
        return true;
    }

private:
    tree_type m_tree;
};

class database {
    struct meta_block {
        extpp::heap::anchor heap;
        extpp::id_generator::anchor ids;
        interned_strings::anchor strings;
        node_index::anchor nodes;
        property_map::anchor properties;
        edge_map::anchor edges;

        static constexpr auto get_binary_format() {
            return make_binary_format(&meta_block::heap, &meta_block::ids,
                                      &meta_block::strings, &meta_block::nodes,
                                      &meta_block::properties, &meta_block::edges);
        }
    };

    using format_type = extpp::default_file_format<meta_block>;

public:
    database(extpp::file& f, uint32_t cache_size)
        : m_format(f, cache_size)
        , m_meta(m_format.get_user_data())
        , m_heap(m_meta.member<&meta_block::heap>(), m_format.get_allocator())
        , m_ids(m_meta.member<&meta_block::ids>(), m_format.get_allocator())
        , m_strings(m_meta.member<&meta_block::strings>(), m_format.get_allocator(), m_heap)
        , m_nodes(m_meta.member<&meta_block::nodes>(), m_format.get_allocator())
        , m_properties(m_meta.member<&meta_block::properties>(), m_heap, m_format.get_allocator())
        , m_edges(m_meta.member<&meta_block::edges>(), m_format.get_allocator())
    {}

    auto& engine() { return m_format.get_engine(); }

    // Creates a new node and returns its id.
    // IDs of nodes that have been deleted can be reused.
    node_id create_node() {
        node_id id(m_ids.allocate());

        bool created = m_nodes.insert(id);
        assert(created);
        (void) created;

        return id;
    }

    // Deletes a node.
    void delete_node(node_id node, bool force) {
        if (!m_nodes.find(node))
            throw std::runtime_error("Node does not exist.");

        if (force) {
            m_edges.remove(node);
        } else {
            if (m_edges.has_edges(node))
                throw std::runtime_error("Node still has incoming or outgoing edges.");
        }

        m_properties.remove(node);
        m_nodes.remove(node);
        m_ids.free(node.value());
    }

    // Returns all properties of a node.
    std::map<std::string, std::string> list_properties(node_id node) const {
        if (!m_nodes.find(node))
            throw std::runtime_error("Node does not exist.");

        std::map<std::string, std::string> result;
        std::string key_buf, value_buf;
        m_properties.iterate_properties(node, [&](auto k, auto v) {
            load(m_heap, k, key_buf);
            load(m_heap, v, value_buf);
            result[key_buf] = value_buf;
        });

        return result;
    }

    // Returns all edges starting at this node.
    std::multimap<std::string, node_id> list_edges(node_id node) const {
        if (!m_nodes.find(node))
            throw std::runtime_error("Node does not exist.");

        std::multimap<std::string, node_id> result;
        std::string key_buf;
        m_edges.iterate_edges(node, [&](auto k, auto destination) {
            load(m_heap, k, key_buf);
            result.emplace(key_buf, destination);
        });

        return result;
    }

    // Set the property `key` of `node` to `value`.
    void set_property(node_id node, const std::string& key, const std::string& value) {
        if (!m_nodes.find(node))
            throw std::runtime_error("Node does not exist.");

        if (key.empty())
            throw std::runtime_error("Property names must not be empty.");

        extpp::heap_reference keyref = m_strings.intern(key);
        extpp::heap_reference valueref = save(m_heap, value);
        m_properties.set(node, keyref, valueref);
    }

    // Remove the property `key` from the given node.
    void unset_property(node_id node, const std::string& key) {
        if (!m_nodes.find(node))
            throw std::runtime_error("Node does not exist.");

        if (key.empty())
            throw std::runtime_error("Property names must not be empty.");

        extpp::heap_reference keyref = m_strings.find(key);
        if (!keyref)
            return; // No interned string -> no property with that name
        m_properties.remove(node, keyref);
    }

    // Creates an edge from src to dest, with the given label.
    void link_nodes(node_id src, const std::string& label, node_id dest) {
        if (!m_nodes.find(src))
            throw std::runtime_error("Source node does not exist.");

        if (!m_nodes.find(dest))
            throw std::runtime_error("Destination node does not exist.");

        if (label.empty())
            throw std::runtime_error("Edge labels must not be empty.");

        extpp::heap_reference labelref = m_strings.intern(label);
        m_edges.link(src, labelref, dest);
    }

    // Deletes the labeled edge between src and dest.
    void unlink_nodes(node_id src, const std::string& label, node_id dest) {
        if (m_nodes.find(src))
            throw std::runtime_error("Source node does not exist.");

        if (!m_nodes.find(dest))
            throw std::runtime_error("Destination node does not exist.");

        if (label.empty())
            throw std::runtime_error("Edge labels must not be empty.");

        extpp::heap_reference labelref = m_strings.find(label);
        if (!labelref)
            return; // No interned string -> no edge with that label
        m_edges.unlink(src, labelref, dest);
    }

    // Returns the IDs of all existing nodes.
    std::vector<node_id> list_nodes() const {
        std::vector<node_id> nodes;
        nodes.reserve(m_nodes.size());

        m_nodes.iterate_nodes([&](node_id id) {
            nodes.push_back(id);
        });
        return nodes;
    }

    void debug_print(std::ostream& o) {
        o << "Allocator state:\n";
        m_format.get_allocator().dump(o);
        o << "\n";

        o << "Heap state:\n";
        m_heap.dump(o);
    }

    void flush() { m_format.flush(); }

private:
    format_type m_format;

    extpp::anchor_handle<meta_block> m_meta;

    // Data storage (only strings right now).
    extpp::heap m_heap;

    // Generates unique node ids.
    extpp::id_generator m_ids;

    // Indexes existing interned string instances.
    interned_strings m_strings;

    // Stores node entries.
    node_index m_nodes;

    // Stores the properties of a node.
    property_map m_properties;

    // Stores the graph edges.
    edge_map m_edges;
};

} // namespace example

enum class subcommand {
    create,
    delete_,
    set,
    unset,
    link,
    unlink,
    print,
    print_all,
    debug
};

struct settings {
    std::string file;
    uint32_t cache_size = 8; // Megabyte
    subcommand cmd = subcommand::print_all;
    bool print_stats = false;

    struct delete_t {
        uint64_t node = 0;
        bool force = false;
    } delete_;

    struct set_t {
        uint64_t node = 0;
        std::string name;
        std::string value;
    } set;

    struct unset_t {
        std::uint64_t node = 0;
        std::string name;
    } unset;

    struct link_t {
        uint64_t source = 0;
        uint64_t destination = 0;
        std::string label;
    } link;

    struct unlink_t {
        uint64_t source = 0;
        uint64_t destination = 0;
        std::string label;
    } unlink;

    struct print_t {
        uint64_t node = 0;
    } print;

    struct gc_t {
        bool compact = false;
    } gc;
};

void parse(settings& s, int argc, char** argv) {
    using namespace clipp;

    bool show_help = false;

    auto help = option("-h", "--help").set(show_help, true);

    auto general = "General options:" % (
        (required("-f", "--file") & value("file", s.file))          % "database file",
        (option("-m", "--cache-size") & value("M", s.cache_size))   % ("cache size in megabyte (default: " + std::to_string(s.cache_size) + " MB)"),
        (option("--stats").set(s.print_stats)                       % "print statistics after command execution."),
        parameter(help)                                             % "display this text"
    );

    auto cmd_create = command("create").set(s.cmd, subcommand::create);

    auto cmd_delete = (
        command("delete").set(s.cmd, subcommand::delete_),
        "Subcommand delete:" % group(
            value("node", s.delete_.node)                       % "node id",
            option("-f", "--force").set(s.delete_.force, true)  % "force deletion (removes all edges first)"
        )
    );

    auto cmd_set = (
        command("set").set(s.cmd, subcommand::set),
        "Subcommand set:" % (
            value("node", s.set.node)                   % "node id",
            value("name", s.set.name)                   % "property name",
            value("value", s.set.value)                 % "property value"
        )
    );

    auto cmd_unset = (
        command("unset").set(s.cmd, subcommand::unset),
        "Subcommand unset:" % (
            value("node", s.unset.node)                 % "node id",
            value("name", s.unset.name)                 % "property name"
        )
    );

    auto cmd_link = (
        command("link").set(s.cmd, subcommand::link),
        "Subcommand link:" % (
            value("source", s.link.source)              % "source node id",
            value("dest", s.link.destination)           % "destination source id",
            value("label", s.link.label)                % "edge label"
        )
    );

    auto cmd_unlink = (
        command("unlink").set(s.cmd, subcommand::unlink),
        "Subcommand link:" % (
            value("source", s.unlink.source)            % "source node id",
            value("dest", s.unlink.destination)         % "destination source id",
            value("label", s.unlink.label)              % "edge label"
        )
    );

    auto cmd_print = (
        command("print").set(s.cmd, subcommand::print),
        "Subcommand print:" % group(
            value("node", s.print.node)                 % "node id"
        )
    );

    auto cmd_print_all = command("print-all").set(s.cmd, subcommand::print_all);

    auto cmd_debug = command("debug").set(s.cmd, subcommand::debug);

    auto cli = (general, (
            cmd_create  | cmd_delete
        |   cmd_set     | cmd_unset
        |   cmd_link    | cmd_unlink
        |   cmd_print   | cmd_print_all
        |   cmd_debug
    )) | help;

    auto print_usage = [&](bool include_help){
        auto fmt = doc_formatting()
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

int main(int argc, char** argv) {
    settings s;
    parse(s, argc, argv);

    // The number of blocks cached in memory.
    const uint32_t cache_blocks = (uint64_t(s.cache_size) * uint64_t(1 << 20)) / objectdb::block_size;

    auto file = extpp::system_vfs().open(s.file.c_str(), extpp::vfs::read_write, extpp::vfs::open_create);
    objectdb::database db(*file, cache_blocks);

    auto print_node = [&](objectdb::node_id node) {
        std::cout << "Node: " << node.value() << "\n";

        auto props = db.list_properties(node);
        std::cout << "Properties: " << (props.empty() ? "None\n" : "\n");
        for (const auto& [key, value] : props) {
            std::cout << "    " << key << ": " << value << "\n";
        }

        auto edges = db.list_edges(node);
        std::cout << "Edges: " << (edges.empty() ? "None\n" : "\n");
        for (const auto& [label, node] : edges) {
            std::cout << "    " << label << ": " << node.value() << "\n";
        }
    };

    try {
        switch (s.cmd) {
        case subcommand::create:
        {
            auto node = db.create_node();
            std::cout << "New node: " << node.value() << std::endl;
            break;
        }
        case subcommand::delete_:
        {
            db.delete_node(objectdb::node_id(s.delete_.node), s.delete_.force);
            break;
        }
        case subcommand::set:
        {
            db.set_property(objectdb::node_id(s.set.node), s.set.name, s.set.value);
            break;
        }
        case subcommand::unset:
        {
            db.unset_property(objectdb::node_id(s.unset.node), s.unset.name);
            break;
        }
        case subcommand::link:
        {
            db.link_nodes(objectdb::node_id(s.link.source), s.link.label, objectdb::node_id(s.link.destination));
            break;
        }
        case subcommand::unlink:
        {
            db.unlink_nodes(objectdb::node_id(s.unlink.source), s.unlink.label, objectdb::node_id(s.unlink.destination));
            break;
        }
        case subcommand::print:
        {
            print_node(objectdb::node_id(s.print.node));
            break;
        }
        case subcommand::print_all:
        {
            auto nodes = db.list_nodes();
            for (auto node : nodes) {
                print_node(node);
                std::cout << "\n";
            }
            break;
        }
        case subcommand::debug:
            db.debug_print(std::cout);
            break;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        std::exit(1);
    }

    db.flush();
    if (s.print_stats) {
        extpp::file_engine_stats stats = db.engine().stats();
        std::cout << "\n"
                  << "I/O statistics:\n"
                  << "  Reads:      " << stats.reads << "\n"
                  << "  Writes:     " << stats.writes << "\n"
                  << "  Cache hits: " << stats.cache_hits << "\n"
                  << std::flush;
    }

} // namespace objectbd
