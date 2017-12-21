#include <extpp/btree.hpp>
#include <extpp/default_file_format.hpp>
#include <extpp/id_generator.hpp>
#include <extpp/heap.hpp>
#include <clipp.h>

#include <cassert>
#include <cstdint>
#include <functional>
#include <iostream>
#include <tuple>

namespace example {

// Nodes are represented by a unique id.
struct node_id {
    node_id() = default;
    explicit node_id(std::uint64_t value)
        : m_value(value) {}

    std::uint64_t value() const { return m_value; }

    bool valid() const { return m_value != 0; }
    explicit operator bool() const { return valid(); }

    bool operator==(const node_id& other) const {
        return m_value == other.m_value;
    }

    bool operator<(const node_id& other) const {
        return m_value < other.m_value;
    }

private:
    std::uint64_t m_value = 0;
};

static constexpr std::uint32_t block_size = 4096;

using engine = extpp::engine<block_size>;
using allocator = extpp::allocator<block_size>;

// Storage for data (strings and so forth).
using heap_type = extpp::heap<block_size>;

inline const extpp::type_index string_type(1);
inline const extpp::type_index interned_string_type(2);

class interned_strings {
    // If a string is interned, it can be looked up using its
    // hash and contents. Only one copy of such a string exists
    // in the entire database. This saves space for frequently used
    // strings and makes comparisons faster (interned strings can be compared
    // by checking their references for equality).
    struct entry {
        std::uint64_t hash = 0;     // The hash of the string.
        extpp::reference string;    // Points to the data.

        struct key {
            // Field to make records unique, duplicate hashes are possible.
            std::uint64_t hash, unique;

            bool operator<(const key& other) const {
                return std::tie(hash, unique) < std::tie(other.hash, other.unique);
            }

            bool operator==(const key& other) const {
                return hash == other.hash && unique == other.unique;
            }
        };

        struct key_extract {
            key operator()(const entry& s) const {
                return key{s.hash, s.string.value()};
            }
        };
    };

    // Contains one entry for every interned string.
    // Note that this table contains *weak* references, i.e.
    // references to strings in this tree do not count towards
    // the root set. Entries are removed whenever the garbage collector
    // destroys as string.
    using tree_type = extpp::btree<entry, entry::key_extract, std::less<>, block_size>;

public:
    using anchor = typename tree_type::anchor;

public:
    interned_strings(extpp::anchor_ptr<anchor> anc, allocator& alloc, heap_type& storage)
        : m_tree(std::move(anc), alloc)
        , m_storage(storage)
    {}

    // Returns the reference to the interned copy of that string, if it exists.
    extpp::reference find(const std::string& str) const {
        return find(str, hash(str));
    }

    // Interns the given string. Either returns a reference to some existing copy
    // of that string or inserts a new copy into the heap.
    extpp::reference intern(const std::string& str) {
        const auto h = hash(str);
        if (auto ref = find(str, h))
            return ref;

        entry ent;
        ent.hash = h;
        ent.string = m_storage.insert(interned_string_type, str.data(), str.size());

        auto [pos, inserted] = m_tree.insert(ent);
        assert(inserted); // Entry must be unique, even when hashes collide.
        (void) pos;
        (void) inserted;
        return ent.string;
    }

    // Called when an interned string is garbage collected. Removes the corresponding
    // entry from the index.
    void remove(extpp::reference ref) {
        assert(m_storage.type(ref) == interned_string_type);

        std::string data;
        m_storage.load(ref, data);

        entry ent;
        ent.hash = hash(data);
        ent.string = ref;

        bool found = m_tree.erase(m_tree.key(ent));
        assert(found);
        (void) found;
    }

private:
    extpp::reference find(const std::string& str, std::uint64_t hash) const {
        entry::key k;
        k.hash = hash;
        k.unique = 0;

        // Loop over all collisions. We have to test the real strings for equality.
        std::string value;
        for (auto pos = m_tree.lower_bound(k); pos != m_tree.end() && pos->hash == hash; ++pos) {
            assert(m_storage.type(pos->string) == interned_string_type);
            m_storage.load(pos->string, value);
            if (value == str)
                return pos->string;
        }
        return {};
    }

    std::uint64_t hash(const std::string& str) const {
        // There are better hash functions, but this will do now for.
        return std::hash<std::string>()(str);
    }

private:
    tree_type m_tree;
    heap_type& m_storage;
};

class property_map {
    struct property {
        node_id node;           // owner of the property
        extpp::reference name;  // name of the property (string, interned)
        extpp::reference value; // value of the property (string)

        struct key {
            // Derived from the fields above.
            std::uint64_t node, name;

            bool operator<(const key& other) const {
                return std::tie(node, name) < std::tie(other.node, other.name);
            }

            bool operator==(const key& other) const {
                return node == other.node && name == other.name;
            }
        };

        struct key_extract {
            key operator()(const property& p) const {
                return key{p.node.value(), p.name.value()};
            }
        };
    };

    // Contains one entry for every node's property.
    using tree_type = extpp::btree<property, property::key_extract, std::less<>, block_size>;

public:
    using anchor = tree_type::anchor;

public:
    property_map(extpp::anchor_ptr<anchor> anc, allocator& alloc)
        : m_tree(std::move(anc), alloc)
    {}

    template<typename Callback>
    void iterate_properties(node_id node, Callback&& cb) const {
        auto [begin, end] = node_range(node);
        for (; begin != end; ++begin)
            cb(begin->name, begin->value);
    }

    // Returns the value of the property `name` in the given node.
    // Returns a null reference if no such property exists.
    extpp::reference get(node_id node, extpp::reference name) const {
        property::key k;
        k.node = node.value();
        k.name = name.value();

        auto pos = m_tree.lower_bound(k);
        if (pos == m_tree.end())
            return {};

        if (pos->node == node && pos->name == name)
            return pos->value;
        return {};
    }

    // Sets the property `name` of the given node to `value`.
    // Returns the old value of that property (if any).
    extpp::reference set(node_id node, extpp::reference name, extpp::reference value) {
        property p;
        p.node = node;
        p.name = name;
        p.value = value;

        auto [pos, inserted] = m_tree.insert(p);
        if (!inserted) {
            // The property already existed.
            extpp::reference prev = pos->value;
            m_tree.replace(pos, p);
            return prev;
        }
        // No previous value.
        return {};
    }

    // Remove a single property from a node. Returns the previous value of that property (if any).
    extpp::reference remove(node_id node, extpp::reference name) {
        property::key k;
        k.node = node.value();
        k.name = name.value();

        auto pos = m_tree.lower_bound(k);
        if (pos == m_tree.end())
            return {};

        if (pos->node == node && pos->name == name) {
            extpp::reference value = pos->value;
            m_tree.erase(pos);
            return value;
        }
        return {};
    }

    // Remove all properties for that node.
    void remove(node_id node) {
        auto [begin, end] = node_range(node);
        m_tree.erase(begin, end);
    }

    // For garbage collection.
    // Calls the visitor for every reference known to this object.
    template<typename T>
    void visit_references(T&& visitor) {
        for (const auto& prop : m_tree) {
            visitor(prop.name);
            visitor(prop.value);
        }
    }

private:
    std::tuple<tree_type::iterator, tree_type::iterator>
    node_range(node_id node) const
    {
        property::key lower, upper;

        lower.node = node.value();
        lower.name = 0;

        upper.node = node.value();
        upper.name = std::uint64_t(-1);
        return std::tuple(m_tree.lower_bound(lower),
                          m_tree.upper_bound(upper));
    }

private:
    tree_type m_tree;
};

class edge_map {
    struct edge {
        node_id source;
        extpp::reference label; // Interned string.
        node_id destination;

        struct key {
            // Derived from the fields above.
            std::uint64_t source, label, destination;

            bool operator<(const key& other) const {
                return std::tie(source, label, destination)
                        < std::tie(other.source, other.label, other.destination);
            }

            bool operator==(const key& other) const {
                return source == other.source
                        && label == other.label
                        && destination == other.destination;
            }
        };

        struct key_extract {
            key operator()(const edge& e) const {
                return key{e.source.value(), e.label.value(), e.destination.value()};
            }
        };
    };

    // Contains one entry for every node's property.
    using tree_type = extpp::btree<edge, edge::key_extract, std::less<>, block_size>;

public:
    class anchor {
        tree_type::anchor map, reverse_map;
        friend class edge_map;
    };

public:
    edge_map(extpp::anchor_ptr<anchor> anc, allocator& alloc)
        : m_map(anc.member(&anchor::map), alloc)
        , m_reverse_map(anc.member(&anchor::reverse_map), alloc)
    {}

    template<typename Callback>
    void iterate_edges(node_id node, Callback&& cb) const {
        auto [begin, end] = node_range(m_map, node);
        for (; begin != end; ++begin)
            cb(begin->label, begin->destination);
    }

    // True if the node has either incoming or outgoing edges.
    bool has_edges(node_id node) const {
        auto out_edges = node_range(m_map, node);
        auto in_edges = node_range(m_reverse_map, node);
        return out_edges.first != out_edges.second || in_edges.first != in_edges.second;
    }

    // Links the two nodes together with a directed edge and the given label.
    // Returns true if the ege was actually inserted, i.e. if it didn't exist.
    bool link(node_id source, extpp::reference label, node_id destination) {
        edge e;
        e.source = source;
        e.label = label;
        e.destination = destination;

        bool inserted;
        std::tie(std::ignore, inserted) = m_map.insert(e);
        if (inserted) {
            std::tie(std::ignore, inserted) = m_reverse_map.insert(reversed(e));
            assert(inserted);
        }
        return inserted;
    }

    // Removes the edge (source, label, destination) and returns true if it existed.
    bool unlink(node_id source, extpp::reference label, node_id destination) {
        edge e;
        e.source = source;
        e.label = label;
        e.destination = destination;

        if (m_map.erase(key(e))) {
            bool removed = m_reverse_map.erase(key(reversed(e)));
            assert(removed); // The reversed entry must have existed.
            (void) removed;
            return true;
        }
        return false;
    }

    // Remove all edges that begin or end at this node.
    void remove(node_id node) {
        auto rm_edges = [&](tree_type& fwd, tree_type& bwd) {
            auto [begin, end] = node_range(fwd, node);
            for (auto i = begin; i != end; ++i) {
                bool removed = bwd.erase(key(reversed(*i)));
                assert(removed); // The reversed entry must have existed.
                (void) removed;
            }
            fwd.erase(begin, end);
        };

        rm_edges(m_map, m_reverse_map);
        rm_edges(m_reverse_map, m_map);
    }

    // For garbage collection.
    // Calls the visitor for every reference known to this object.
    template<typename T>
    void visit_references(T&& visitor) {
        // No need to visit reverse map because it contains the same references.
        for (const auto& e : m_map) {
            visitor(e.label);
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
        return edge::key_extract()(e);
    }

    std::pair<tree_type::iterator, tree_type::iterator>
    node_range(const tree_type& map, node_id node) const
    {
        edge::key lower, upper;

        lower.source = node.value();
        lower.label = 0;
        lower.destination = 0;

        upper.source = node.value();
        upper.label = std::uint64_t(-1);
        upper.destination = std::uint64_t(-1);
        return std::pair(map.lower_bound(lower),
                         map.upper_bound(upper));
    }


private:
    tree_type m_map, m_reverse_map;
};

// Contains one entry for every existing graph node.
class node_index {
    struct node {
        node_id id;
    };

    struct key_extract {
        node_id operator()(const node& n) const {
            return n.id;
        }
    };

    using tree_type = extpp::btree<node, key_extract, std::less<>, block_size>;

public:
    using iterator = tree_type::iterator;
    using cursor = tree_type::cursor;

    using anchor = tree_type::anchor;

public:
    node_index(extpp::anchor_ptr<anchor> anc, allocator& alloc)
        : m_tree(std::move(anc), alloc)
    {}

    std::uint64_t size() const { return m_tree.size(); }

    iterator begin() const { return m_tree.begin(); }
    iterator end() const { return m_tree.end(); }

    iterator find(node_id id) const {
        return m_tree.find(id);
    }

    bool insert(node_id id) {
        node n;
        n.id = id;

        auto [pos, inserted] = m_tree.insert(n);
        (void) pos;
        return inserted;
    }

    bool remove(node_id id) {
        return m_tree.erase(id);
    }

private:
    tree_type m_tree;
};

using id_generator = extpp::id_generator<std::uint64_t, block_size>;

class database {
    struct meta_block {
        heap_type::anchor heap;
        id_generator::anchor ids;
        interned_strings::anchor strings;
        node_index::anchor nodes;
        property_map::anchor properties;
        edge_map::anchor edges;
    };

    using format_type = extpp::default_file_format<meta_block, block_size>;

public:
    database(extpp::file& f, std::uint32_t cache_size)
        : m_format(f, cache_size)
        , m_meta(m_format.user_data())
        , m_heap(m_meta.member(&meta_block::heap), m_format.get_allocator())
        , m_ids(m_meta.member(&meta_block::ids), m_format.get_allocator())
        , m_strings(m_meta.member(&meta_block::strings), m_format.get_allocator(), m_heap)
        , m_nodes(m_meta.member(&meta_block::nodes), m_format.get_allocator())
        , m_properties(m_meta.member(&meta_block::properties), m_format.get_allocator())
        , m_edges(m_meta.member(&meta_block::edges), m_format.get_allocator())
    {
        register_heap_types();
    }

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
        auto pos = m_nodes.find(node);
        if (pos == m_nodes.end())
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
        auto node_pos = m_nodes.find(node);
        if (node_pos == m_nodes.end())
            throw std::runtime_error("Node does not exist.");

        std::map<std::string, std::string> result;
        std::string key_buf, value_buf;
        m_properties.iterate_properties(node, [&](auto k, auto v) {
            m_heap.load(k, key_buf);
            m_heap.load(v, value_buf);
            result[key_buf] = value_buf;
        });

        return result;
    }

    // Returns all edges starting at this node.
    std::multimap<std::string, node_id> list_edges(node_id node) const {
        auto node_pos = m_nodes.find(node);
        if (node_pos == m_nodes.end())
            throw std::runtime_error("Node does not exist.");

        std::multimap<std::string, node_id> result;
        std::string key_buf;
        m_edges.iterate_edges(node, [&](auto k, auto destination) {
            m_heap.load(k, key_buf);
            result.emplace(key_buf, destination);
        });

        return result;
    }

    // Set the property `key` of `node` to `value`.
    void set_property(node_id node, const std::string& key, const std::string& value) {
        auto node_pos = m_nodes.find(node);
        if (node_pos == m_nodes.end())
            throw std::runtime_error("Node does not exist.");

        if (key.empty())
            throw std::runtime_error("Property names must not be empty.");

        // TODO: Intern small values as well?
        extpp::reference keyref = m_strings.intern(key);
        extpp::reference valueref = m_heap.insert(string_type, value.data(), value.size());
        m_properties.set(node, keyref, valueref);
    }

    // Remove the property `key` from the given node.
    void unset_property(node_id node, const std::string& key) {
        auto node_pos = m_nodes.find(node);
        if (node_pos == m_nodes.end())
            throw std::runtime_error("Node does not exist.");

        if (key.empty())
            throw std::runtime_error("Property names must not be empty.");

        extpp::reference keyref = m_strings.find(key);
        if (!keyref)
            return; // No interned string -> no property with that name
        m_properties.remove(node, keyref);
    }

    // Creates an edge from src to dest, with the given label.
    void link_nodes(node_id src, const std::string& label, node_id dest) {
        auto src_pos = m_nodes.find(src);
        if (src_pos == m_nodes.end())
            throw std::runtime_error("Source node does not exist.");

        auto dest_pos = m_nodes.find(dest);
        if (dest_pos == m_nodes.end())
            throw std::runtime_error("Destination node does not exist.");

        if (label.empty())
            throw std::runtime_error("Edge labels must not be empty.");

        extpp::reference labelref = m_strings.intern(label);
        m_edges.link(src, labelref, dest);
    }

    // Deletes the labeled edge between src and dest.
    void unlink_nodes(node_id src, const std::string& label, node_id dest) {
        auto src_pos = m_nodes.find(src);
        if (src_pos == m_nodes.end())
            throw std::runtime_error("Source node does not exist.");

        auto dest_pos = m_nodes.find(dest);
        if (dest_pos == m_nodes.end())
            throw std::runtime_error("Destination node does not exist.");

        if (label.empty())
            throw std::runtime_error("Edge labels must not be empty.");

        extpp::reference labelref = m_strings.find(label);
        if (!labelref)
            return; // No interned string -> no edge with that label
        m_edges.unlink(src, labelref, dest);
    }

    // Returns the IDs of all existing nodes.
    std::vector<node_id> list_nodes() const {
        std::vector<node_id> nodes;
        nodes.reserve(m_nodes.size());

        for (const auto& node : m_nodes) {
            nodes.push_back(node.id);
        }
        return nodes;
    }

    void debug_print(std::ostream& o) {
        o << "Allocator state:\n";
        m_format.get_allocator().debug_print(o);
        o << "\n";

        o << "Heap state:\n";
        m_heap.debug_print(o);
    }

    void gc(bool compact) {
        if (compact) {
            collect_impl(m_heap.begin_compaction());
        } else {
            collect_impl(m_heap.begin_collection());
        }
    }

    void flush() { m_format.flush(); }

private:
    void register_heap_types() {
        extpp::type_info string;
        string.index = string_type;
        string.dynamic_size = true;
        string.contains_references = false;

        extpp::type_info interned_string;
        interned_string.index = interned_string_type;
        interned_string.dynamic_size = true;
        interned_string.contains_references = false;
        interned_string.finalizer = [this](extpp::reference ref) {
            // Called when the heap's garbage collector destroys a no longer
            // referenced interned string. Makes sure that the string
            // is no longer referenced from the (weak) index.
            m_strings.remove(ref);
        };

        m_heap.register_type(string);
        m_heap.register_type(interned_string);
    }

    template<typename Collector>
    void collect_impl(Collector&& c) {
        // Visit all root references before starting the garbage collection.
        // We do not visit the `m_strings` index because its a set of weak
        // references that get cleaned up by the finalizer of the interned_string type.
        auto vis = [&](auto ref) {
            c.visit(ref);
        };

        m_properties.visit_references(vis);
        m_edges.visit_references(vis);
        c();
    }

private:
    format_type m_format;
    extpp::handle<meta_block, block_size> m_meta;

    // Data storage (only strings right now).
    heap_type m_heap;

    // Generates unique node ids.
    id_generator m_ids;

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
    gc,
    debug
};

struct settings {
    std::string file;
    std::uint32_t cache_size = 128;
    subcommand cmd = subcommand::print_all;
    bool print_stats = false;

    struct delete_t {
        std::uint64_t node = 0;
        bool force = false;
    } delete_;

    struct set_t {
        std::uint64_t node = 0;
        std::string name;
        std::string value;
    } set;

    struct unset_t {
        std::uint64_t node = 0;
        std::string name;
    } unset;

    struct link_t {
        std::uint64_t source = 0;
        std::uint64_t destination = 0;
        std::string label;
    } link;

    struct unlink_t {
        std::uint64_t source = 0;
        std::uint64_t destination = 0;
        std::string label;
    } unlink;

    struct print_t {
        std::uint64_t node = 0;
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
        (option("-m", "--cache-size") & value("M", s.cache_size))   % ("cache size in blocks (default: " + std::to_string(s.cache_size) + ")"),
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

    auto cmd_gc = (
        command("gc").set(s.cmd, subcommand::gc),
        "Subcommand gc:" % group(
            option("--compact").set(s.gc.compact, true) % "perform compaction"
        )
    );
    auto cmd_debug = command("debug").set(s.cmd, subcommand::debug);

    auto cli = (general, (
            cmd_create  | cmd_delete
        |   cmd_set     | cmd_unset
        |   cmd_link    | cmd_unlink
        |   cmd_print   | cmd_print_all
        |   cmd_gc      | cmd_debug
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

    auto file = extpp::system_vfs().open(s.file.c_str(), extpp::vfs::read_write, extpp::vfs::open_create);
    example::database db(*file, s.cache_size);

    auto print_node = [&](example::node_id node) {
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
            db.delete_node(example::node_id(s.delete_.node), s.delete_.force);
            break;
        }
        case subcommand::set:
        {
            db.set_property(example::node_id(s.set.node), s.set.name, s.set.value);
            break;
        }
        case subcommand::unset:
        {
            db.unset_property(example::node_id(s.unset.node), s.unset.name);
            break;
        }
        case subcommand::link:
        {
            db.link_nodes(example::node_id(s.link.source), s.link.label, example::node_id(s.link.destination));
            break;
        }
        case subcommand::unlink:
        {
            db.unlink_nodes(example::node_id(s.unlink.source), s.unlink.label, example::node_id(s.unlink.destination));
            break;
        }
        case subcommand::print:
        {
            print_node(example::node_id(s.print.node));
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
        case subcommand::gc:
        {
            db.gc(s.gc.compact);
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
        extpp::engine_stats stats = db.engine().stats();
        std::cout << "\n"
                  << "I/O statistics:\n"
                  << "  Reads:      " << stats.reads << "\n"
                  << "  Writes:     " << stats.writes << "\n"
                  << "  Cache hits: " << stats.cache_hits << "\n"
                  << std::flush;
    }

}
