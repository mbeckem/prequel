#ifndef DATABASE_HPP
#define DATABASE_HPP

#include "./fixed_string.hpp"

#include <prequel/hash_table.hpp>
#include <prequel/heap.hpp>
#include <prequel/serialization.hpp>

#include <optional>

namespace keyvaluedb {

// Allocate and free a string on disk.
prequel::heap::reference allocate_string(prequel::heap& h, const std::string& value);
void free_string(prequel::heap& h, prequel::heap::reference ref);

// Load string from disk by dereferencing the argument.
std::string load_string(const prequel::heap& h, prequel::heap::reference ref);

// Returns the string hash value.
inline uint64_t string_hash(const char* data, size_t size) {
    return prequel::fnv_1a(reinterpret_cast<const unsigned char*>(data), size);
}

inline uint64_t string_hash(const std::string& str) {
    return string_hash(str.data(), str.size());
}

/*
 * The hash table entry (string key and value).
 *
 * A database key is either a long string or a short string (up to 23 bytes).
 * Long strings are allocated on the heap, while short strings can be inlined
 * into the hash table itself.
 */
struct database_entry {
public:
    // Short strings contain their storage directly.
    using short_key_t = fixed_string<23>;

    struct long_key_t {
        // Reference to the string storage in the heap.
        prequel::heap::reference ref;

        // Cached hash of the long string referenced here.
        uint64_t hash = 0;

        static constexpr auto get_binary_format() {
            return prequel::binary_format(&long_key_t::hash, &long_key_t::ref);
        }
    };

    using key_t = std::variant<short_key_t, long_key_t>;

public:
    /* Keys are either inlined or on the heap */
    key_t key;

    /* Values are always on the heap right now */
    prequel::heap::reference value;

public:
    // Returns the key by which entries of the hash table are being compared.
    struct derive_key {
        key_t operator()(const database_entry& entry) const { return entry.key; }
    };

    // Returns the hash value for a given key.
    struct key_hash {
        uint64_t operator()(const key_t& key) const {
            return std::visit([&](const auto& k) { return hash_value(k); }, key);
        }

    private:
        uint64_t hash_value(const short_key_t& key) const {
            return string_hash(key.data(), key.size());
        }

        uint64_t hash_value(const long_key_t& key) const {
            return key.hash; /* cached at creation time */
        }
    };

    // Compares two keys for equality. Needs to access the heap when it
    // has to compare long strings.
    struct key_equal {
        const prequel::heap& heap;
        key_equal(const prequel::heap& heap_)
            : heap(heap_) {}

        bool operator()(const key_t& lhs, const key_t& rhs) const {
            return std::visit(
                [&](const auto& l, const auto& r) {
                    if constexpr (!std::is_same_v<decltype(l), decltype(r)>) {
                        return false;
                    }
                    return ranges_equal(unwrap_key(l), unwrap_key(r));
                },
                lhs, rhs);
        }

        bool operator()(const std::string& lhs, const key_t& rhs) const {
            return std::visit([&](const auto& r) { return ranges_equal(lhs, unwrap_key(r)); }, rhs);
        }

        bool operator()(const key_t& lhs, const std::string& rhs) const {
            return (*this)(rhs, lhs);
        }

    private:
        // Returns the string as-is (as a reference).
        const database_entry::short_key_t& unwrap_key(const database_entry::short_key_t& str) const {
            return str;
        }

        // Dereferences the long string reference and returns the content.
        std::string unwrap_key(const database_entry::long_key_t& str) const {
            return load_string(heap, str.ref);
        }

        template<typename Range1, typename Range2>
        bool ranges_equal(Range1&& r1, Range2&& r2) const {
            return std::equal(r1.begin(), r1.end(), r2.begin(), r2.end());
        }
    };

    static constexpr auto get_binary_format() {
        return prequel::binary_format(&database_entry::key, &database_entry::value);
    }
};

static_assert(prequel::serialized_size<database_entry>() == 32);

class database {
private:
    /*
     * Hash table of key/value-pairs, indexed by the key using the above hash and equals functions.
     */
    using table_t = prequel::hash_table<database_entry, database_entry::derive_key,
                                        database_entry::key_hash, database_entry::key_equal>;

public:
    class anchor {
        prequel::heap::anchor strings;
        table_t::anchor table;

        friend database;
        friend prequel::binary_format_access;

        static constexpr auto get_binary_format() { return prequel::binary_format(&anchor::table); }
    };

public:
    database(prequel::anchor_handle<anchor> anchor_, prequel::allocator& alloc);

    /*
     * Searches for the given key without retrieving the value from disk.
     */
    bool contains(const std::string& key) const;

    /*
     * Searches for the given key and returns the associated value on success.
     */
    std::optional<std::string> find(const std::string& key) const;

    /*
     * Inserts the key+value pair into the database if the key
     * does not already exist.
     * Returns true if the insertion was successful.
     */
    bool insert(const std::string& key, const std::string& value);

    /*
     * Erases the key+value pair associated with this key from the database.
     * Returns true if the key existed.
     */
    bool erase(const std::string& key);

    /*
     * Returns the number of key+value pairs in the database.
     */
    uint64_t size() const;

    /*
     * Returns the size of the database on disk, in bytes.
     * This includes preallocated storage.
     */
    uint64_t byte_size() const;

    /*
     * Checks the internal structure of the database.
     */
    void validate() const;

    /*
     * Dumps debugging information into the provided output stream.
     */
    void dump(std::ostream& os) const;

    database(const database&) = delete;
    database& operator=(const database&) = delete;

private:
    /*
     * For hash table searches. Contains a reference to the string data
     * and the precomputed hash value (so the string is only hashed once).
     */
    struct search_key {
        const std::string& data;
        const uint64_t hash;

        search_key(const std::string& data_, uint64_t hash_)
            : data(data_)
            , hash(hash_) {}
    };

    struct search_key_hash {
        search_key_hash() = default;

        uint64_t operator()(const search_key& key) const noexcept { return key.hash; }
    };

    struct search_key_equals {
        const database* db;

        search_key_equals(const database* db_)
            : db(db_) {}

        bool operator()(const search_key& lhs, const database_entry::key_t& rhs) const {
            return database_entry::key_equal(db->m_strings)(lhs.data, rhs);
        }
    };

private:
    // Storage for long strings.
    prequel::heap m_strings;

    // Maps string keys to string values.
    table_t m_table;
};

} // namespace keyvaluedb

#endif // DATABASE_HPP
