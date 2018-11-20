#include "./database.hpp"

#include <fmt/format.h>
#include <fmt/ostream.h>

namespace keyvaluedb {

prequel::heap::reference allocate_string(prequel::heap& h, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max())
        throw std::invalid_argument("Value is too large.");
    return h.allocate(reinterpret_cast<const unsigned char*>(value.data()), value.size());
}

std::string load_string(const prequel::heap& h, prequel::heap::reference ref) {
    const uint32_t size = h.size(ref);

    std::string str(size, '\0');
    h.load(ref, reinterpret_cast<unsigned char*>(&str[0]), str.size());
    return str;
}

void free_string(prequel::heap& h, prequel::heap::reference ref) {
    h.free(ref);
}

static std::string load_string(const prequel::heap& h, const database_entry::key_t& key) {
    struct visitor {
        const prequel::heap* h;

        std::string operator()(const database_entry::short_key_t& k) const {
            return std::string(k.begin(), k.end());
        }

        std::string operator()(const database_entry::long_key_t& k) const {
            return load_string(*h, k.ref);
        }
    };

    return std::visit(visitor{&h}, key);
}

database::database(prequel::anchor_handle<anchor> anchor_, prequel::allocator& alloc)
    : m_strings(anchor_.member<&anchor::strings>(), alloc)
    , m_table(anchor_.member<&anchor::table>(), alloc, database_entry::derive_key(),
              database_entry::key_hash(), database_entry::key_equal(m_strings)) {}

bool database::contains(const std::string& key) const {
    database_entry ignored;
    return m_table.find_compatible(search_key(key, string_hash(key)), search_key_hash(),
                                   search_key_equals(this), ignored);
}

std::optional<std::string> database::find(const std::string& key) const {
    database_entry ent;
    if (!m_table.find_compatible(search_key(key, string_hash(key)), search_key_hash(),
                                 search_key_equals(this), ent)) {
        return {};
    }

    return std::optional(load_string(m_strings, ent.value));
}

/*
 * This function does two lookups: once to determine whether the key exists,
 * and once to insert it into the table. The second lookup is unneccessary in theory,
 * but we currently lack the facilities to create the value on demand if it does not exist.
 *
 * (We dont want to create a key+value pair on disk on every insertion beforehand,
 * because that would be completely stupid if the key already existed, therefore the contains()
 * will a compatible in-memory key before the real insertion).
 */
bool database::insert(const std::string& key, const std::string& value) {
    if (contains(key))
        return false;

    if (std::find(key.begin(), key.end(), 0) != key.end())
        throw std::runtime_error("Keys cannot contain zero bytes.");

    database_entry new_ent;
    if (key.size() <= database_entry::short_key_t::max_size) {
        new_ent.key = database_entry::short_key_t(std::string_view(key));
    } else {
        new_ent.key = database_entry::long_key_t{allocate_string(m_strings, key), string_hash(key)};
    }
    new_ent.value = allocate_string(m_strings, value);

    // The insertion must succeed here because we checked contains() at the top.
    if (!m_table.insert(new_ent))
        throw std::logic_error("Insertion failed.");

    return true;
}

bool database::erase(const std::string& key) {
    return m_table.erase_compatible(search_key(key, string_hash(key)), search_key_hash(),
                                    search_key_equals(this));
}

uint64_t database::size() const {
    return m_table.size();
}

uint64_t database::byte_size() const {
    return m_strings.byte_size() + m_table.byte_size();
}

void database::validate() const {
    m_table.validate();
    m_strings.validate();
}

void database::dump(std::ostream& os) const {
    fmt::print(os,
               "Database:\n"
               "  Entries:     {}\n"
               "  Byte size:   {}\n"
               "\n",
               size(), byte_size());

    fmt::print(os,
               "Hash-Table stats:\n"
               "  Entries:             {}\n"
               "  Allocated buckets:   {}\n"
               "  Primary buckets:     {}\n"
               "  Overflow buckets:    {}\n"
               "  Fill factor:         {}\n"
               "  Overhead:            {}\n"
               "  Byte size:           {}\n"
               "\n",
               m_table.size(), m_table.allocated_buckets(), m_table.primary_buckets(),
               m_table.overflow_buckets(), m_table.fill_factor(), m_table.overhead(),
               m_table.byte_size());

    fmt::print(os,
               "Heap stats:\n"
               "  Objects count:   {}\n"
               "  Objects size:    {}\n"
               "  Byte size:       {}\n"
               "\n",
               m_strings.objects_count(), m_strings.objects_size(), m_strings.byte_size());

    fmt::print(os, "Entries:\n");
    m_table.iterate([&](const database_entry& entry) {
        std::string key = load_string(m_strings, entry.key);
        std::string value = load_string(m_strings, entry.value);
        fmt::print(os, "  {} -> {}\n", key, value);

        return prequel::iteration_control::next;
    });
}

} // namespace keyvaluedb
