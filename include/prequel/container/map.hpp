#ifndef PREQUEL_CONTAINER_MAP_HPP
#define PREQUEL_CONTAINER_MAP_HPP

#include <prequel/btree.hpp>

namespace prequel {

template<typename Key, typename Value, typename KeyLess = std::less<>>
class map {
public:
    using mapped_type = Value;

    using key_type = Key;

    struct entry {
        Key key;
        Value value;

        entry() = default;

        entry(Key key, Value value)
            : key(std::move(key))
            , value(std::move(value)) {}

        static constexpr auto get_binary_format() {
            return binary_format(&entry::key, &entry::value);
        }
    };

    using value_type = entry;

private:
    struct derive_key {
        Key operator()(const entry& e) const { return e.key; }
    };

    // Implemented as a btree.
    using tree_type = btree<entry, derive_key, KeyLess>;

public:
    class anchor {
        typename tree_type::anchor tree;

        static constexpr auto get_binary_format() { return binary_format(&anchor::tree); }

        friend map;
        friend binary_format_access;
    };

public:
    explicit map(anchor_handle<anchor> anchor_, allocator& alloc_, KeyLess less_ = KeyLess())
        : m_tree(std::move(anchor_).template member<&anchor::tree>(), alloc_, std::move(less_)) {}

    engine& get_engine() const { return m_tree.get_engine(); }
    allocator& get_allocator() const { return m_tree.get_allocator(); }

private:
    tree_type m_tree;
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_MAP_HPP
