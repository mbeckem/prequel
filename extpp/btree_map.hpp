#ifndef EXTPP_BTREE_MAP_HPP
#define EXTPP_BTREE_MAP_HPP

#include <extpp/btree.hpp>
#include <extpp/defs.hpp>

#include <functional>

namespace extpp {

template<typename Key, typename T, u32 BlockSize, typename Compare = std::less<Key>>
class btree_map {
public:
    static constexpr u32 block_size = BlockSize;

    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, Value>;
    using key_compare = Compare;

private:
    struct key_extract {
        const key_type& operator()(const value_type& v) const {
            return v.first;
        }
    };

    using tree_type = btree<value_type, key_extract, key_compare, block_size>;

public:
    using anchor = typename tree_type::anchor;

    using size_type = typename tree_type::size_type;
    using difference_Type = typename tree_type::difference_type;

    using iterator = typename tree_type::iterator;
    using const_iterator = typename iterator;

public:
    btree_map(block_data<anchor> anc, block_allocator& alloc, Compare comp = Compare())
        : m_tree(std::move(anc), alloc, key_extract(), std::move(comp)) {}

    iterator begin() const { return m_tree.begin(); }
    iterator end() const { return m_tree.end(); }

    bool empty() const { return m_tree.empty(); }
    size_type size() const { return m_tree.size(); }

    void clear() { m_tree.free(); }
    std::pair<iterator, bool> insert(const value_type& value) { return m_tree.insert(value); }

    template<typename Operation>
    void modify(const iterator& pos, Operation&& op) {
        // TODO: In this case, m_tree.modify() could be an unchecked operation
        // because the key will never change.
        m_tree.modify(pos, [&](value_type& v) {
            op(v.second);
        });
    }

    // TODO
    iterator erase();

    size_type count(const key_type& key) const { return find(key) != end(); }
    iterator find(const key_type& key) const { return m_tree.find(key); }

    std::pair<iterator, iterator> equal_range(const key_type& key) const { return m_tree.equal_range(key); }
    iterator lower_bound(const key_type& key) const { return m_tree.lower_bound(key); }
    iterator upper_bound(const key_type& key) const { return m_tree.upper_bound(key); }

private:
    tree_type m_tree;
};

} // namespace extpp

#endif // EXTPP_BTREE_MAP_HPP
