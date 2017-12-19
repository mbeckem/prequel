#ifndef EXTPP_SET_HPP
#define EXTPP_SET_HPP

#include <extpp/btree.hpp>
#include <extpp/defs.hpp>

namespace extpp {

template<typename Key, u32 BlockSize, typename Compare = std::less<Key>>
class set {
public:
    static constexpr u32 block_size = BlockSize;

    using key_type = Key;
    using value_type = Key;
    using key_compare = Compare;

private:
    struct key_extract {
        const key_type operator()(const key_type& k) const {
            return k;
        }
    };

public:
    using tree_type = btree<key_type, key_extract, key_compare, block_size>;
    using anchor = typename tree_type::anchor;
    using size_type = typename tree_type::size_type;
    using iterator = typename tree_type::iterator;
    using const_iterator = iterator;
    using cursor = typename tree_type::cursor;

public:
    set(handle<anchor, BlockSize> anc, allocator<BlockSize>& alloc, Compare comp = Compare())
        : m_tree(std::move(anc), alloc, key_extract(), std::move(comp)) {}

    allocator<BlockSize>& get_allocator() const { return m_tree.get_allocator(); }
    engine<BlockSize>& get_engine() const { return m_tree.get_engine(); }
    const tree_type& tree() const { return m_tree; }

    iterator begin() const { return m_tree.begin(); }
    iterator end() const { return m_tree.end(); }

    bool empty() const { return m_tree.empty(); }
    size_type size() const { return m_tree.size(); }

    iterator lower_bound(const key_type& key) const { return m_tree.lower_bound(key); }
    iterator upper_bound(const key_type& key) const { return m_tree.upper_bound(key); }
    std::pair<iterator, iterator> equal_range(const key_type& key) const { return m_tree.equal_range(key); }

    iterator find(const key_type& key) const { return m_tree.find(key); }
    size_type count(const key_type& key) const { return find(key) != end(); }

    void clear() { m_tree.clear(); }
    std::pair<iterator, bool> insert(const value_type& value) { return m_tree.insert(value); }
    bool erase(const key_type& key) { return m_tree.erase(key); }
    iterator erase(const iterator& pos) { return m_tree.erase(pos); }

private:
    tree_type m_tree;
};

} // namespace extpp

#endif // EXTPP_SET_HPP
