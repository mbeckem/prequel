#ifndef EXTPP_BTREE_STATE_HPP
#define EXTPP_BTREE_STATE_HPP

#include <extpp/address.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/allocator.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/btree/base.hpp>
#include <extpp/btree/node.hpp>

namespace extpp::btree_detail {

template<typename Value, typename KeyExtract, typename KeyCompare, u32 BlockSize>
class state : public uses_allocator {
public:
    static constexpr u32 block_size = BlockSize;

    using value_type = Value;
    using key_type = std::decay_t<std::result_of_t<KeyExtract(Value)>>;

    using node_address = raw_address;

    using leaf_type = leaf_node<state>;
    using leaf_address = typename leaf_type::address_type;

    using internal_type = internal_node<state>;
    using internal_address = typename internal_type::address_type;

    struct anchor {
        /// The number of entries in this tree.
        u64 size = 0;

        /// The number of leaf nodes in this tree.
        u64 leaves = 0;

        /// The number of internal nodes in this tree.
        u32 internals = 0;

        /// The height of this tree.
        /// - 0: empty (no nodes)
        /// - 1: root is a leaf with at least one value
        /// - > 1: root is an internal node with at least one key and two children
        u32 height = 0;

        /// Points to the root node (if any).
        node_address root;

        /// Points to the leftmost leaf (if any).
        leaf_address leftmost;

        /// Points to the rightmost leaf (if any).
        leaf_address rightmost;
    };

public:
    key_type key(const value_type& v) const { return m_key_extract(v); }

    bool key_less(const key_type& a, const key_type& b) const {
        return m_key_compare(a, b);
    }

    bool key_equal(const key_type& a, const key_type& b) const {
        return !key_less(a, b) && !key_less(b, a); // TODO SFINAE ==
    }

    bool key_greater(const key_type& a, const key_type& b) const {
        return key_less(b, a);
    }

    u64 size() const { return m_anchor->size; }
    void set_size(u64 size) {
        m_anchor->size = size;
        m_anchor.dirty();
    }

    u32 height() const { return m_anchor->height; }
    void set_height(u32 height) {
        m_anchor->height = height;
        m_anchor.dirty();
    }

    raw_address allocate_internal() {
        auto addr = this->get_allocator().allocate(1);
        m_anchor->internals++;
        m_anchor.dirty();
        return addr;
    }

    raw_address allocate_leaf() {
        auto addr = this->get_allocator().allocate(1);
        m_anchor->leaves++;
        m_anchor.dirty();
        return addr;
    }

    void free(internal_address addr) {
        this->get_allocator().free(addr);
        m_anchor->internals--;
        m_anchor.dirty();
    }

    void free(leaf_address addr) {
        this->get_allocator().free(addr);
        m_anchor->leaves--;
        m_anchor.dirty();
    }

    const anchor_ptr<anchor>& get_anchor() const { return m_anchor; }

    leaf_address cast_leaf(const node_address& node) const {
        return static_cast<leaf_address>(node);
    }

    internal_address cast_internal(const node_address& node) const {
        return static_cast<internal_address>(node);
    }

    leaf_type access(leaf_address addr) const { return extpp::access(this->get_engine(), addr); }
    internal_type access(internal_address addr) const { return extpp::access(this->get_engine(), addr); }

public:
    state(anchor_ptr<anchor> anc, allocator& alloc,
          KeyExtract key_extract, KeyCompare key_compare)
        : state::uses_allocator(alloc, BlockSize)
        , m_anchor(std::move(anc))
        , m_key_extract(std::move(key_extract))
        , m_key_compare(std::move(key_compare))
    {}

private:
    anchor_ptr<anchor> m_anchor;
    KeyExtract m_key_extract;
    KeyCompare m_key_compare;
};

} // namespace extpp::btree_detail

#endif // EXTPP_BTREE_STATE_HPP
