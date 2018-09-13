#ifndef EXTPP_BTREE_HPP
#define EXTPP_BTREE_HPP

#include <extpp/raw_btree.hpp>
#include <extpp/serialization.hpp>

#include <fmt/ostream.h>

#include <memory>
#include <ostream>

namespace extpp {

// TODO: Move somewhere better.
struct identity_t {
    template<typename T>
    T operator()(const T& value) const { return value; }
};

template<typename Value, typename DeriveKey = identity_t, typename KeyLess = std::less<>>
class btree {
public:
    using value_type = Value;
    using key_type = std::decay_t<std::result_of_t<DeriveKey(Value)>>;

public:
    class anchor {
        raw_btree::anchor tree;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::tree);
        }

        friend class btree;
        friend class binary_format_access;
    };

public:
    class cursor {
        raw_btree::cursor inner;

    private:
        friend class btree;

        cursor(raw_btree::cursor&& inner): inner(std::move(inner)) {}

    public:
        cursor() = default;

        bool at_end() const { return inner.at_end(); }
        bool erased() const { return inner.erased(); }
        explicit operator bool() const { return static_cast<bool>(inner); }

        void reset() { inner.reset(); }
        void move_min() { inner.move_min(); }
        void move_max() { inner.move_max(); }
        void move_next() { inner.move_next(); }
        void move_prev() { inner.move_prev(); }

        bool lower_bound(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.lower_bound(buffer.data());
        }

        bool upper_bound(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.upper_bound(buffer.data());
        }

        bool find(const key_type& key) {
            auto buffer = serialized_value(key);
            return inner.find(buffer.data());
        }

        bool insert(const value_type& value) {
            auto buffer = serialized_value(value);
            return inner.insert(buffer.data());
        }

        bool insert_or_update(const value_type& value) {
            auto buffer = serialized_value(value);
            return inner.insert_or_update(buffer.data());
        }

        void erase() { inner.erase(); }

        value_type get() const { return deserialized_value<value_type>(inner.get()); }

        void set(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.set(buffer.data());
        }

        void validate() const { inner.validate(); }

        bool operator==(const cursor& other) const { return inner == other.inner; }
        bool operator!=(const cursor& other) const { return inner != other.inner; }
    };

    class loader {
        raw_btree::loader inner;

    private:
        friend class btree;

        loader(raw_btree::loader&& inner): inner(std::move(inner)) {}

    public:
        loader() = delete;
        loader(loader&&) noexcept = default;
        loader& operator=(loader&&) noexcept = default;

        void insert(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.insert(buffer.data());
        }

        template<typename InputIter>
        void insert(const InputIter& begin, const InputIter& end) {
            // TODO: Test whether batching here is worth it.
            for (auto i = begin; i != end; ++i) {
                insert(*i);
            }
        }

        void finish() { inner.finish(); }
        void discard() { inner.discard(); }
    };

public:
    using cursor_seek_t = raw_btree::cursor_seek_t;

    static constexpr cursor_seek_t seek_none = raw_btree::seek_none;
    static constexpr cursor_seek_t seek_min = raw_btree::seek_min;
    static constexpr cursor_seek_t seek_max = raw_btree::seek_max;

    struct insert_result {
        cursor position;
        bool inserted = false;

        insert_result() = default;

        insert_result(cursor position, bool inserted)
            : position(std::move(position))
            , inserted(inserted)
        {}
    };

public:
    explicit btree(anchor_handle<anchor> anchor_, allocator& alloc_, DeriveKey derive_key = DeriveKey(), KeyLess less = KeyLess())
        : m_state(std::make_unique<state_t>(std::move(derive_key), std::move(less)))
        , m_inner(std::move(anchor_).template member<&anchor::tree>(), make_options(), alloc_)
    {}

    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    /// \name Tree size
    ///
    /// \{

    static constexpr u32 value_size() { return serialized_size<value_type>(); }
    static constexpr u32 key_size() { return serialized_size<key_type>(); }

    u32 internal_node_capacity() const { return m_inner.internal_node_capacity(); }
    u32 leaf_node_capacity() const { return m_inner.leaf_node_capacity(); }

    bool empty() const { return m_inner.empty(); }
    u64 size() const { return m_inner.size(); }
    u32 height() const { return m_inner.height(); }
    u64 internal_nodes() const { return m_inner.internal_nodes(); }
    u64 leaf_nodes() const { return m_inner.leaf_nodes(); }
    u64 nodes() const { return m_inner.nodes(); }

    double fill_factor() const { return m_inner.fill_factor(); }
    u64 byte_size() const { return m_inner.byte_size(); }
    double overhead() const { return m_inner.overhead(); }

    /// \}

    /// Derives the key from the given value by applying the DeriveKey function.
    key_type derive_key(const value_type& value) const {
        return m_state->derive(value);
    }

    /// Compares the two keys and returns true iff `lhs < rhs`, as specified
    /// by the KeyLess function.
    bool key_less(const key_type& lhs, const key_type& rhs) const {
        return m_state->less(lhs, rhs);
    }

    cursor create_cursor(cursor_seek_t seek = seek_none) const {
        return cursor(m_inner.create_cursor(seek));
    }

    cursor find(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.find(buffer.data()));
    }

    cursor lower_bound(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.lower_bound(buffer.data()));
    }

    cursor upper_bound(const key_type& key) const {
        auto buffer = serialized_value(key);
        return cursor(m_inner.upper_bound(buffer.data()));
    }

    insert_result insert(const value_type& value) {
        auto buffer = serialized_value(value);
        auto result = m_inner.insert(buffer.data());
        return insert_result(cursor(std::move(result.position)), result.inserted);
    }

    insert_result insert_or_update(const value_type& value) {
        auto buffer = serialized_value(value);
        auto result = m_inner.insert_or_update(buffer.data());
        return insert_result(cursor(std::move(result.position)), result.inserted);
    }

    loader bulk_load() {
        return loader(m_inner.bulk_load());
    }

    void reset() { m_inner.reset(); }
    void clear() { m_inner.clear(); }

public:
    class node_view final {
    public:
        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        bool is_leaf() const { return m_inner.is_leaf(); }
        bool is_internal() const { return m_inner.is_internal(); }

        u32 level() const { return m_inner.level(); }
        block_index address() const { return m_inner.address(); }
        block_index parent_address() const { return m_inner.parent_address(); }

        // For internal nodes.
        u32 child_count() const { return m_inner.child_count(); }
        u32 key_count() const { return m_inner.key_count(); }
        key_type key(u32 index) const { return deserialized_value<key_type>(m_inner.key(index)); }
        block_index child(u32 index) const { return m_inner.child(index); }

        // For leaf nodes.
        u32 value_count() const { return m_inner.value_count(); }
        value_type value(u32 index) const { return deserialized_value<value_type>(m_inner.value(index)); }

    private:
        friend class btree;

        node_view(const raw_btree::node_view& nv)
            : m_inner(nv) {}

    private:
        const raw_btree::node_view& m_inner;
    };

    template<typename Func>
    void visit(Func&& fn) const {
        m_inner.visit([&](const raw_btree::node_view& raw_view) -> bool {
            node_view typed_view(raw_view);
            return fn(typed_view);
        });
    }

    void dump(std::ostream& os);

    void validate() const { m_inner.validate(); }

    const raw_btree& raw() const { return m_inner; }

private:
    raw_btree_options make_options() {
        raw_btree_options options;
        options.value_size = value_size();
        options.key_size = key_size();
        options.user_data = m_state.get();
        options.derive_key = derive_key;
        options.key_less = key_less;
        return options;
    }

    static void derive_key(const byte* value_buffer, byte* key_buffer, void* user_data) {
        const state_t* state = reinterpret_cast<const state_t*>(user_data);
        value_type value = deserialized_value<value_type>(value_buffer);
        serialize(state->derive(value), key_buffer);
    }

    static bool key_less(const byte* lhs_buffer, const byte* rhs_buffer, void* user_data) {
        const state_t* state = reinterpret_cast<const state_t*>(user_data);
        key_type lhs = deserialized_value<key_type>(lhs_buffer);
        key_type rhs = deserialized_value<key_type>(rhs_buffer);
        return state->less(lhs, rhs);
    }

private:
    // Allocated on the heap for stable addresses (user data pointer in raw_btree).
    struct state_t {
        DeriveKey m_derive;
        KeyLess m_less;

        state_t(DeriveKey&& derive_key, KeyLess&& less)
            : m_derive(std::move(derive_key))
            , m_less(std::move(less))
        {}

        bool less(const key_type& a, const key_type& b) const {
            return m_less(a, b);
        }

        key_type derive(const value_type& v) const {
            return m_derive(v);
        }
    };

private:
    std::unique_ptr<state_t> m_state;
    raw_btree m_inner;
};

template<typename T, typename DeriveKey, typename KeyLess>
void btree<T, DeriveKey, KeyLess>::dump(std::ostream& os) {
    fmt::print(os,
       "Btree:\n"
       "  Value size: {}\n"
       "  Key size: {}\n"
       "  Internal node capacity: {}\n"
       "  Leaf node capacity: {}\n"
       "  Height: {}\n"
       "  Size: {}\n"
       "  Internal nodes: {}\n"
       "  Leaf nodes: {}\n",
       value_size(), key_size(),
       internal_node_capacity(), leaf_node_capacity(),
       height(), size(), internal_nodes(), leaf_nodes());

    if (!empty())
        os << "\n";

    auto visitor = [&](const node_view& node) {
        if (node.is_internal()) {
            fmt::print(os,
                       "Internal node @{}:\n"
                       "  Parent: @{}\n"
                       "  Level: {}\n"
                       "  Children: {}\n",
                       node.address(), node.parent_address(),
                       node.level(), node.child_count());

            const u32 child_count = node.child_count();
            for (u32 i = 0; i < child_count - 1; ++i) {
                fmt::print(os, "  {}: @{} (<= {})\n",
                           i, node.child(i), node.key(i));
            }
            fmt::print(os, "  {}: @{}\n", child_count - 1, node.child(child_count - 1));
        } else {
            const u32 size = node.value_count();
            fmt::print(os,
                       "Leaf node @{}:\n"
                       "  Parent: @{}\n"
                       "  Values: {}\n",
                       node.address(), node.parent_address(), size);
            for (u32 i = 0; i < size; ++i) {
                fmt::print(os, "  {}: {}\n", i, node.value(i));
            }
        }
        return true;
    };
    visit(visitor);
}

} // namespace extpp

#endif // EXTPP_BTREE_HPP
