#ifndef EXTPP_LIST_HPP
#define EXTPP_LIST_HPP

#include <extpp/raw_list.hpp>
#include <extpp/serialization.hpp>

#include <fmt/ostream.h>

namespace extpp {

template<typename T>
class list {
public:
    using value_type = T;
    using size_type = u64;

public:
    class anchor {
        raw_list::anchor list;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::list);
        }

        friend class list;
        friend binary_format_access;
    };

public:
    class cursor {
        raw_list::cursor inner;

    private:
        friend class list;

        cursor(raw_list::cursor&& inner): inner(std::move(inner)) {}

    public:
        cursor() = default;

        bool invalid() const { return inner.at_end(); }
        explicit operator bool() const { return static_cast<bool>(inner); }

        bool erased() const { return inner.erased(); }

        void move_first() { inner.move_first(); }
        void move_last() { inner.move_last(); }
        void move_next() { inner.move_next(); }
        void move_prev() { inner.move_prev(); }

        void erase() { inner.erase(); }

        void insert_before(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.insert_before(buffer.data());
        }

        void insert_after(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.insert_after(buffer.data());
        }

        value_type get() const {
            return deserialized_value<value_type>(inner.get(), value_size());
        }

        void set(const value_type& value) {
            auto buffer = serialized_value(value);
            inner.set(buffer.data());
        }

        const raw_list::cursor& raw() const { return inner; }
    };

public:
    using cursor_seek_t = raw_list::cursor_seek_t;

    static constexpr cursor_seek_t seek_none = raw_list::seek_none;
    static constexpr cursor_seek_t seek_first = raw_list::seek_first;
    static constexpr cursor_seek_t seek_last = raw_list::seek_last;

public:
    explicit list(handle<anchor> anchor_, allocator& alloc_)
        : inner(std::move(anchor_).template member<&anchor::list>(), value_size(), alloc_)
    {}

public:
    engine& get_engine() const { return inner.get_engine(); }
    allocator& get_allocator() const { return inner.get_allocator(); }

    static constexpr u32 value_size() { return serialized_size<T>(); }
    u32 node_capacity() const { return inner.node_capacity(); }

    bool empty() const { return inner.empty(); }
    u64 size() const { return inner.size(); }
    u64 nodes() const { return inner.nodes(); }
    double fill_factor() const { return inner.fill_factor(); }
    u64 byte_size() const { return inner.byte_size(); }
    double overhead() const { return inner.overhead(); }

    cursor create_cursor(cursor_seek_t seek = seek_none) const {
        return cursor(inner.create_cursor(seek));
    }

    void push_front(const T& value) {
        auto buffer = serialized_value(value);
        inner.push_front(buffer.data());
    }

    void push_back(const T& value) {
        auto buffer = serialized_value(value);
        inner.push_back(buffer.data());
    }

    void reset() { inner.reset(); }
    void clear() { inner.clear(); }
    void pop_front() { inner.pop_front(); }
    void pop_back() { inner.pop_back(); }

    const raw_list& raw() const { return inner; }

public:
    class node_view final {
    public:
        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        block_index address() const { return m_inner.address(); }
        block_index next_address() const { return m_inner.next_address(); }
        block_index prev_address() const { return m_inner.prev_address(); }

        u32 value_count() const { return m_inner.value_count(); }
        value_type value(u32 index) const { return deserialized_value<value_type>(m_inner.value(index)); }

    private:
        friend class list;

        node_view(const raw_list::node_view& nv)
            : m_inner(nv) {}

    private:
        const raw_list::node_view& m_inner;
    };

    template<typename Func>
    void visit(Func&& fn) const {
        inner.visit([&](const raw_list::node_view& raw_view) -> bool {
            node_view typed_view(raw_view);
            return fn(typed_view);
        });
    }

    void dump(std::ostream& os) const;

private:
    raw_list inner;
};

template<typename T>
void list<T>::dump(std::ostream& os) const {
    fmt::print(
        "List:\n"
        "  Value size: {}\n"
        "  Block size: {}\n"
        "  Node Capacity: {}\n"
        "  Size: {}\n"
        "  Nodes: {}\n"
        "\n",
        value_size(),
        get_engine().block_size(),
        node_capacity(),
        size(),
        nodes());

    if (!empty())
        os << "\n";

    visit([&](const node_view& node) -> bool{
        fmt::print(
            "  Node @{}:\n"
            "    Previous: @{}\n"
            "    Next: @{}\n"
            "    Size: {}\n",
            node.address(),
            node.prev_address(),
            node.next_address(),
            node.value_count());

        u32 size = node.value_count();
        for (u32 i = 0; i < size; ++i) {
            fmt::print("    {:>4}: {}\n", i, node.value(i));
        }
        fmt::print("\n");
        return true;
    });
}

} // namespace extpp

#endif // EXTPP_LIST_HPP
