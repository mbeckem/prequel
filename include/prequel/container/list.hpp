#ifndef PREQUEL_CONTAINER_LIST_HPP
#define PREQUEL_CONTAINER_LIST_HPP

#include <prequel/container/raw_list.hpp>
#include <prequel/serialization.hpp>

#include <fmt/ostream.h>

namespace prequel {

/**
 * A list of fixed-size values implemented using linked blocks.
 */
template<typename T>
class list {
public:
    using value_type = T;
    using size_type = u64;

public:
    class anchor {
        raw_list::anchor list;

        static constexpr auto get_binary_format() { return binary_format(&anchor::list); }

        friend class list;
        friend binary_format_access;
    };

public:
    /// Cursors are used to traverse the values in a list.
    class cursor {
    public:
        cursor() = default;

        /// Returns the size of a serialized value.
        static constexpr u32 value_size() { return list::value_size(); }

        /// Returns the current value.
        value_type get() const { return deserialize<value_type>(inner.get()); }

        /// Replaces the current value with the given argument.
        void set(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            inner.set(buffer.data());
        }

        /// Returns true if the cursor has become invalid (by iterating
        /// past the end or the beginning).
        bool at_end() const { return inner.at_end(); }

        /// Returns true if the cursor's current list element has been erased.
        /// Iterators to erased elements have to be moved before they can be useful again.
        bool erased() const { return inner.erased(); }

        //// Equivalent to `!at_end()`.
        explicit operator bool() const { return static_cast<bool>(inner); }

        /// Moves this cursor to the first value in the list.
        void move_first() { inner.move_first(); }

        /// Moves this cursor to the last value in the list.
        void move_last() { inner.move_last(); }

        /// Moves this cursor the the next value.
        void move_next() { inner.move_next(); }

        /// Moves this cursor to the previous value.
        void move_prev() { inner.move_prev(); }

        /// Inserts the new value *before* the current list element.
        /// The cursor must point to a valid element.
        void insert_before(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            inner.insert_before(buffer.data());
        }

        /// Inserts the new value *after* the current list element.
        /// The cursor must point to a valid element.
        void insert_after(const value_type& value) {
            auto buffer = serialize_to_buffer(value);
            inner.insert_after(buffer.data());
        }

        /// Erases the element that this cursors points at.
        /// In order for this to work, the cursor must not be at the end and must not
        /// already point at an erased element.
        void erase() { inner.erase(); }

        const raw_list::cursor& raw() const { return inner; }

    private:
        friend class list;

        cursor(raw_list::cursor&& inner_)
            : inner(std::move(inner_)) {}

    private:
        raw_list::cursor inner;
    };

public:
    using cursor_seek_t = raw_list::cursor_seek_t;

    static constexpr cursor_seek_t seek_none = raw_list::seek_none;
    static constexpr cursor_seek_t seek_first = raw_list::seek_first;
    static constexpr cursor_seek_t seek_last = raw_list::seek_last;

public:
    explicit list(anchor_handle<anchor> anchor_, allocator& alloc_)
        : inner(std::move(anchor_).template member<&anchor::list>(), value_size(), alloc_) {}

public:
    engine& get_engine() const { return inner.get_engine(); }
    allocator& get_allocator() const { return inner.get_allocator(); }

    /// Returns the size of a value.
    static constexpr u32 value_size() { return serialized_size<T>(); }

    /// Returns the maximum number of values per list node.
    u32 node_capacity() const { return inner.node_capacity(); }

    /// Returns true if the list is empty.
    bool empty() const { return inner.empty(); }

    /// Returns the number of items in the list.
    u64 size() const { return inner.size(); }

    /// Returns the number of nodes in the list.
    u64 nodes() const { return inner.nodes(); }

    /// The average fullness of this list's nodes.
    double fill_factor() const { return inner.fill_factor(); }

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const { return inner.byte_size(); }

    /// The relative overhead compared to a linear file filled with the same entries.
    /// Computed by dividing the total size of the list by the total size of its elements.
    ///
    /// \note Because nodes are at worst only half full, this value should never be
    /// much greater than 2.
    double overhead() const { return inner.overhead(); }

    /// Creates a new cursor associated with this list.
    /// The cursor is initially invalid and has to be moved to some element first,
    /// unless `seek_first` or `seek_last` are specified, in which case
    /// the cursor will attempt to move to the first/last element.
    cursor create_cursor(cursor_seek_t seek = seek_none) const {
        return cursor(inner.create_cursor(seek));
    }

    /// Inserts a new element at the beginning of the list.
    void push_front(const T& value) {
        auto buffer = serialize_to_buffer(value);
        inner.push_front(buffer.data());
    }

    /// Inserts a new element at the end of the list.
    void push_back(const T& value) {
        auto buffer = serialize_to_buffer(value);
        inner.push_back(buffer.data());
    }

    /// Removes the first element from the list.
    void pop_front() { inner.pop_front(); }

    /// Removes the last element from the list.
    void pop_back() { inner.pop_back(); }

    /// Removes all elements from the list.
    /// After clear() has returned, the list will not hold any nodes.
    /// \post `empty()`.
    void clear() { inner.clear(); }

    /// Removes all data from this list.
    /// The list will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset() { inner.reset(); }

    /// Returns a reference to the raw list instance.
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
        value_type value(u32 index) const { return deserialize<value_type>(m_inner.value(index)); }

    private:
        friend class list;

        node_view(const raw_list::node_view& nv)
            : m_inner(nv) {}

    private:
        const raw_list::node_view& m_inner;
    };

    /// Visits every node from begin to end.
    /// The function will be invoked for every node until it returns false, at which point the iteration
    /// through the list will stop.
    /// The list must not be modified during this operation.
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
        value_size(), get_engine().block_size(), node_capacity(), size(), nodes());

    if (!empty())
        os << "\n";

    visit([&](const node_view& node) -> bool {
        fmt::print(
            "  Node @{}:\n"
            "    Previous: @{}\n"
            "    Next: @{}\n"
            "    Size: {}\n",
            node.address(), node.prev_address(), node.next_address(), node.value_count());

        u32 size = node.value_count();
        for (u32 i = 0; i < size; ++i) {
            fmt::print("    {:>4}: {}\n", i, node.value(i));
        }
        fmt::print("\n");
        return true;
    });
}

} // namespace prequel

#endif // PREQUEL_CONTAINER_LIST_HPP
