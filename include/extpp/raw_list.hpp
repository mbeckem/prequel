#ifndef EXTPP_RAW_LIST_HPP
#define EXTPP_RAW_LIST_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/handle.hpp>

#include <memory>
#include <ostream>

namespace extpp {

class raw_list;
class raw_list_impl;
class raw_list_cursor;
class raw_list_cursor_impl;

class raw_list_anchor {
    /// The number of values in this list.
    u64 size = 0;

    /// The number of list nodes (== blocks).
    u64 nodes = 0;

    /// The index of the first node, or invalid if empty.
    block_index first;

    /// The index of the last node, or invalid if empty.
    block_index last;

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_list_anchor::size, &raw_list_anchor::nodes,
                                  &raw_list_anchor::first, &raw_list_anchor::last);
    }

    friend class raw_list_impl;
    friend class binary_format_access;
};

class raw_list
{
public:
    using anchor = raw_list_anchor;
    using cursor = raw_list_cursor;

public:
    enum cursor_seek_t {
        seek_none = 0,
        seek_first = 1,
        seek_last = 2
    };

public:
    explicit raw_list(handle<anchor> _anchor, u32 value_size, allocator& _alloc);
    ~raw_list();

    raw_list(const raw_list&) = delete;
    raw_list& operator=(const raw_list&) = delete;

    raw_list(raw_list&&) noexcept;
    raw_list& operator=(raw_list&&) noexcept;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    /// \name List size
    ///
    /// \{

    /// Returns the size (in bytes) of every value in the list.
    u32 value_size() const;

    /// Returns the maximum number of values per list node.
    u32 node_capacity() const;

    /// Returns true if the list is empty.
    bool empty() const;

    /// Returns the number of items in the list.
    u64 size() const;

    /// Returns the number of nodes in the list.
    u64 nodes() const;

    /// The average fullness of this list's nodes.
    double fill_factor() const;

    /// The size of this datastructure in bytes (not including the anchor).
    u64 byte_size() const;

    /// The relative overhead compared to a linear file filled with the same entries.
    /// Computed by dividing the total size of the list by the total size of its elements.
    ///
    /// \note Because nodes are at worst only half full, this value should never be
    /// much greater than 2.
    double overhead() const;

    /// \}

    /// Creates a new cursor associated with this list.
    /// The cursor is initially invalid and has to be moved to some element first,
    /// unless `seek_first` or `seek_last` are specified, in which case
    /// the cursor will attempt to move to the first/last element.
    cursor create_cursor(cursor_seek_t seek = seek_none) const;

    /// Inserts a new element at the beginning of the list.
    /// The value must be `value_size()` bytes long.
    void push_front(const byte* value);

    /// Inserts a new element at the end of the list.
    /// The value must be `value_size()` bytes long.
    void push_back(const byte* value);

    /// Removes all data from this list.
    /// The list will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

    /// Removes all elements from the list.
    /// After clear() has returned, the list will not hold any nodes.
    /// \post `empty()`.
    void clear();

    /// Removes the first element from the list.
    void pop_front();

    /// Removes the last element from the list.
    void pop_back();

    // TODO: validate

public:
    class node_view {
    public:
        node_view() = default;
        virtual ~node_view();

        node_view(const node_view&) = delete;
        node_view& operator=(const node_view&) = delete;

    public:
        virtual block_index address() const = 0;
        virtual block_index next_address() const = 0;
        virtual block_index prev_address() const = 0;

        virtual u32 value_count() const = 0;
        virtual const byte* value(u32 index) const = 0;
    };

    /// Visits internal node from begin to end.
    /// The function will be invoked for every node until it returns false, at which point the iteration
    /// through the list will stop.
    /// The list must not be modified during this operation.
    void visit(bool (*visit_fn)(const node_view& node, void* user_data), void* user_data = nullptr) const;

    template<typename Func>
    void visit(Func&& fn) const {
        using func_t = std::remove_reference_t<Func>;

        bool (*visit_fn)(const node_view&, void*) = [](const node_view& node, void* user_data) -> bool {
            func_t* fn = reinterpret_cast<func_t*>(user_data);
            return (*fn)(node);
        };
        visit(visit_fn, reinterpret_cast<void*>(std::addressof(fn)));
    }

    void dump(std::ostream& os) const;

private:
    raw_list_impl& impl() const;

private:
    std::unique_ptr<raw_list_impl> m_impl;
};

class raw_list_cursor {
public:
    raw_list_cursor();
    raw_list_cursor(const raw_list_cursor&);
    raw_list_cursor(raw_list_cursor&&) noexcept;
    ~raw_list_cursor();

    raw_list_cursor& operator=(const raw_list_cursor&);
    raw_list_cursor& operator=(raw_list_cursor&&) noexcept;

    void move_first();
    void move_last();
    void move_next();
    void move_prev();

    void erase();
    void insert_before(const byte* data);
    void insert_after(const byte* data);

    const byte* get() const;
    void set(const byte* data);
    u32 value_size() const;

    // Returns true if the cursor has become invalid (by iterating
    // past the end or the beginning).
    bool at_end() const;

    // Returns true if the cursor's current list element has been erased.
    // Iterators to erased elements have to be moved before they can be useful again.
    bool erased() const;

    explicit operator bool() const { return !at_end(); }

private:
    friend class raw_list;

    raw_list_cursor(std::unique_ptr<raw_list_cursor_impl> impl);

private:
    raw_list_cursor_impl& impl() const;

private:
    std::unique_ptr<raw_list_cursor_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_RAW_LIST_HPP
