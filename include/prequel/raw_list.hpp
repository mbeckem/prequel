#ifndef PREQUEL_RAW_LIST_HPP
#define PREQUEL_RAW_LIST_HPP

#include <prequel/allocator.hpp>
#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/block_index.hpp>
#include <prequel/defs.hpp>
#include <prequel/handle.hpp>

#include <memory>
#include <ostream>

namespace prequel {

class raw_list;
class raw_list_cursor;

namespace detail {

struct raw_list_anchor {
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
};

class raw_list_impl;
class raw_list_cursor_impl;

} // namespace detail

using raw_list_anchor = detail::raw_list_anchor;

/**
 * A list of fixed-size values implemented using linked blocks.
 * The size of values can be determined at runtime (e.g. through user input)
 * but must remain constant during the use of an array.
 */
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
    /**
     * Loads a raw list instance rooted at the existing anchor.
     * `value_size` and `_alloc` must be equivalent every time the list is loaded.
     */
    explicit raw_list(anchor_handle<anchor> _anchor, u32 value_size, allocator& _alloc);
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

    /// Returns the size of a value.
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

    /// Removes the first element from the list.
    void pop_front();

    /// Removes the last element from the list.
    void pop_back();

    /// Removes all elements from the list.
    /// After clear() has returned, the list will not hold any nodes.
    /// \post `empty()`.
    void clear();

    /// Removes all data from this list.
    /// The list will not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

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

    /// Visits every node from begin to end.
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
    detail::raw_list_impl& impl() const;

private:
    std::unique_ptr<detail::raw_list_impl> m_impl;
};

/// Cursors are used to traverse the values in a list.
class raw_list_cursor {
public:
    raw_list_cursor();
    raw_list_cursor(const raw_list_cursor&);
    raw_list_cursor(raw_list_cursor&&) noexcept;
    ~raw_list_cursor();

    raw_list_cursor& operator=(const raw_list_cursor&);
    raw_list_cursor& operator=(raw_list_cursor&&) noexcept;

public:
    /// Returns the size of a value.
    u32 value_size() const;

    /// Returns a pointer to the current value. The returned pointer has exactly `value_size` readable bytes.
    /// Throws an exception if the cursor does not currently point to a valid value.
    const byte* get() const;

    /// Returns a pointer to the current value. The returned pointer has exactly `value_size` readable bytes.
    /// Throws an exception if the cursor does not currently point to a valid value.
    void set(const byte* data);

    /// Returns true if the cursor has become invalid (by iterating
    /// past the end or the beginning).
    bool at_end() const;

    /// Returns true if the cursor's current list element has been erased.
    /// Iterators to erased elements have to be moved before they can be useful again.
    bool erased() const;

    /// Equivalent to `!at_end()`.
    explicit operator bool() const { return !at_end(); }

    /// Moves this cursor to the first value in the list.
    void move_first();

    /// Moves this cursor to the last value in the list.
    void move_last();

    /// Moves this cursor the the next value.
    void move_next();

    /// Moves this cursor to the previous value.
    void move_prev();

    /// Inserts the new value *before* the current list element.
    /// The cursor must point to a valid element.
    /// The provided byte buffer must have `value_size` readable bytes.
    void insert_before(const byte* data);

    /// Inserts the new value *after* the current list element.
    /// The cursor must point to a valid element.
    /// The provided byte buffer must have `value_size` readable bytes.
    void insert_after(const byte* data);

    /// Erases the element that this cursors points at.
    /// In order for this to work, the cursor must not be at the end and must not
    /// already point at an erased element.
    void erase();

private:
    detail::raw_list_cursor_impl& impl() const;

private:
    friend raw_list;

    raw_list_cursor(std::unique_ptr<detail::raw_list_cursor_impl> impl);

private:
    std::unique_ptr<detail::raw_list_cursor_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_RAW_LIST_HPP
