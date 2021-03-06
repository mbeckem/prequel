#ifndef PREQUEL_CONTAINER_STACK_HPP
#define PREQUEL_CONTAINER_STACK_HPP

#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/block_index.hpp>
#include <prequel/container/allocator.hpp>
#include <prequel/exception.hpp>
#include <prequel/handle.hpp>
#include <prequel/serialization.hpp>

#include <fmt/ostream.h>

#include <memory>
#include <ostream>

namespace prequel {

class raw_stack;

namespace detail {

class raw_stack_impl;

struct raw_stack_anchor {
    /// Number of values in the stack.
    u64 size = 0;

    /// Number of nodes in the stack.
    u64 nodes = 0;

    /// Topmost node on the stack.
    block_index top;
};

} // namespace detail

using raw_stack_anchor = detail::raw_stack_anchor;

class raw_stack {
public:
    using anchor = raw_stack_anchor;

public:
    /**
     * Accesses a raw stack instance rooted at the given anchor.
     * `value_size` and `alloc` must be equivalent every time the stack is loaded.
     */
    explicit raw_stack(anchor_handle<anchor> _anchor, u32 value_size, allocator& alloc);
    ~raw_stack();

    raw_stack(const raw_stack&) = delete;
    raw_stack& operator=(const raw_stack&) = delete;

    raw_stack(raw_stack&&) noexcept;
    raw_stack& operator=(raw_stack&&) noexcept;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    /**
     * Returns the size of a serialized value on disk.
     */
    u32 value_size() const;

    /**
     * Returns the number of serialized values that fit into a single stack node.
     */
    u32 node_capacity() const;

    /**
     * Returns true if the stack is empty.
     */
    bool empty() const;

    /**
     * Returns the nuber of values on the stack.
     */
    u64 size() const;

    /**
     * Returns the number of nodes currenty allocated by the stack.
     */
    u64 nodes() const;

    /**
     * The average fill factor of the stack's nodes.
     */
    double fill_factor() const;

    /**
     * Returns the total size of disk datastructure on disk, in bytes.
     */
    u64 byte_size() const;

    /**
     * Returns the relative overhead of this datastructure compared to a linear file, i.e.
     * the allocated storage (see byte_size()) dividied by the used storage (i.e. size() * value_size()).
     */
    double overhead() const;

    /**
     * Retrieves the top value and copies it into the provided value buffer.
     * The buffer must be at least `value_size()` bytes long.
     *
     * @throws bad_operation If the stack is empty.
     */
    void top(byte* value) const;

    /**
     * Pushes the value onto the stack, by copying `value_size()` bytes
     * from the provided buffer to disk.
     */
    void push(const byte* value);

    /**
     * Removes the top element from the stack.
     *
     * @throws bad_operation If the stack is empty.
     */
    void pop();

    /**
     * Removes all elements from the stack.
     * @post `size() == 0`.
     */
    void clear();

    /**
     * Resets the stack to its empty state and releases all allocated storage.
     * @post `size() == 0 && byte_size() == 0`.
     */
    void reset();

    /**
     * Validates this instance's basic invariants.
     */
    void validate() const;

    /**
     * Writes the state of this stack into the provided output stream (for debugging purposes).
     */
    void dump(std::ostream& os) const;

private:
    detail::raw_stack_impl& impl() const;

private:
    std::unique_ptr<detail::raw_stack_impl> m_impl;
};

template<typename T>
class stack {
public:
    using value_type = T;
    using size_type = u64;

public:
    class anchor {
        raw_stack::anchor stack;

        static constexpr auto get_binary_format() { return binary_format(&anchor::stack); }

        friend binary_format_access;
        friend class stack;
    };

public:
    /**
     * Accesses a stack instance rooted at the given anchor.
     * `value_size` and `alloc` must be equivalent every time the stack is loaded.
     */
    stack(anchor_handle<anchor> _anchor, allocator& _alloc)
        : m_inner(std::move(_anchor).template member<&anchor::stack>(), value_size(), _alloc) {}

public:
    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    /**
     * Returns the size of a serialized value on disk.
     * This is a compile-time instance.
     */
    static constexpr u32 value_size() { return serialized_size<T>(); }

    /**
     * Returns the number of serialized values that fit into a single stack node.
     */
    u32 node_capacity() const { return m_inner.node_capacity(); }

    /**
     * Returns true if the stack is empty.
     */
    bool empty() const { return m_inner.empty(); }

    /**
     * Returns the nuber of values on the stack.
     */
    u64 size() const { return m_inner.size(); }

    /**
     * Returns the number of nodes currenty allocated by the stack.
     */
    u64 nodes() const { return m_inner.nodes(); }

    /**
     * The average fill factor of the stack's nodes.
     */
    double fill_factor() const { return m_inner.fill_factor(); }

    /**
     * Returns the total size of disk datastructure on disk, in bytes.
     */
    u64 byte_size() const { return m_inner.byte_size(); }

    /**
     * Returns the relative overhead of this datastructure compared to a linear file, i.e.
     * the allocated storage (see byte_size()) dividied by the used storage (i.e. size() * value_size()).
     */
    double overhead() const { return m_inner.overhead(); }

    /**
     * Returns the top value, if it exists.
     *
     * @throws bad_operation If the stack is empty.
     */
    T top() const {
        serialized_buffer<T> buffer;
        m_inner.top(buffer.data());
        return deserialize_from_buffer<T>(buffer);
    }

    /**
     * Pushes the value onto the stack.
     */
    void push(const T& value) {
        auto buffer = serialize_to_buffer(value);
        m_inner.push(buffer.data());
    }

    /**
     * Removes the top element from the stack.
     *
     * @throws bad_operation If the stack is empty.
     */
    void pop() { m_inner.pop(); }

    /**
     * Removes all elements from the stack.
     * @post `size() == 0`.
     */
    void clear() { m_inner.clear(); }

    /**
     * Resets the stack to its empty state and releases all allocated storage.
     * @post `size() == 0 && byte_size() == 0`.
     */
    void reset() { m_inner.reset(); }

    /**
     * Validates this instance's basic invariants.
     */
    void validate() const { m_inner.validate(); }

    // TODO
    // void dump(std::ostream& os);

    /**
     * Returns a reference to the raw stack implementation.
     */
    const raw_stack& raw() const { return m_inner; }

private:
    raw_stack m_inner;
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_STACK_HPP
