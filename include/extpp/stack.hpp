#ifndef EXTPP_STACK_HPP
#define EXTPP_STACK_HPP

#include <extpp/raw_stack.hpp>
#include <extpp/serialization.hpp>

#include <fmt/ostream.h>

namespace extpp {

template<typename T>
class stack {
public:
    using value_type = T;
    using size_type = u64;

public:
    class anchor {
        raw_stack::anchor stack;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::stack);
        }

        friend binary_format_access;
        friend class stack;
    };

public:
    stack(anchor_handle<anchor> _anchor, allocator& _alloc)
        : m_inner(std::move(_anchor).template member<&anchor::stack>(), value_size(), _alloc)
    {}

public:
    engine& get_engine() const { return m_inner.get_engine(); }
    allocator& get_allocator() const { return m_inner.get_allocator(); }

    static constexpr u32 value_size() { return serialized_size<T>(); }
    u32 node_capacity() const { return m_inner.node_capacity(); }

    bool empty() const { return m_inner.empty(); }
    u64 size() const  { return m_inner.size(); }
    u64 nodes() const { return m_inner.nodes(); }
    double fill_factor() const { return m_inner.fill_factor(); }
    u64 byte_size() const { return m_inner.byte_size(); }
    double overhead() const { return m_inner.overhead(); }

    T top() const {
        serialized_buffer<T> buffer;
        m_inner.top(buffer.data());
        return deserialized_value<T>(buffer.data());
    }

    void push(const T& value) {
        auto buffer = serialized_value(value);
        m_inner.push(buffer.data());
    }

    void pop() { m_inner.pop(); }

    void clear() { m_inner.clear(); }
    void reset() { m_inner.reset(); }

    void validate() const { m_inner.validate(); }

    // TODO
    // void dump(std::ostream& os);

    const raw_stack& raw() const { return m_inner; }

private:
    raw_stack m_inner;
};

} // namespace extpp

#endif // EXTPP_STACK_HPP
