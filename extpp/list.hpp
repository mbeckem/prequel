#ifndef EXTPP_LIST_HPP
#define EXTPP_LIST_HPP

#include <extpp/raw_list.hpp>
#include <extpp/serialization.hpp>

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

        friend list;
        friend binary_format_access;
    };

public:
    class iterator {
    public:
        // TODO

    private:
        raw_list::iterator inner;
    };

    class visitor {
        // TODO

    private:
        raw_list::visitor inner;
    };

public:
    list(handle<anchor> anchor_, allocator& alloc_)
        : m_list(anchor_.member<&anchor::list>(), value_size(), alloc_)
    {}

    list(const list&) = delete;
    list& operator=(const list&) = delete;

    list(list&&) noexcept = default;
    list& operator=(list&&) noexcept = default;

public:
    engine& get_engine() const { return m_list.get_engine(); }
    allocator& get_allocator() const { return m_list.get_allocator(); }

    static constexpr u32 value_size() const { return serialized_size<T>(); }

    u32 node_capacity() const { return m_list.node_capacity(); }
    bool empty() const { return m_list.empty(); }
    u64 size() const { return m_list.size(); }
    u64 nodes() const { return m_list.nodes(); }

    double fill_factor() const { return m_list.fill_factor(); }
    u64 byte_size() const { return m_list.byte_size(); }
    double overhead() const { return m_list.overhead(); }

    // TODO:
    iterator begin() const;
    iterator end() const;
    visitor visit() const;

    void clear() { m_list.clear(); }
    void erase(const iterator& pos) { m_list.erase(pos); }

    void push_front(const T& value);
    void push_back(const T& value);
    iterator insert(const iterator& pos, const T& value);

    void pop_front() { m_list.pop_front(); }
    void pop_back() { m_list.pop_back(); }

private:
    template<typename Func>
    static auto with_serialized(const T& value, Func&& fn) const {
        byte buffer[serialized_size<T>()];
        serialize(value, buffer, sizeof(buffer));
        return fn(buffer);
    }

private:
    raw_list m_list;
};

template<typename T>
inline void list<T>::push_front(const T& value) {
    with_serialized(value, [&](const byte* data) {
        m_list.push_front(data);
    });
}

template<typename T>
inline void list<T>::push_back(const T& value) {
    with_serialized(value, [&](const byte* data) {
        m_list.push_back(data);
    });
}

template<typename T>
inline list<T>::iterator list<T>::insert(const iterator& pos, const T& value) {
    with_serialized(value, [&](const byte* data) {
        return iterator(m_list.insert(pos.inner, data));
    });
}

} // namespace extpp

#endif // EXTPP_LIST_HPP
