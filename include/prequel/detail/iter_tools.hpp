#ifndef PREQUEL_DETAIL_ITER_TOOLS_HPP
#define PREQUEL_DETAIL_ITER_TOOLS_HPP

#include <prequel/defs.hpp>

#include <tuple>

namespace prequel {
namespace detail {

template<typename Iter>
class iterator_range {
private:
    Iter first;
    Iter second;

public:
    using iterator = Iter;

    iterator_range(Iter a, Iter b)
        : first(a), second(b) {}

    const Iter& begin() const { return first; }
    const Iter& end() const { return second; }
};

template<typename Iter>
iterator_range<Iter> iter_range(Iter a, Iter b) {
    return { std::move(a), std::move(b) };
}

template<typename Iter>
iterator_range<Iter> iter_range(std::pair<Iter, Iter> p) {
    return { std::move(p.first), std::move(p.second) };
}


template<typename Iter>
iterator_range<Iter> iter_range(std::tuple<Iter, Iter> p) {
    return { std::move(std::get<0>(p)), std::move(std::get<1>(p)) };
}

template<typename Tuple, typename Visitor, size_t... I>
constexpr void tuple_for_each_impl(Tuple&& t, Visitor&& v, std::index_sequence<I...>) {
    ((void) v(std::get<I>(t)), ...);
}

template<typename Tuple, typename Visitor>
constexpr void tuple_for_each(Tuple&& t, Visitor&& v) {
    return tuple_for_each_impl(std::forward<Tuple>(t), std::forward<Visitor>(v),
                               std::make_index_sequence<std::tuple_size_v<std::decay_t<Tuple>>>());
}

// For when you really need to iterate over a range of numbers ...
template<typename Integer>
class identity_iterator {
public:
    using difference_type = Integer;
    using value_type = Integer;
    using pointer = const Integer*;
    using reference = const Integer&;
    using iterator_category = std::random_access_iterator_tag;

public:
    identity_iterator() = default;

    identity_iterator(Integer value): m_value(std::move(value)) {}

    identity_iterator operator++(int) {
        identity_iterator ret = *this;
        ++m_value;
        return ret;
    }

    identity_iterator& operator++() {
        ++m_value;
        return *this;
    }

    identity_iterator operator--(int) {
        identity_iterator ret = *this;
        --m_value;
        return ret;
    }

    identity_iterator& operator--() {
        --m_value;
        return *this;
    }

    identity_iterator operator+(const Integer& value) const {
        return identity_iterator(m_value + value);
    }

    identity_iterator operator-(const Integer& value) const {
        return identity_iterator(m_value - value);
    }

    identity_iterator& operator+=(const Integer& value) {
        m_value += value;
        return *this;
    }

    identity_iterator& operator-=(const Integer& value) {
        m_value -= value;
        return *this;
    }

    Integer operator*() const { return m_value; }

    Integer operator[](const Integer& value) const {
        return m_value + value;
    }

    Integer operator-(const identity_iterator& other) const {
        return m_value - other.m_value;
    }

#define PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(token) \
    bool operator token(const identity_iterator& other) const { \
        return m_value token other.m_value; \
    }

    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(==);
    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(!=);
    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(<=);
    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(>=);
    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(<);
    PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE(>);

#undef PREQUEL_DETAIL_IDENTITY_ITERATOR_TRIVIAL_COMPARE

private:
    Integer m_value{};
};

} // namespace detail
} // namespace prequel

#endif // PREQUEL_DETAIL_ITER_TOOLS_HPP
