#ifndef EXTPP_DETAIL_ITER_TOOLS_HPP
#define EXTPP_DETAIL_ITER_TOOLS_HPP

#include <extpp/defs.hpp>

#include <tuple>

namespace extpp {
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

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_ITER_TOOLS_HPP
