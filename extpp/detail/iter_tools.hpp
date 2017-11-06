#ifndef EXTPP_DETAIL_ITER_TOOLS_HPP
#define EXTPP_DETAIL_ITER_TOOLS_HPP

#include <extpp/defs.hpp>

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

    Iter begin() const { return first; }
    Iter end() const { return second; }
};

template<typename Iter>
iterator_range<Iter> iter_range(Iter a, Iter b) {
    return { std::move(a), std::move(b) };
}

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_ITER_TOOLS_HPP
