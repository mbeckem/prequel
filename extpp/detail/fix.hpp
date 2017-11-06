#ifndef EXTPP_DETAIL_FIX_HPP
#define EXTPP_DETAIL_FIX_HPP

#include <type_traits>
#include <utility>

namespace extpp {
namespace detail {

template<typename Function>
struct fix_impl {
    Function function;

    template<typename... Args>
    decltype(auto) operator()(Args&&... args) const {
        return function(*this, std::forward<Args>(args)...);
    }
};

/// Makes it possible to write recursive lambda functions.
///
/// In C++, lambda functions cannot refer to themselves by name, i.e.
/// they cannot be used to implement recursive functions.
/// The function `fix` takes a lambda (or any other function object)
/// as an argument and returns a new function object that always
/// passes itself as the first argument.
/// This additional argument enables the function to refer to itself.
///
/// Example:
///
///     auto fib = fix([](auto& self, int i) -> int {
///         if (i == 0)
///             return 0;
///         if (i == 1)
///             return 1;
///         return self(i - 2) + self(i - 1);
///     });
///     fib(42); // Works.
///
template<typename Function>
auto fix(Function&& fn) {
    return fix_impl<std::decay_t<Function>>{std::forward<Function>(fn)};
}

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_FIX_HPP
