#ifndef PREQUEL_DETAIL_FIX_HPP
#define PREQUEL_DETAIL_FIX_HPP

#include <type_traits>
#include <utility>

namespace prequel {
namespace detail {

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
///     fix fib = [](auto& self, int i) -> int {
///         if (i == 0)
///             return 0;
///         if (i == 1)
///             return 1;
///         return self(i - 2) + self(i - 1);
///     });
///     fib(42); // Works.
///
template<typename Function>
class fix {
private:
    Function m_fn;

public:
    fix(const Function& fn)
        : m_fn(fn) {}

    fix(Function&& fn)
        : m_fn(std::move(fn)) {}

    template<typename... Args>
    decltype(auto) operator()(Args&&... args) const {
        return m_fn(*this, std::forward<Args>(args)...);
    }
};

} // namespace detail
} // namespace prequel

#endif // PREQUEL_DETAIL_FIX_HPP
