#ifndef EXTPP_DETAIL_FUNC_UTILS_HPP
#define EXTPP_DETAIL_FUNC_UTILS_HPP

#include <extpp/defs.hpp>

#include <type_traits>
#include <utility>

namespace extpp {
namespace detail {

template<typename Function>
class rollback_impl {
private:
    Function fn;
    bool invoke;

public:
    rollback_impl(Function fn)
        : fn(fn)
        , invoke(true)
    {}

    ~rollback_impl() noexcept(noexcept(fn())) {
        if (invoke) {
            fn();
        }
    }

    /// Once `commit` has been called on a rollback object,
    /// the rollback function will not be executed upon destruction.
    void commit() {
        invoke = false;
    }

    rollback_impl(rollback_impl&& other) noexcept(std::is_nothrow_move_constructible<Function>::value)
        : fn(std::move(other.fn))
        , invoke(std::exchange(other.invoke, false))
    {}

    rollback_impl& operator=(const rollback_impl&) = delete;
};

/// Constructs an object that will invoke `fn` if it gets destroyed.
/// The invocation of `fn` can be stopped by calling `commit()` prior
/// to the destruction.
///
/// \relates rollback_impl
template<typename Function>
auto rollback(const Function& fn) {
    return rollback_impl<Function>(fn);
}

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_FUNC_UTILS_HPP
