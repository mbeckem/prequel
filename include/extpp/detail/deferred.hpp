#ifndef EXTPP_DETAIL_DEFERRED_HPP
#define EXTPP_DETAIL_DEFERRED_HPP

#include <extpp/defs.hpp>

#include <exception>
#include <type_traits>
#include <utility>

namespace extpp {
namespace detail {

/// An object that performs some action when the enclosing scope ends.
///
/// The deferred class stores a function object and invokes it from its
/// destructor. The execution of the function object can be disabled by using
/// the `disable()` member function prior to its destruction.
template<typename Function>
class deferred {
private:
    Function fn;
    bool invoke;

public:
    deferred(const Function& fn)
        : fn(fn)
        , invoke(true)
    {}

    deferred(Function&& fn)
        : fn(std::move(fn))
        , invoke(true)
    {}

    ~deferred() noexcept(noexcept(fn())) {
        if (invoke) {
            try {
                fn();
            } catch (...) {
                if (!std::uncaught_exceptions())
                    throw;
            }
        }
    }

    /// Once `disable` has been called on a rollback object,
    /// the rollback function will not be executed upon destruction.
    void disable() {
        invoke = false;
    }

    deferred(deferred&& other) = delete;
    deferred& operator=(const deferred&) = delete;
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_DEFERRED_HPP
