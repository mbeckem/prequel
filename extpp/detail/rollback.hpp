#ifndef EXTPP_DETAIL_FUNC_UTILS_HPP
#define EXTPP_DETAIL_FUNC_UTILS_HPP

#include <extpp/defs.hpp>

#include <exception>
#include <type_traits>
#include <utility>

namespace extpp {
namespace detail {

template<typename Function>
class rollback {
private:
    Function fn;
    bool invoke;

public:
    rollback(const Function& fn)
        : fn(fn)
        , invoke(true)
    {}

    rollback(Function&& fn)
        : fn(std::move(fn))
        , invoke(true)
    {}

    ~rollback() noexcept(noexcept(fn())) {
        if (invoke) {
            if (std::uncaught_exceptions()) {
                try {
                    fn();
                } catch (...) {}
            } else {
                fn();
            }
        }
    }

    /// Once `commit` has been called on a rollback object,
    /// the rollback function will not be executed upon destruction.
    void commit() {
        invoke = false;
    }

    rollback(rollback&& other) = delete;
    rollback& operator=(const rollback&) = delete;
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_FUNC_UTILS_HPP
