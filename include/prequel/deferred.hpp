#ifndef PREQUEL_DEFERRED_HPP
#define PREQUEL_DEFERRED_HPP

#include <exception>
#include <type_traits>
#include <utility>

namespace prequel {

/// An object that performs some action when the enclosing scope ends.
///
/// The deferred class stores a function object and invokes it from its
/// destructor. The execution of the function object can be disabled by using
/// the `disable()` member function prior to its destruction.
template<typename Function>
class deferred {
public:
    deferred(const Function& fn_)
        : fn(fn_)
        , invoke(true) {}

    deferred(Function&& fn_)
        : fn(std::move(fn_))
        , invoke(true) {}

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

    /// Disables the execution of the deferred function.
    void disable() noexcept { invoke = false; }

    deferred(deferred&& other) = delete;
    deferred& operator=(const deferred&) = delete;

private:
    Function fn;
    bool invoke;
};

} // namespace prequel

#endif // PREQUEL_DEFERRED_HPP
