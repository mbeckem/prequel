#ifndef EXTPP_ASSERT_HPP
#define EXTPP_ASSERT_HPP

#include <cassert>

// TODO: Other compilers.

#define EXTPP_LIKELY(x)      (__builtin_expect(!!(x), 1))
#define EXTPP_UNLIKELY(x)    (__builtin_expect(!!(x), 0))

// TODO: Own debugging macro
#ifndef NDEBUG
#define EXTPP_DEBUG
#endif

#ifdef EXTPP_DEBUG

/// When in debug mode, check against the given condition
/// and abort the program with a message if the check fails.
/// Does nothing in release mode.
#define EXTPP_ASSERT(cond, message)                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            ::extpp::assert_impl(__FILE__, __LINE__, #cond, (message)); \
        }                                                               \
    } while (0)

/// Same as EXTPP_ASSERT, but usable in constexpr functions.
#define EXTPP_CONSTEXPR_ASSERT(cond, message)                           \
    do {                                                                \
        if (!(cond)) {                                                  \
            throw ::extpp::assertion_failure_impl_(                     \
                __FILE__, __LINE__, #cond, (message));                  \
        }                                                               \
    } while (0)

#else

#define EXTPP_ASSERT(cond, message)

#define EXTPP_CONSTEXPR_ASSERT(cond, message)

#endif

/// Always check against a (rare) error condition and abort the program
/// with a message if the check fails.
#define EXTPP_CHECK(cond, message)                                      \
    do {                                                                \
        if (EXTPP_UNLIKELY(!(cond))) {                                  \
            ::extpp::assert_impl(__FILE__, __LINE__, #cond, (message)); \
        }                                                               \
    } while (0)

/// Unconditionally abort the program with a message.
#define EXTPP_ABORT(message) (::extpp::abort_impl(__FILE__, __LINE__, (message)))

/// Unconditionally terminate the program when unreachable code is executed.
#define EXTPP_UNREACHABLE(message) (::extpp::unreachable_impl_(__FILE__, __LINE__, (message)))

namespace extpp {

struct assertion_failure_impl_ {
    /// The constructor simply calls check_impl, this is part of the assertion implemention
    /// for constexpr functions.
    assertion_failure_impl_(const char* file, int line, const char* cond, const char* message);
};

[[noreturn]] void assert_impl(const char* file, int line, const char* cond, const char* message);
[[noreturn]] void unreachable_impl_(const char* file, int line, const char* message);
[[noreturn]] void abort_impl(const char* file, int line, const char* message);

} // namespace extpp

#endif // EXTPP_ASSERT_HPP
