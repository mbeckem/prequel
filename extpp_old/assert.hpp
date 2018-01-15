#ifndef EXTPP_ASSERT_HPP
#define EXTPP_ASSERT_HPP

#include <cassert>

/// \defgroup assertions Assertion Macros
/// @{

// TODO: Other compilers.
/// Evaluates the expression `x` and gives a hint to the compiler
/// that it is likely to be true.
#define EXTPP_LIKELY(x)      (__builtin_expect(!!(x), 1))

/// Evaluates the expression `x` and gives a hint to the compiler
/// that it is likely to be false.
#define EXTPP_UNLIKELY(x)    (__builtin_expect(!!(x), 0))

// TODO: Own debugging macro
#ifndef NDEBUG

/// EXTPP_DEBUG is defined when this library was built in debug mode.
#define EXTPP_DEBUG

#endif

#ifdef EXTPP_DEBUG

/// When in debug mode, check against the given condition
/// and abort the program with a message if the check fails.
/// Does nothing in release mode.
#define EXTPP_ASSERT(cond, message)                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            ::extpp::detail::assert_impl(                               \
                __FILE__, __LINE__, #cond, (message));                  \
        }                                                               \
    } while (0)

/// Same as EXTPP_ASSERT, but usable in constexpr functions.
#define EXTPP_CONSTEXPR_ASSERT(cond, message)                           \
    do {                                                                \
        if (!(cond)) {                                                  \
            throw ::extpp::detail::assertion_failure_impl_(             \
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
            ::extpp::detail::assert_impl(                               \
                __FILE__, __LINE__, #cond, (message));                  \
        }                                                               \
    } while (0)

/// Unconditionally abort the program with a message.
#define EXTPP_ABORT(message) (::extpp::detail::abort_impl(__FILE__, __LINE__, (message)))

/// Unconditionally terminate the program when unreachable code is executed.
#define EXTPP_UNREACHABLE(message) (::extpp::detail::unreachable_impl_(__FILE__, __LINE__, (message)))

/// @}

/// \cond INTERNAL
namespace extpp::detail {

struct assertion_failure_impl_ {
    /// The constructor simply calls check_impl, this is part of the assertion implemention
    /// for constexpr functions.
    assertion_failure_impl_(const char* file, int line, const char* cond, const char* message);
};

[[noreturn]] void assert_impl(const char* file, int line, const char* cond, const char* message);
[[noreturn]] void unreachable_impl_(const char* file, int line, const char* message);
[[noreturn]] void abort_impl(const char* file, int line, const char* message);

} // namespace extpp::detail
/// \endcond

#endif // EXTPP_ASSERT_HPP
