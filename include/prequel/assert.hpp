#ifndef PREQUEL_ASSERT_HPP
#define PREQUEL_ASSERT_HPP

#include <cassert>

/// \defgroup assertions Assertion Macros
/// @{

// TODO: Other compilers.
/// Evaluates the expression `x` and gives a hint to the compiler
/// that it is likely to be true.
#define PREQUEL_LIKELY(x) (__builtin_expect(!!(x), 1))

/// Evaluates the expression `x` and gives a hint to the compiler
/// that it is likely to be false.
#define PREQUEL_UNLIKELY(x) (__builtin_expect(!!(x), 0))

// TODO: Own debugging macro
#ifndef NDEBUG

/// PREQUEL_DEBUG is defined when this library is used in debug mode.
#    define PREQUEL_DEBUG

#endif

#ifdef PREQUEL_DEBUG

/// When in debug mode, check against the given condition
/// and abort the program with a message if the check fails.
/// Does nothing in release mode.
#    define PREQUEL_ASSERT(cond, message)                                             \
        do {                                                                          \
            if (!(cond)) {                                                            \
                ::prequel::detail::assert_impl(__FILE__, __LINE__, #cond, (message)); \
            }                                                                         \
        } while (0)

/// Same as PREQUEL_ASSERT, but usable in constexpr functions.
#    define PREQUEL_CONSTEXPR_ASSERT(cond, message)                                         \
        do {                                                                                \
            if (!(cond)) {                                                                  \
                throw ::prequel::detail::assertion_failure_impl_(__FILE__, __LINE__, #cond, \
                                                                 (message));                \
            }                                                                               \
        } while (0)

#else

#    define PREQUEL_ASSERT(cond, message)

#    define PREQUEL_CONSTEXPR_ASSERT(cond, message)

#endif

/// Always check against a (rare) error condition and abort the program
/// with a message if the check fails.
#define PREQUEL_CHECK(cond, message)                                              \
    do {                                                                          \
        if (PREQUEL_UNLIKELY(!(cond))) {                                          \
            ::prequel::detail::assert_impl(__FILE__, __LINE__, #cond, (message)); \
        }                                                                         \
    } while (0)

/// Unconditionally abort the program with a message.
#define PREQUEL_ABORT(message) (::prequel::detail::abort_impl(__FILE__, __LINE__, (message)))

/// Unconditionally terminate the program when unreachable code is executed.
#define PREQUEL_UNREACHABLE(message) \
    (::prequel::detail::unreachable_impl_(__FILE__, __LINE__, (message)))

/// @}

/// \cond INTERNAL
namespace prequel::detail {

struct assertion_failure_impl_ {
    /// The constructor simply calls check_impl, this is part of the assertion implemention
    /// for constexpr functions.
    assertion_failure_impl_(const char* file, int line, const char* cond, const char* message);
};

[[noreturn]] void assert_impl(const char* file, int line, const char* cond, const char* message);
[[noreturn]] void unreachable_impl_(const char* file, int line, const char* message);
[[noreturn]] void abort_impl(const char* file, int line, const char* message);

} // namespace prequel::detail
/// \endcond

#endif // PREQUEL_ASSERT_HPP
