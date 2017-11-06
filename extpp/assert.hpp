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

#define EXTPP_ASSERT(cond, message)                                     \
    do {                                                                \
        if (!(cond)) {                                                  \
            ::extpp::check_impl_(__FILE__, __LINE__, #cond, (message)); \
        }                                                               \
    } while (0)

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

#define EXTPP_CHECK(cond, message)                                      \
    do {                                                                \
        if (EXTPP_UNLIKELY(!(cond))) {                                  \
            ::extpp::check_impl_(__FILE__, __LINE__, #cond, (message)); \
        }                                                               \
    } while (0)

#define UNREACHABLE(message) (::keydb::unreachable_impl_(__FILE__, __LINE__, (message)))

namespace extpp {

struct assertion_failure_impl_ {
    /// The constructor simply calls check_impl, this is part of the assertion implemention
    /// for constexpr functions.
    assertion_failure_impl_(const char* file, int line, const char* cond, const char* message);
};

[[noreturn]] void check_impl_(const char* file, int line, const char* cond, const char* message);

[[noreturn]] void unreachable_impl_(const char* file, int line, const char* message);

} // namespace extpp

#endif // EXTPP_ASSERT_HPP
