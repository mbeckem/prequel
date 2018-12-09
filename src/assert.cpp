#include <prequel/assert.hpp>

#include <prequel/exception.hpp>

#include <cstdlib>
#include <cstring>
#include <iostream>

namespace prequel::detail {

assertion_failure_impl_::assertion_failure_impl_(const char* file, int line, const char* cond,
                                                 const char* message) {
    assert_impl(file, line, cond, message);
}

void assert_impl(const char* file, int line, const char* condition, const char* message) {
    std::cerr << "Assertion `" << condition << "` failed";
    if (message && std::strlen(message) > 0) {
        std::cerr << ": " << message;
    }
    std::cerr << "\n";
    std::cerr << "    (in " << file << ":" << line << ")" << std::endl;
    std::abort();
}

void unreachable_impl_(const char* file, int line, const char* message) {
    std::cerr << "Unreachable code executed";
    if (message && std::strlen(message) > 0) {
        std::cerr << ": " << message;
    }
    std::cerr << ".\n";
    std::cerr << "    (in " << file << ":" << line << ")" << std::endl;
    std::abort();
}

void abort_impl(const char* file, int line, const char* message) {
    if (message) {
        std::cerr << "Abort: " << message;
    } else {
        std::cerr << "Abort.";
    }
    std::cerr << "\n"
              << "    (in " << file << ":" << line << ")" << std::endl;
    std::abort();
}

} // namespace prequel::detail
