#include <extpp/assert.hpp>

#include <cstdlib>
#include <cstring>
#include <iostream>

namespace extpp {

assertion_failure_impl_::assertion_failure_impl_(
        const char* file, int line,
        const char* cond, const char* message) {
    check_impl_(file, line, cond, message);
}


void check_impl_(const char* file, int line, const char* condition, const char* message) {
    std::cerr << "Assertion `" << condition << "` failed";
    if (message && std::strlen(message) > 0) {
        std::cerr << ": " << message;
    }
    std::cerr << "\n";
    std::cerr << "    (in " << file << ":" << line << ")"
              << std::endl;
    std::abort();
}

void unreachable_impl_(const char* file, int line, const char* message) {
    std::cerr << "Unreachable code executed";
    if (message && std::strlen(message) > 0) {
        std::cerr << ": " << message;
    }
    std::cerr << ".\n";
    std::cerr << "    (in " << file << ":" << line << ")"
              << std::endl;
    std::abort();
}

} // namespace extpp
