#ifndef EXTPP_LOG_HPP
#define EXTPP_LOG_HPP

#include <fmt/format.h>

namespace extpp {

template<typename... Args>
void print_trace(const char* module, const char* format, Args&&...args) {
    // TODO: TIME
    fmt::print("{}: ", module);
    fmt::print(format, std::forward<Args>(args)...);
    fmt::print("\n");
}

} // namespace extpp

#endif // EXTPP_LOG_HPP
