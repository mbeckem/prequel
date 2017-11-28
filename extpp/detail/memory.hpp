#ifndef EXTPP_DETAIL_MEMORY_HPP
#define EXTPP_DETAIL_MEMORY_HPP

#include <extpp/defs.hpp>

#include <cstdlib>
#include <cstring>
#include <type_traits>

namespace extpp::detail {

/// Copy `count` objects from `source` to `dest`.
template<typename T>
static void copy(const T* source, size_t count, T* dest) {
    static_assert(std::is_trivially_copyable<T>::value,
                  "T must be trivially copyable");
    std::memmove(dest, source, sizeof(T) * count);
}

/// Shifts `count` objects from `source` by `shift` positions.
/// E.g. `shift(ptr, 5, 1)` moves 5 objects starting from `ptr` one step to the right.
template<typename T>
static void shift(T* source, size_t count, std::ptrdiff_t shift) {
    copy(source, count, source + shift);
}

} // namespace extpp::detail

#endif // EXTPP_DETAIL_MEMORY_HPP
