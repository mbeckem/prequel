#include <prequel/hash.hpp>

namespace prequel {

u64 fnv_1a(const byte* data, size_t length) noexcept {
    static const u64 magic_prime = UINT64_C(0x00000100000001b3);

    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    for (; length > 0; --length) {
        hash = hash ^ *data++;
        hash = hash * magic_prime;
    }

    return hash;
}

} // namespace prequel
