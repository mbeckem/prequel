#ifndef PREQUEL_HASH_HPP
#define PREQUEL_HASH_HPP

#include <prequel/defs.hpp>
#include <prequel/serialization.hpp>

namespace prequel {

/**
 * FNV-1a hash of the given data array.
 * Make sure that `data` is in a well defined (i.e. platform independent)
 * format in order to get the same hash on all platforms.
 */
u64 fnv_1a(const byte* data, size_t length) noexcept;

/**
 * FNV-1a hash of the given value.
 * The value is serialized before hashing for a consistent hash on all platforms.
 * Use the raw \ref fnv_a1(const byte*, size_t) function if that overhead is unacceptable.
 */
template<typename T>
u64 fnv_1a(const T& value) noexcept {
    auto buffer = serialize_to_buffer(value);
    return fnv_1a(buffer.data(), buffer.size());
}

/**
 * An function object that hashes its input using the FNV-1a hash function.
 */
struct fnv_hasher {
    template<typename T>
    u64 operator()(const T& value) const noexcept {
        return fnv_1a(value);
    }
};

} // namespace prequel

#endif // PREQUEL_HASH_HPP
