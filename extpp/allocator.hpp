#ifndef EXTPP_ALLOCATOR_HPP
#define EXTPP_ALLOCATOR_HPP

#include <extpp/address.hpp>
#include <extpp/defs.hpp>

namespace extpp {

template<u32 BlockSize>
class allocator {
public:
    virtual ~allocator() = default;

    /// Allocates `n` consecutive blocks.
    /// Returns the address of the first block.
    virtual raw_address<BlockSize> allocate(u64 n) = 0;

    // TODO: Reallocate.

    /// Frees blocks previously allocated using `allocate()`.
    virtual void free(raw_address<BlockSize> a) = 0;
};

} // namespace extpp

#endif // EXTPP_ALLOCATOR_HPP
