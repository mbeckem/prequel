#include <prequel/block_index.hpp>

#include <prequel/engine.hpp>

#include <prequel/address.hpp>

namespace prequel {

void zero_blocks(engine& e, block_index index, u64 size) {
    return zero(e, e.to_address(index), e.to_byte_size(size));
}

void copy_blocks(engine& e, block_index src, block_index dest, u64 size) {
    return copy(e, e.to_address(src), e.to_address(dest), e.to_byte_size(size));
}

} // namespace prequel
