#include <extpp/block_index.hpp>

#include <extpp/engine.hpp>
#include <extpp/address.hpp>

namespace extpp {

void zero_blocks(engine& e, block_index index, u64 size) {
    return zero(e, e.to_address(index), e.to_byte_size(size));
}

void copy_blocks(engine& e, block_index dest, block_index src, u64 size) {
    return copy(e, e.to_address(dest), e.to_address(src), e.to_byte_size(size));
}

} // namespace extpp
