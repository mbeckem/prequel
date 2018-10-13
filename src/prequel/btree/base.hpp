#ifndef PREQUEL_BTREE_BASE_HPP
#define PREQUEL_BTREE_BASE_HPP

#include <prequel/defs.hpp>

#include <array>

namespace prequel::detail::btree_impl {

// Foward declarations
class tree;
class cursor;
class loader;
class internal_node;
class leaf_node;

// Key size is limited to allow for stack allocation, C++ does not have VLAs.
// Might as well use alloca() and rely on compiler extensions but this should be fine for now.
static constexpr u32 max_key_size = 256;

using key_buffer = std::array<byte, max_key_size>;

} // namespace prequel::detail::btree_impl

#endif // PREQUEL_BTREE_BASE_HPP
