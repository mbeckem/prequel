#ifndef EXTPP_BTREE_BASE_HPP
#define EXTPP_BTREE_BASE_HPP

#include <extpp/defs.hpp>

#include <array>

namespace extpp::detail::btree_impl {

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

} // namespace extpp::detail::btree_impl

#endif // EXTPP_BTREE_BASE_HPP
