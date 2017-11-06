#ifndef EXTPP_BLOCK_HPP
#define EXTPP_BLOCK_HPP

#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/type_traits.hpp>

namespace extpp {
namespace detail {

template<typename T, u32 Padding>
struct add_padding;

template<typename T>
struct add_padding<T, 0>: T {
    using T::T;

    static constexpr u32 extra_padding = 0;
};

template<typename T, u32 Padding>
struct add_padding: T {
    using T::T;

    static constexpr u32 extra_padding = Padding;

protected:
    byte m_extra_padding[Padding] = {};
};

template<template<u32 N> class Proto, u32 BlockSize, u32 Capacity,
         bool fits = (sizeof(Proto<Capacity>) <= BlockSize)>
struct select_capacity_impl;

template<template<u32 N> class Proto, u32 BlockSize, u32 Capacity>
struct select_capacity_impl<Proto, BlockSize, Capacity, true> : Proto<Capacity> {
    using Proto<Capacity>::Proto;

    static constexpr u32 capacity = Capacity;
};

template<template<u32 N> class Proto, u32 BlockSize, u32 Capacity>
struct select_capacity_impl<Proto, BlockSize, Capacity, false>
        : select_capacity_impl<Proto, BlockSize, Capacity - 1> {};

template<template<u32 N> class Proto, u32 BlockSize, bool fits>
struct select_capacity_impl<Proto, BlockSize, 0, fits> {
    static_assert(always_false_v<select_capacity_impl>,
                  "The block prototype cannot fit the provided BlockSize.");
};

template<typename T, u32 BlockSize, bool small_enough = (sizeof(T) <= BlockSize)>
struct make_block;

template<typename T, u32 BlockSize>
struct make_block<T, BlockSize, false> {
    static_assert(always_false_v<T>,
                  "T is too large to fit into the given block size.");
};

template<typename T, u32 BlockSize>
struct make_block<T, BlockSize, true> {
    using type = add_padding<T, BlockSize - sizeof(T)>;
};

} // namespace detail

template<typename T, u32 BlockSize>
struct make_block {
    using type = typename detail::make_block<T, BlockSize>::type;

    static_assert(sizeof(type) == BlockSize, "Size must be exact.");

    static_assert(is_pow2(BlockSize), "BlockSize must be a power of two.");

    static_assert(is_trivial<type>::value, "Blocks must be trivially copyable.");
};

template<typename T, u32 BlockSize>
using make_block_t = typename make_block<T, BlockSize>::type;

template<template<u32 N> class Proto,
         u32 BlockSize, u32 MaxCapacity>
struct make_variable_block {
    static_assert(MaxCapacity > 0, "MaxCapacity must be greater than 0.");

    using type = make_block_t<detail::select_capacity_impl<Proto, BlockSize, MaxCapacity>, BlockSize>;
};

template<template<u32 N> class Proto, u32 BlockSize, u32 MaxCapacity>
using make_variable_block_t = typename make_variable_block<Proto, BlockSize, MaxCapacity>::type;

template<typename Header, typename Value, u32 BlockSize>
struct make_array_block {
private:
    static_assert(sizeof(Header) <= BlockSize,
                  "Header cannot fit into BlockSize.");

    // This is just an estimate because it doesn't take Value's alignment into account.
    // The real capacity is determined by make_variable_block and may be a bit lower.
    static constexpr u32 max_array_size = (BlockSize - sizeof(Header)) / sizeof(Value);

    static_assert(max_array_size > 0,
                  "No space left in block after array header");

    template<u32 N>
    struct array_proto: Header {
        using Header::Header;

        Value values[N];
    };

public:
    using type = make_variable_block_t<array_proto, BlockSize, max_array_size>;
};

template<typename Header, typename Value, u32 BlockSize>
using make_array_block_t = typename make_array_block<Header, Value, BlockSize>::type;

} // namespace extpp

#endif // EXTPP_BLOCK_HPP
