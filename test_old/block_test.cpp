#include <catch.hpp>

#include <extpp/block.hpp>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 block_size = 512;

struct header1 {
    int a = 0, b = 0, c = 1;
};

struct block1 : make_block_t<header1, block_size> {};

static_assert(sizeof(block1) == block_size, "");
static_assert(std::is_base_of<header1, block1>::value, "");
static_assert(is_trivial<block1>::value, "");

struct header2 {
    u32 count = 0;
};

struct block2 : make_array_block_t<header2, int, block_size> {};

static_assert(sizeof(block2) == block_size, "");
static_assert(std::is_base_of<header2, block2>::value, "");
static_assert(is_trivial<block2>::value, "");
static_assert(sizeof(header2) + sizeof(int) * block2::capacity <= block_size, "");

template<u32 N>
struct tmpl3 {
    int a[N];
    int b[N * 2];
};

constexpr u32 estimate = block_size / (sizeof(int) * 3);

struct block3 : make_variable_block_t<tmpl3, block_size, estimate> {};

static_assert(block3::capacity == estimate, "");
static_assert(sizeof(block3) == block_size, "");

struct header4 {
    char data[block_size - 1];
};

struct block4 : make_block_t<header4, block_size> {};

static_assert(sizeof(block4) == block_size, "");
static_assert(block4::extra_padding == 1, "");
static_assert(sizeof(block4::m_extra_padding) == 1, "");
