#include <catch.hpp>

#include <extpp/detail/memory.hpp>

using namespace extpp::detail;

// Tests assume itanium abi.

struct test1 {
    int x1;
    int x2;
    int x3;
};

static_assert(offset_of_member<test1, &test1::x1>() == 0,
              "offset of first member.");
static_assert(offset_of_member<test1, &test1::x2>() == sizeof(int),
              "offset of second member.");
static_assert(offset_of_member<test1, &test1::x3>() == 2*sizeof(int),
              "offset of third member.");

struct base1 {
    double x;
};

struct base2 {
    double y;
};

struct test2 : base1, base2 {
    double z;
};

static_assert(offset_of_member<test2, &base1::x>() == 0, "first member");
static_assert(offset_of_member<test2, &test2::x>() == 0, "first member");
static_assert(offset_of_member<test2, &base2::y>() == sizeof(double), "second member");
static_assert(offset_of_member<test2, &test2::y>() == sizeof(double), "second member");
static_assert(offset_of_member<test2, &test2::z>() == sizeof(double) * 2, "third member");

static_assert(offset_of_base<test2, base1>() == 0, "first base");
static_assert(offset_of_base<test2, base2>() == sizeof(base1), "first base");
