#include <catch.hpp>

#include <extpp/address.hpp>

#include <type_traits>

using namespace extpp;

struct t1 {
    int x;
};

struct t2 : t1 {
    int y;
};

struct t3 {
    int z;
};

struct t4 : t3, t2 {
    int a;
};

constexpr u32 bs = 128;

static_assert(std::is_convertible<address<t2, bs>, address<t1, bs>>::value,
              "convertible child->parent.");
static_assert(!std::is_convertible<address<t1, bs>, address<t2, bs>>::value,
              "not convertible parent->child");

static_assert(std::is_convertible<address<t4, bs>, address<t3, bs>>::value,
              "convertible child->parent.");
static_assert(std::is_convertible<address<t4, bs>, address<t2, bs>>::value,
              "convertible child->parent.");
static_assert(std::is_convertible<address<t4, bs>, address<t1, bs>>::value,
              "convertible child->parent.");

TEST_CASE("basic address casts", "[address]") {
    {
        address<t2, bs> c = address_cast<t2>(raw_address<bs>(64));
        address<t1, bs> p = c;
        REQUIRE(p.raw() == c.raw());
    }

    {
        address<t4, bs> c = address_cast<t4>(raw_address<bs>(64));
        address<t3, bs> p1 = c;
        address<t2, bs> p2 = c;

        REQUIRE(p1.raw() == c.raw());
        REQUIRE(p2.raw() == c.raw() + sizeof(int)); // Itanium abi
    }

    {
        address<t2, bs> c1 = address_cast<t2>(raw_address<bs>(64));
        address<t4, bs> c2 = address_cast<t4>(c1);

        REQUIRE(c2.raw() == c1.raw() - sizeof(int)); // Itanium abi
    }

    {
        address<t4, bs> a1;
        address<t2, bs> a2 = a1;

        REQUIRE(!a1);
        REQUIRE(!a2);
    }
}
