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

static_assert(std::is_convertible<address<t2>, address<t1>>::value,
              "convertible child->parent.");
static_assert(!std::is_convertible<address<t1>, address<t2>>::value,
              "not convertible parent->child");

static_assert(std::is_convertible<address<t4>, address<t3>>::value,
              "convertible child->parent.");
static_assert(std::is_convertible<address<t4>, address<t2>>::value,
              "convertible child->parent.");
static_assert(std::is_convertible<address<t4>, address<t1>>::value,
              "convertible child->parent.");

static_assert(sizeof(address<t1>) == sizeof(u64), "Requires EBO.");

TEST_CASE("basic address casts", "[address]") {
    {
        address<t2> c = raw_address_cast<t2>(raw_address::byte_address(64));
        address<t1> p = c;
        REQUIRE(p.raw() == c.raw());
    }

    {
        address<t4> c = raw_address_cast<t4>(raw_address::byte_address(64));
        address<t3> p1 = c;
        address<t2> p2 = c;

        REQUIRE(p1.raw() == c.raw());
        REQUIRE(p2.raw() == c.raw() + sizeof(int)); // Itanium abi
    }

    {
        address<t2> c1 = raw_address_cast<t2>(raw_address::byte_address(64));
        address<t4> c2 = address_cast<t4>(c1);

        REQUIRE(c2.raw() == c1.raw() - sizeof(int)); // Itanium abi
    }

    {
        address<t4> a1;
        address<t2> a2 = a1;

        REQUIRE(!a1);
        REQUIRE(!a2);
    }
}

TEST_CASE("address comparisons", "[address]") {
    raw_address a1;
    raw_address a2(128);
    raw_address a3(256 + 5);
    raw_address a4(1024 + 1);

    REQUIRE(a1 < a2);
    REQUIRE(a1 < a3);
    REQUIRE(a1 < a4);

    REQUIRE(!(a1 >= a2));
    REQUIRE(!(a1 >= a3));
    REQUIRE(!(a1 >= a4));

    REQUIRE(a2 < a3);
    REQUIRE(a2 < a4);

    REQUIRE(a3 < a4);
}
