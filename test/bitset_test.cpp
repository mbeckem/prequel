#include <catch.hpp>

#include <extpp/detail/bitset.hpp>

using namespace extpp;
using namespace extpp::detail;

TEST_CASE("bitset set and find", "[bitset]") {
    bitset set;

    REQUIRE(set.size() == 0);
    REQUIRE(set.find_set(0) == bitset::npos);
    REQUIRE(set.find_unset(0) == bitset::npos);

    set.resize(333);
    REQUIRE(set.size() == 333);

    for (size_t i = 0; i < 333; ++i) {
        if (set.test(i))
            FAIL("Bit " << i << " should be unset.");
    }

    REQUIRE(set.find_unset(0) == 0);
    REQUIRE(set.find_unset(55) == 55);
    REQUIRE(set.find_unset(332) == 332);
    REQUIRE(set.find_unset(333) == bitset::npos);
    REQUIRE(set.find_set() == bitset::npos);

    set.set(0);
    REQUIRE(set.test(0));
    REQUIRE(set.find_set() == 0);
    REQUIRE(set.find_unset() == 1);

    set.set(33);
    REQUIRE(set.test(33));
    REQUIRE(set.find_set(0) == 0);
    REQUIRE(set.find_set(1) == 33);
    REQUIRE(set.find_unset(33) == 34);

    set.set(132);
    REQUIRE(set.find_set(34) == 132);

    set.clear();
    set.resize(12345);

    for (size_t i = 1000; i < 2000; ++i) {
        set.set(i);
        if (!set.test(i))
            FAIL("Bit is unset: " << i);
    }

    for (size_t i = 10000; i < 12000; ++i) {
        set.set(i);
        if (!set.test(i))
            FAIL("Bit is unset: " << i);
    }

    REQUIRE(set.find_set() == 1000);
    REQUIRE(set.find_set(1001) == 1001);
    REQUIRE(set.find_unset() == 0);
    REQUIRE(set.find_unset(1000) == 2000);
    REQUIRE(set.find_set(2000) == 10000);
    REQUIRE(set.find_unset(10000) == 12000);
    REQUIRE(set.find_set(12000) == bitset::npos);
}

TEST_CASE("bitset count", "[bitset]") {
    bitset b(12345);
    REQUIRE(b.count() == 0);

    b.set(117);
    REQUIRE(b.count() == 1);

    b.set(125);
    REQUIRE(b.count() == 2);
    REQUIRE(b.count(64, 53) == 0);
    REQUIRE(b.count(64, 54) == 1);
    REQUIRE(b.count(64, 61) == 1);
    REQUIRE(b.count(64, 62) == 2);

    REQUIRE(b.count(117, 0) == 0);
    REQUIRE(b.count(117, 1) == 1);
    REQUIRE(b.count(117, 8) == 1);
    REQUIRE(b.count(117, 9) == 2);

    b.reset();
    for (int i = 10; i < 64; ++i)
        b.set(i);
    for (int i = 64; i < 320; ++i)
        b.set(i);
    b.set(333);

    REQUIRE(b.count() == 311);
    REQUIRE(b.count(10) == 311);
    REQUIRE(b.count(11) == 310);
    REQUIRE(b.count(11, 334 - 11) == 310);
    REQUIRE(b.count(11, 334 - 12) == 309);

    b.reset();
    b.set(5);
    b.set(122);
    REQUIRE(b.count() == 2);
}
