#include <catch.hpp>

#include <extpp/detail/bitset.hpp>

using namespace extpp;
using namespace extpp::detail;

TEST_CASE("bitset", "[bitset]") {
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
        // TODO: Bulk set.
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
