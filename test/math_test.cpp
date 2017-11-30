#include <catch.hpp>

#include <extpp/math.hpp>

using namespace extpp;

TEST_CASE("round power of two", "[math]") {
    REQUIRE(round_towards_pow2(1u) == 1);
    REQUIRE(round_towards_pow2(2u) == 2);
    REQUIRE(round_towards_pow2(64u) == 64);

    REQUIRE(round_towards_pow2(3u) == 4);
    REQUIRE(round_towards_pow2(33u) == 64);
    REQUIRE(round_towards_pow2(100u) == 128);
    REQUIRE(round_towards_pow2(65535u) == 65536);
}

TEST_CASE("is pow2", "[math]") {
    for (u64 i = 0; i < 64; ++i) {
        u64 v = u64(1) << i;
        REQUIRE(is_pow2(v));
    }

    REQUIRE_FALSE(is_pow2(0u));
    REQUIRE_FALSE(is_pow2(3u));
    REQUIRE_FALSE(is_pow2(5u));
    REQUIRE_FALSE(is_pow2(11u));
    REQUIRE_FALSE(is_pow2(33u));
    REQUIRE_FALSE(is_pow2(10055u));
    REQUIRE_FALSE(is_pow2(456461u));
    REQUIRE_FALSE(is_pow2(13211324u));
    REQUIRE_FALSE(is_pow2(545723333u));
}

TEST_CASE("log2", "[math]") {
    for (u64 i = 0; i < 64; ++i) {
        u64 v = u64(1) << i;
        REQUIRE(extpp::log2(v) == i);
    }

    REQUIRE(extpp::log2(7u) == 2);
    REQUIRE(extpp::log2(9u) == 3);
    REQUIRE(extpp::log2(15u) == 3);
    REQUIRE(extpp::log2(16u) == 4);
    REQUIRE(extpp::log2(1025u) == 10);
    REQUIRE(extpp::log2(65535u) == 15);
}

TEST_CASE("overflow", "[math]") {
    using limits = std::numeric_limits<int>;

    REQUIRE(checked_add(1, 1) == 2);
    REQUIRE(checked_add(-1, -5) == -6);
    REQUIRE_THROWS(checked_add(limits::max(), 1));

    REQUIRE(checked_sub(5000, 4000) == 1000);
    REQUIRE_THROWS(checked_sub(limits::min(), 1000));

    REQUIRE(checked_mul(4, 12) == 48);
    REQUIRE_THROWS(checked_mul(limits::max(), 2));
}
