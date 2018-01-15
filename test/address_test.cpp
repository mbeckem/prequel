#include <catch.hpp>

#include <extpp/address.hpp>

#include <type_traits>

using namespace extpp;

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

TEST_CASE("instance <-> member", "[address]") {
    struct test_t {
        u32 a = 0;
        u16 b = 1;
        u16 c = 2;
        u64 d = 3;
        std::tuple<u8, u16> e{5, 6};
        u32 f[3] = {7, 8, 9};

        static constexpr auto get_binary_format() {
            return make_binary_format(&test_t::a, &test_t::b, &test_t::c,
                                      &test_t::d, &test_t::e, &test_t::f);
        }
    };

    address<test_t> base(raw_address(0));
    address<u32> a = base.member<&test_t::a>();
    address<u16> b = base.member<&test_t::b>();
    address<u16> c = base.member<&test_t::c>();
    address<u64> d = base.member<&test_t::d>();
    address<std::tuple<u8, u16>> e = base.member<&test_t::e>();
    address<u32[3]> f = base.member<&test_t::f>();

    REQUIRE(a.raw().value() == 0);
    REQUIRE(b.raw().value() == 4);
    REQUIRE(c.raw().value() == 6);
    REQUIRE(d.raw().value() == 8);
    REQUIRE(e.raw().value() == 16);
    REQUIRE(f.raw().value() == 19);

    REQUIRE(a.instance<&test_t::a>() == base);
    REQUIRE(b.instance<&test_t::b>() == base);
    REQUIRE(c.instance<&test_t::c>() == base);
    REQUIRE(d.instance<&test_t::d>() == base);
    REQUIRE(e.instance<&test_t::e>() == base);
    REQUIRE(f.instance<&test_t::f>() == base);
}
