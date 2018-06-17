#include <catch.hpp>

#include <extpp/detail/free_list.hpp>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 block_size = 512;

using free_list_t = detail::free_list;

TEST_CASE("freelist", "[freelist]") {
    test_file file(block_size);
    file.open();

    REQUIRE(file.get_engine().size() == 0);

    file.get_engine().grow(1024);

    free_list_t::anchor anchor{};

    // block indices [1, 1024] are valid.
    free_list_t list(make_anchor_handle(anchor), file.get_engine());

    REQUIRE(list.empty());
    REQUIRE_THROWS_AS(list.pop(), std::logic_error);

    for (u32 i = 1; i <= 1024; ++i) {
        list.push(block_index(i));
    }

    for (u32 i = 1024; i >= 1; --i) {
        auto addr = list.pop();
        auto expected = block_index(i);
        if (addr != expected)
            FAIL("Expected address " << expected << " but saw " << addr);
    }

    REQUIRE(list.empty());
    REQUIRE_THROWS_AS(list.pop(), std::logic_error);
}
