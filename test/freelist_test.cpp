#include <catch.hpp>

#include <extpp/detail/free_list.hpp>

#include "./test_file.hpp"

using namespace extpp;
using namespace detail;

constexpr u32 block_size = 512;

using list_t = free_list<block_size>;
using file_t = test_file<list_t::anchor, block_size>;

TEST_CASE("freelist", "[freelist]") {
    file_t file;
    file.open();

    extpp::file& fd = file.engine().fd();
    REQUIRE(fd.file_size() == block_size);

    fd.truncate(block_size * 1025);

    // block indices [1, 1024] are valid.
    list_t list(file.anchor(), file.engine());

    REQUIRE(list.empty());
    REQUIRE_THROWS_AS(list.pop(), std::logic_error);

    for (u32 i = 1; i <= 1024; ++i) {
        list.push(raw_address<block_size>::from_block(i));
    }

    for (u32 i = 1024; i >= 1; --i) {
        auto addr = list.pop();
        auto expected = raw_address<block_size>::from_block(i);
        if (addr != expected)
            FAIL("Expected address " << expected << " but saw " << addr);
    }

    REQUIRE(list.empty());
    REQUIRE_THROWS_AS(list.pop(), std::logic_error);
}
