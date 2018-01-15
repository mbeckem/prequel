#include <catch.hpp>

#include <extpp/exception.hpp>
#include <extpp/node_allocator.hpp>

#include "./test_file.hpp"

using namespace extpp;

using file_t = test_file<node_allocator::anchor>;

TEST_CASE("node allocator", "[node-allocator]") {
    file_t file(512);
    file.open();
    {
        node_allocator alloc(file.get_anchor(), file.get_engine());

        REQUIRE(alloc.block_size() == 512);
        REQUIRE(alloc.data_total() == 0);
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == 0);

        REQUIRE_THROWS_AS(alloc.allocate(2), unsupported);
        REQUIRE_THROWS_AS(alloc.allocate(9999), unsupported);

        auto a1 = alloc.allocate(1);
        auto a2 = alloc.allocate(1);
        REQUIRE(a1 + alloc.block_size() == a2);
        REQUIRE(alloc.data_total() == alloc.chunk_size());
        REQUIRE(alloc.data_used() == 2);
        REQUIRE(alloc.data_free() == alloc.chunk_size() - 2);

        alloc.free(a1);
        alloc.free(a2);

        REQUIRE(alloc.data_total() == alloc.chunk_size());
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == alloc.data_total());
    }
}
