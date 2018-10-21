#include <catch.hpp>

#include <prequel/exception.hpp>
#include <prequel/node_allocator.hpp>

#include "./test_file.hpp"

using namespace prequel;

TEST_CASE("node allocator", "[node-allocator]") {
    test_file file(512);

    {
        node_allocator::anchor anchor;
        node_allocator alloc(make_anchor_handle(anchor), file.get_engine());

        REQUIRE(alloc.block_size() == 512);
        REQUIRE(alloc.data_total() == 0);
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == 0);

        REQUIRE_THROWS_AS(alloc.allocate(2), unsupported);
        REQUIRE_THROWS_AS(alloc.allocate(9999), unsupported);

        auto a1 = alloc.allocate(1);
        auto a2 = alloc.allocate(1);
        REQUIRE(a1 + 1 == a2);
        REQUIRE(alloc.data_total() == alloc.chunk_size());
        REQUIRE(alloc.data_used() == 2);
        REQUIRE(alloc.data_free() == alloc.chunk_size() - 2);

        alloc.free(a1, 1);
        alloc.free(a2, 1);

        REQUIRE(alloc.data_total() == alloc.chunk_size());
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == alloc.data_total());
    }
}
