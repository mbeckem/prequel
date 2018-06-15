#include <catch.hpp>

#include <extpp/exception.hpp>
#include <extpp/node_allocator.hpp>

#include "./test_file.hpp"

using namespace extpp;

TEST_CASE("node allocator", "[node-allocator]") {
    test_file file(512);
    file.open();

    auto anchor = make_anchor_handle(node_allocator::anchor());
    {
        node_allocator alloc(anchor, file.get_engine());

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

        alloc.free(a1);
        alloc.free(a2);

        REQUIRE(alloc.data_total() == alloc.chunk_size());
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == alloc.data_total());
    }
}
