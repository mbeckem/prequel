#include <catch.hpp>

#include <prequel/id_generator.hpp>
#include <prequel/node_allocator.hpp>

#include "./test_file.hpp"

using namespace prequel;

static constexpr u32 block_size = 512;

TEST_CASE("ID generator", "[id_generator]") {
    test_file file(block_size);

    node_allocator::anchor anchor;
    node_allocator node_alloc(make_anchor_handle(anchor), file.get_engine());
    {
        id_generator::anchor gen_anchor;
        id_generator gen(make_anchor_handle(gen_anchor), node_alloc);

        SECTION("Sequential IDs") {
            REQUIRE(gen.allocate() == 1);
            REQUIRE(gen.allocate() == 2);
            REQUIRE(gen.allocate() == 3);
            REQUIRE(gen.allocate() == 4);
            REQUIRE(gen.allocate() == 5);
            REQUIRE(gen.allocate() == 6);

            REQUIRE(gen.max() == 6);
        }

        SECTION("IDs are reused") {
            gen.allocate(); // 1
            gen.allocate(); // 2
            gen.allocate(); // 3
            gen.allocate(); // 4
            gen.allocate(); // 5

            gen.free(2);
            gen.free(1);
            REQUIRE(gen.allocate() == 1);
            REQUIRE(gen.allocate() == 2);

            gen.free(4);
            gen.free(1);
            gen.free(2);
            gen.free(5); // 4-5 reduces "max"

            REQUIRE(gen.max() == 3);
            REQUIRE(gen.allocate() == 1);
            REQUIRE(gen.allocate() == 2);
            REQUIRE(gen.allocate() == 4);

            gen.free(1);
            gen.free(3);
            gen.free(2);
            gen.free(4);
            REQUIRE(gen.max() == 0);
        }
    }
}
