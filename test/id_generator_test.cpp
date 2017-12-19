#include <catch.hpp>

#include <extpp/id_generator.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 block_size = 512;

using generator_t = id_generator<u64, block_size>;
using file_t = test_file<generator_t::anchor, block_size>;

TEST_CASE("ID generator", "[id_generator]") {
    file_t file;
    file.open();
    {
        generator_t gen(file.get_anchor(), file.get_allocator());

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
    file.close();
}
