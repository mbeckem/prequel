#include <catch.hpp>

#include <extpp/file_engine.hpp>
#include <extpp/region_allocator.hpp>

#include <iostream>
#include <random>

using namespace extpp;

namespace {

constexpr u32 block_size = 256;

}

TEST_CASE("region allocator usage", "[region-allocator]") {
    constexpr u32 data_chunk = 32;
    constexpr u32 metadata_chunk = 16;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file_engine engine(*file, block_size, 16);
    engine.grow(256);

    region_allocator::anchor anchor;

    region_allocator alloc(make_anchor_handle(anchor), engine);
    alloc.min_chunk(data_chunk);
    alloc.min_meta_chunk(metadata_chunk);
    alloc.validate();

    REQUIRE(alloc.min_chunk() == data_chunk);
    REQUIRE(alloc.min_meta_chunk() == metadata_chunk);

    REQUIRE(!alloc.begin());
    REQUIRE(alloc.used() == 0);
    REQUIRE(alloc.size() == 0);

    alloc.initialize(block_index(0), engine.size());
    REQUIRE(alloc.begin().value() == 0);
    REQUIRE(alloc.size() == 256);
    REQUIRE(alloc.used() == 0);

    REQUIRE_THROWS_AS(alloc.initialize(block_index(0), engine.size()), invalid_argument); // Already initialized.

    block_index i1 = alloc.allocate(40);
    REQUIRE(alloc.used() == 64 + 16); // 1 Metadata and rounded up to pow2.
    REQUIRE(i1.value() == 16); // Metadata was allocated first.

    block_index i2 = alloc.allocate(1);
    REQUIRE(alloc.used() == 64 + 16); // Enough space remained.
    REQUIRE(i2.value() == (i1 + 40).value());
}
