#include <catch.hpp>

#include <extpp/default_allocator.hpp>

#include <iostream>
#include <random>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 bs = 1024;
using alloc_t = default_allocator<bs>;
using engine_t = engine<bs>;

TEST_CASE("default allocator", "[default-allocator]") {
    constexpr u32 data_chunk = 32;
    constexpr u32 metadata_chunk = 16;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file->truncate(bs);

    engine_t engine(*file, 16);
    auto anchor = construct<alloc_t::anchor>(engine, raw_address::block_address(block_index(0), bs));

    alloc_t alloc(anchor, engine);
    alloc.min_chunk(data_chunk);
    alloc.min_meta_chunk(metadata_chunk);

    SECTION("simple alloc free") {
        auto a1 = alloc.allocate(1);
        REQUIRE(alloc.data_total() == data_chunk);
        REQUIRE(alloc.metadata_total() == metadata_chunk);

        auto a2 = alloc.allocate(4);
        REQUIRE(alloc.data_total() == data_chunk);
        REQUIRE(alloc.data_used() == 5);
        REQUIRE(a2.get_block_index(bs) == a1.get_block_index(bs) + 1);

        auto a3 = alloc.allocate(1);
        REQUIRE(alloc.data_used() == 6);
        REQUIRE(a3.get_block_index(bs) == a2.get_block_index(bs) + 4);

        alloc.free(a2);
        REQUIRE(alloc.data_used() == 2);
        REQUIRE(alloc.data_free() == data_chunk - 2);

        auto a4 = alloc.allocate(1);
        REQUIRE(a4.get_block_index(bs) == a1.get_block_index(bs) + 1);
        alloc.free(a4);

        auto a5  = alloc.allocate(5);
        REQUIRE(a5.get_block_index(bs) == a3.get_block_index(bs) + 1);
        REQUIRE(alloc.data_used() == 7);

        alloc.free(a1);
        alloc.free(a3);
        alloc.free(a5);

        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == data_chunk);
    }

    SECTION("reallocate") {
        auto b1 = alloc.reallocate({}, 500);
        REQUIRE(alloc.data_total() == 512); // Next pow2
        REQUIRE(alloc.data_free() == 12);

        auto b2 = alloc.reallocate(b1, 512);
        REQUIRE(b2 == b1); // In place.
        REQUIRE(alloc.data_total() == 512);
        REQUIRE(alloc.data_free() == 0);

        auto b3 = alloc.reallocate(b2, 1000);
        REQUIRE(b3 == b2); // In place.
        REQUIRE(alloc.data_total() == 1024);
        REQUIRE(alloc.data_free() == 24);

        auto g1 = alloc.allocate(24);
        unused(g1);

        auto b4 = alloc.reallocate(b3, 1024);
        REQUIRE(b4 != b3); // Not in place because of "g1".
        REQUIRE(alloc.data_total() == 2048);
        REQUIRE(alloc.data_free() == 1000);

        auto b5 = alloc.reallocate(b4, 3024);
        REQUIRE(b5 == b4); // In place, we are the last extent.
        REQUIRE(alloc.data_total() == 4096);
        REQUIRE(alloc.data_free() == 1048); // 48 additional blocks b/c the +2k blocks are rounded to 2048.

        auto b6 = alloc.reallocate(b5, 3072);
        REQUIRE(b6 == b5);
        REQUIRE(alloc.data_total() == 4096);
        REQUIRE(alloc.data_free() == 1000);

        auto g2 = alloc.allocate(50);
        REQUIRE(alloc.data_total() == 4096);
        REQUIRE(alloc.data_free() == 950);

        auto b7 = alloc.reallocate(b6, 1000);
        REQUIRE(b7 == b6);
        REQUIRE(alloc.data_free() == 3022);

        auto b8 = alloc.reallocate(b7, 3072);
        REQUIRE(b8 == b7);
        REQUIRE(alloc.data_free() == 950);

        auto g3 = alloc.reallocate(g2, 100);
        REQUIRE(g3 == g2);
        REQUIRE(alloc.data_free() == 900);
        REQUIRE(alloc.data_total() == 4096);
    }

    SECTION("alloc/free with randomized freeing") {
        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() == 0);
        REQUIRE(alloc.data_total() == 0);

        std::vector<raw_address> allocs;
        for (int i = 1; i <= 2000; ++i) {
            allocs.push_back(alloc.allocate(i));
        }

        REQUIRE(alloc.data_used() == 2001000);
        REQUIRE(alloc.data_total() >= 2001000);

        std::shuffle(allocs.begin(), allocs.end(), std::default_random_engine());

        for (auto addr : allocs)
            alloc.free(addr);

        REQUIRE(alloc.data_used() == 0);
        REQUIRE(alloc.data_free() >= 2001000);
        REQUIRE(alloc.data_total() >= 2001000);
    }

    SECTION("allocating after free reuses memory") {
        alloc.min_chunk(16);

        auto a1 = alloc.allocate(32);
        REQUIRE(alloc.data_total() == 32);
        alloc.free(a1);
        REQUIRE(alloc.data_used() == 0);

        auto a2 = alloc.allocate(16);
        REQUIRE(a2 == a1);
        REQUIRE(alloc.data_used() == 16);
        REQUIRE(alloc.data_total() == 32);

        auto a3 = alloc.allocate(14);
        REQUIRE(a3.get_block_index(bs) == a2.get_block_index(bs) + 16);
        REQUIRE(alloc.data_used() == 30);

        auto a4 = alloc.allocate(3);
        REQUIRE(a4.get_block_index(bs) == a3.get_block_index(bs) + 14);
        REQUIRE(alloc.data_total() == 48);
        REQUIRE(alloc.data_used() == 33);

        alloc.free(a3);
        auto a5 = alloc.reallocate(a2, 30);
        REQUIRE(a5 == a2);
        REQUIRE(alloc.data_used() == 33);
    }
}
