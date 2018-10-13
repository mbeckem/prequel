#include <catch.hpp>

#include <prequel/default_allocator.hpp>
#include <prequel/file_engine.hpp>

#include <iostream>
#include <random>

using namespace prequel;

namespace {

constexpr u32 block_size = 256;

}

TEST_CASE("default allocator", "[default-allocator]") {
    constexpr u32 data_chunk = 32;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file_engine engine(*file, block_size, 16);

    default_allocator::anchor anchor;

    default_allocator alloc(make_anchor_handle(anchor), engine);
    alloc.min_chunk(data_chunk);
    alloc.validate();
    alloc.can_grow(true);

    REQUIRE(alloc.min_chunk() == data_chunk);

    SECTION("simple alloc free") {
        auto a1 = alloc.allocate(1);
        REQUIRE(a1 == block_index(6)); // preallocates 6 blocks, then allocates data
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().data_used == 1);
        REQUIRE(alloc.stats().meta_data == 6);

        auto a2 = alloc.allocate(4);
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().data_used == 5);
        REQUIRE(a2 == a1 + 1);

        auto a3 = alloc.allocate(1);
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().data_used == 6);
        REQUIRE(a3 == a2 + 4);

        alloc.free(a2, 4);
        REQUIRE(alloc.stats().data_used == 2);
        REQUIRE(alloc.stats().data_free == data_chunk - 2 - 6);

        auto a4 = alloc.allocate(1);
        REQUIRE(a4 == a1 + 1);
        alloc.free(a4, 1);

        auto a5  = alloc.allocate(5);
        REQUIRE(a5 == a3 + 1);
        REQUIRE(alloc.stats().data_used == 7);

        alloc.validate();

        alloc.free(a1, 1);
        alloc.free(a3, 1);
        alloc.free(a5, 5);

        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free == data_chunk - 6);
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().meta_data == 6);

        alloc.validate();
    }

    SECTION("reallocate") {
        auto b1 = alloc.reallocate({}, 0, 500);
        REQUIRE(alloc.stats().data_total == 512 + 32); // Next pow2
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 12 + 32);

        auto b2 = alloc.reallocate(b1, 500, 501);
        REQUIRE(b2 == b1); // In place.
        REQUIRE(alloc.stats().data_total == 512 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 11 + 32);

        auto b3 = alloc.reallocate(b2, 501, 1000);
        REQUIRE(b3 == b2); // In place.
        REQUIRE(alloc.stats().data_total == 1024 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 24 + 32);

        // Just there to fragment:
        auto g1 = alloc.allocate(24);
        unused(g1);

        auto b4 = alloc.reallocate(b3, 1000, 1024);
        REQUIRE(b4 != b3); // Not in place because of "g1".
        REQUIRE(alloc.stats().data_total == 2048 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 1000 + 32);

        alloc.validate();

        auto b5 = alloc.reallocate(b4, 1024, 3024);
        REQUIRE(b5 == b4); // In place b4 was the last extent.
        REQUIRE(alloc.stats().data_total == 4096 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 1048 + 32); // 48 additional blocks b/c the +2k blocks are rounded to 2048.

        auto b6 = alloc.reallocate(b5, 3024, 3072);
        REQUIRE(b6 == b5);
        REQUIRE(alloc.stats().data_total == 4096 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 1000 + 32);

        // Just there to fragment:
        auto g2 = alloc.allocate(50);
        REQUIRE(alloc.stats().data_total == 4096 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 950 + 32);

        auto b7 = alloc.reallocate(b6, 3072, 1000);
        REQUIRE(b7 == b6);
        REQUIRE(alloc.stats().data_total == 4096 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 3022 + 32);

        auto b8 = alloc.reallocate(b7, 1000, 3072);
        REQUIRE(b8 == b7);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 950 + 32);

        auto g3 = alloc.reallocate(g2, 50, 100);
        REQUIRE(g3 == g2);
        REQUIRE(alloc.stats().data_total == 4096 + 32);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 900 + 32);

        alloc.validate();
    }

    SECTION("alloc/free with randomized freeing") {
        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free == 0);
        REQUIRE(alloc.stats().data_total == 0);

        std::vector<std::pair<block_index, u64>> allocs;
        for (int i = 1; i <= 512; ++i) {
            allocs.push_back(std::make_pair(alloc.allocate(i), i));
        }

        alloc.validate();

        REQUIRE(alloc.stats().data_used == 131328);
        REQUIRE(alloc.stats().data_total >= 131328);
        std::shuffle(allocs.begin(), allocs.end(), std::default_random_engine());

        for (auto& pair : allocs) {
            pair.first = alloc.reallocate(pair.first, pair.second, pair.second * 3);
            pair.second *= 3;
        }

        alloc.validate();

        for (auto pair : allocs) {
            alloc.free(pair.first, pair.second);
        }

        alloc.validate();

        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free >= 131328 * 3);
        REQUIRE(alloc.stats().data_total >= 131328 * 3);
    }

    SECTION("allocating after free reuses memory") {
        auto a1 = alloc.allocate(32);
        REQUIRE(alloc.stats().data_used == 32);
        REQUIRE(alloc.stats().data_total == 64);
        REQUIRE(alloc.stats().data_free + alloc.stats().meta_data == 32);

        alloc.free(a1, 32);
        REQUIRE(alloc.stats().data_used == 0);

        auto a2 = alloc.allocate(16);
        REQUIRE(a2 == a1);
        REQUIRE(alloc.stats().data_used == 16);
        REQUIRE(alloc.stats().data_total == 64);

        auto a3 = alloc.allocate(14);
        REQUIRE(a3 == a2 + 16);
        REQUIRE(alloc.stats().data_used == 30);

        auto a4 = alloc.allocate(3);
        REQUIRE(a4 == a3 + 14);
        REQUIRE(alloc.stats().data_total == 64);
        REQUIRE(alloc.stats().data_used == 33);

        alloc.free(a3, 14);
        auto a5 = alloc.reallocate(a2, 16, 30);
        REQUIRE(a5 == a2);
        REQUIRE(alloc.stats().data_used == 33);

        alloc.validate();
    }

    SECTION("reallocate reuses space from the right") {
        auto a1 = alloc.allocate(10);
        auto a2 = alloc.allocate(10);
        REQUIRE(a1 == a2 - 10);

        alloc.free(a2, 10);
        REQUIRE(alloc.stats().data_free == 16); // 6 metadata blocks used
        REQUIRE(alloc.stats().meta_data == 6);

        auto a3 = alloc.reallocate(a1, 10, 26);
        REQUIRE(a1 == a3);
        REQUIRE(alloc.stats().data_free == 0);

        alloc.validate();
    }

    SECTION("reallocate reuses space from the left") {
        auto a1 = alloc.allocate(24);
        auto a2 = alloc.allocate(2);
        REQUIRE(alloc.stats().data_free == 0); // 6 metadata blocks used
        REQUIRE(alloc.stats().meta_data == 6);

        alloc.free(a1, 24);

        auto a3 = alloc.reallocate(a2, 2, 3);
        REQUIRE(a3 == a1); // All the way to the left.
        REQUIRE(alloc.stats().data_free == 23);

        auto a4 = alloc.reallocate(a3, 3, 26);
        REQUIRE(a4 == a3);
        REQUIRE(alloc.stats().data_free == 0);

        alloc.validate();
    }

    SECTION("partial free") {
        auto a1 = alloc.allocate(50);
        alloc.free(a1 + 25, 25);

        auto a2 = alloc.allocate(25);
        REQUIRE(a1 + 25 == a2);

        u64 free_before = alloc.stats().data_free;
        alloc.free(a1, 25);
        alloc.free(a2, 25);
        REQUIRE(alloc.stats().data_free == free_before + 50);
    }
}

TEST_CASE("default allocator with custom region", "[default-allocator]") {
    constexpr u32 data_chunk = 32;
    constexpr u32 metadata_chunk = 16;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file_engine engine(*file, block_size, 16);
    engine.grow(1337);

    default_allocator::anchor anchor;
    default_allocator alloc(make_anchor_handle(anchor), engine);
    alloc.can_grow(false);
    alloc.add_region(block_index(50), 1337 - 50);

    {
        block_index i1 = alloc.allocate(1);
        REQUIRE(i1 == block_index(56)); // after the first 6 metadata blocks
        REQUIRE(alloc.stats().data_free == 1337 - 50 - 1 - 6);

        block_index i2 = alloc.allocate(alloc.stats().data_free);
        REQUIRE(i2 == block_index(57));

        REQUIRE(alloc.stats().data_total == 1337 - 50);
        REQUIRE(alloc.stats().data_free == 0);
        REQUIRE(alloc.stats().meta_data == 6);

        REQUIRE_THROWS_AS(alloc.allocate(1), prequel::bad_alloc);

        // Partial free
        alloc.free(block_index(1000), 1);
        REQUIRE(alloc.stats().data_free == 1);

        // Reuse freed block
        block_index i3 = alloc.allocate(1);
        REQUIRE(i3 == block_index(1000));
    }
}
