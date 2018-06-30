#include <catch.hpp>

#include <extpp/default_allocator.hpp>
#include <extpp/file_engine.hpp>

#include <iostream>
#include <random>

using namespace extpp;

namespace {

constexpr u32 block_size = 256;

}

TEST_CASE("default allocator", "[default-allocator]") {
    constexpr u32 data_chunk = 32;
    constexpr u32 metadata_chunk = 16;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file_engine engine(*file, block_size, 16);

    default_allocator::anchor anchor;

    default_allocator alloc(make_anchor_handle(anchor), engine);
    alloc.min_chunk(data_chunk);
    alloc.min_meta_chunk(metadata_chunk);
    alloc.validate();

    REQUIRE(alloc.min_chunk() == data_chunk);
    REQUIRE(alloc.min_meta_chunk() == metadata_chunk);

    SECTION("simple alloc free") {
        auto a1 = alloc.allocate(1);
        REQUIRE(a1 == block_index(16)); // 16 metadata blocks, data block.
        REQUIRE(alloc.allocated_size(a1) == 1);
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().metadata_total == metadata_chunk);

        auto a2 = alloc.allocate(4);
        REQUIRE(alloc.allocated_size(a2) == 4);
        REQUIRE(alloc.stats().data_total == data_chunk);
        REQUIRE(alloc.stats().data_used == 5);
        REQUIRE(a2 == a1 + 1);

        auto a3 = alloc.allocate(1);
        REQUIRE(alloc.allocated_size(a3) == 1);
        REQUIRE(alloc.stats().data_used == 6);
        REQUIRE(a3 == a2 + 4);

        alloc.free(a2);
        REQUIRE(alloc.stats().data_used == 2);
        REQUIRE(alloc.stats().data_free == data_chunk - 2);

        auto a4 = alloc.allocate(1);
        REQUIRE(alloc.allocated_size(a4) == 1);
        REQUIRE(a4 == a1 + 1);
        alloc.free(a4);

        auto a5  = alloc.allocate(5);
        REQUIRE(alloc.allocated_size(a5) == 5);
        REQUIRE(a5 == a3 + 1);
        REQUIRE(alloc.stats().data_used == 7);

        alloc.free(a1);
        alloc.free(a3);
        alloc.free(a5);

        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free == data_chunk);
    }

    SECTION("reallocate") {
        auto b1 = alloc.reallocate({}, 500);
        REQUIRE(alloc.allocated_size(b1) == 500);
        REQUIRE(alloc.stats().data_total == 512); // Next pow2
        REQUIRE(alloc.stats().data_free == 12);

        auto b2 = alloc.reallocate(b1, 512);
        REQUIRE(b2 == b1); // In place.
        REQUIRE(alloc.allocated_size(b2) == 512);
        REQUIRE(alloc.stats().data_total == 512);
        REQUIRE(alloc.stats().data_free == 0);

        auto b3 = alloc.reallocate(b2, 1000);
        REQUIRE(b3 == b2); // In place.
        REQUIRE(alloc.allocated_size(b3) == 1000);
        REQUIRE(alloc.stats().data_total == 1024);
        REQUIRE(alloc.stats().data_free == 24);

        auto g1 = alloc.allocate(24);
        unused(g1);

        auto b4 = alloc.reallocate(b3, 1024);
        REQUIRE(b4 != b3); // Not in place because of "g1".
        REQUIRE(alloc.allocated_size(b4) == 1024);
        REQUIRE(alloc.stats().data_total == 2048);
        REQUIRE(alloc.stats().data_free == 1000);

        alloc.validate();

        auto b5 = alloc.reallocate(b4, 3024);
        REQUIRE(b5 == b4); // In place, we are the last extent.
        REQUIRE(alloc.allocated_size(b5) == 3024);
        REQUIRE(alloc.stats().data_total == 4096);
        REQUIRE(alloc.stats().data_free == 1048); // 48 additional blocks b/c the +2k blocks are rounded to 2048.

        auto b6 = alloc.reallocate(b5, 3072);
        REQUIRE(b6 == b5);
        REQUIRE(alloc.stats().data_total == 4096);
        REQUIRE(alloc.stats().data_free == 1000);

        auto g2 = alloc.allocate(50);
        REQUIRE(alloc.stats().data_total == 4096);
        REQUIRE(alloc.stats().data_free == 950);

        auto b7 = alloc.reallocate(b6, 1000);
        REQUIRE(b7 == b6);
        REQUIRE(alloc.stats().data_free == 3022);

        auto b8 = alloc.reallocate(b7, 3072);
        REQUIRE(b8 == b7);
        REQUIRE(alloc.stats().data_free == 950);

        auto g3 = alloc.reallocate(g2, 100);
        REQUIRE(g3 == g2);
        REQUIRE(alloc.allocated_size(g3) == 100);
        REQUIRE(alloc.stats().data_free == 900);
        REQUIRE(alloc.stats().data_total == 4096);

        alloc.validate();
    }

    SECTION("alloc/free with randomized freeing") {
        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free == 0);
        REQUIRE(alloc.stats().data_total == 0);

        std::vector<block_index> allocs;
        for (int i = 1; i <= 2000; ++i) {
            allocs.push_back(alloc.allocate(i));
        }

        alloc.validate();

        REQUIRE(alloc.stats().data_used == 2001000);
        REQUIRE(alloc.stats().data_total >= 2001000);
        std::shuffle(allocs.begin(), allocs.end(), std::default_random_engine());

        int index = 0;
        for (auto& addr : allocs) {
            addr = alloc.reallocate(addr, alloc.allocated_size(addr) * 2);
            ++index;
        }

        alloc.validate();

        for (auto addr : allocs)
            alloc.free(addr);

        alloc.validate();

        REQUIRE(alloc.stats().data_used == 0);
        REQUIRE(alloc.stats().data_free >= 2001000 * 2);
        REQUIRE(alloc.stats().data_total >= 2001000 * 2);
    }

    SECTION("allocating after free reuses memory") {
        alloc.min_chunk(16);

        auto a1 = alloc.allocate(32);
        REQUIRE(alloc.stats().data_total == 32);
        alloc.free(a1);
        REQUIRE(alloc.stats().data_used == 0);

        auto a2 = alloc.allocate(16);
        REQUIRE(a2 == a1);
        REQUIRE(alloc.stats().data_used == 16);
        REQUIRE(alloc.stats().data_total == 32);

        auto a3 = alloc.allocate(14);
        REQUIRE(a3 == a2 + 16);
        REQUIRE(alloc.stats().data_used == 30);

        auto a4 = alloc.allocate(3);
        REQUIRE(a4 == a3 + 14);
        REQUIRE(alloc.stats().data_total == 48);
        REQUIRE(alloc.stats().data_used == 33);

        alloc.free(a3);
        auto a5 = alloc.reallocate(a2, 30);
        REQUIRE(a5 == a2);
        REQUIRE(alloc.stats().data_used == 33);

        alloc.validate();
    }

    SECTION("reallocate reuses space from the right") {
        alloc.min_chunk(32);

        auto a1 = alloc.allocate(10);
        auto a2 = alloc.allocate(10);
        alloc.free(a2);
        REQUIRE(alloc.stats().data_free == 22);

        auto a3 = alloc.reallocate(a1, 32);
        REQUIRE(a1 == a3);
        REQUIRE(alloc.allocated_size(a3) == 32);
        REQUIRE(alloc.stats().data_free == 0);
    }

    SECTION("reallocate reuses space from the left") {
        alloc.min_chunk(32);

        auto a1 = alloc.allocate(30);
        auto a2 = alloc.allocate(2);
        REQUIRE(alloc.stats().data_free == 0);

        alloc.free(a1);
        auto a3 = alloc.reallocate(a2, 3);
        REQUIRE(a3 == a1); // All the way to the left.
        REQUIRE(alloc.allocated_size(a3) == 3);
        REQUIRE(alloc.stats().data_free == 29);

        auto a4 = alloc.reallocate(a3, 32);
        REQUIRE(a4 == a3);
        REQUIRE(alloc.allocated_size(a4) == 32);
        REQUIRE(alloc.stats().data_free == 0);

        alloc.validate();
    }
}

namespace {

struct test_block_source : block_source {
    block_index m_begin;
    u64 m_size = 0;
    u64 m_capacity = 0;

public:
    block_index begin() override { return m_begin; }
    u64 available() override { return m_capacity - m_size; }
    u64 size() override { return m_size; }

    void grow(u64 n) override {
        if (n > available()) {
            EXTPP_THROW(bad_alloc("Not enough space left in test_block_source."));
        }

        m_size += n;
    }
};

} // namespace

TEST_CASE("default allocator with custom block source", "[default-allocator]") {
    constexpr u32 data_chunk = 32;
    constexpr u32 metadata_chunk = 16;

    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file_engine engine(*file, block_size, 16);
    engine.grow(1337);

    test_block_source source;
    source.m_begin = block_index(50);
    source.m_capacity = 987;

    default_allocator::anchor anchor;

    default_allocator alloc(make_anchor_handle(anchor), engine, source);
    alloc.min_chunk(data_chunk);
    alloc.min_meta_chunk(metadata_chunk);
    {
        block_index i1 = alloc.allocate(1);
        REQUIRE(i1 == block_index(66)); // after the first 16 metadata blocks
        REQUIRE(source.size() == 48);   // 32 data, 16 metadata

        alloc.allocate(alloc.stats().data_free);
        REQUIRE(source.size() == 48); // No change.

        REQUIRE_THROWS_AS(alloc.allocate(source.available() + 1), extpp::bad_alloc);
    }
}
