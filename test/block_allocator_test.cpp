#include <catch.hpp>

#include <extpp/block_allocator.hpp>
#include <extpp/engine.hpp>
#include <extpp/io.hpp>

using namespace extpp;

constexpr u32 block_size = 4096;

using allocator_t = block_allocator<block_size>;

// TODO

//TEST_CASE("block allocator", "[block-allocator]") {
//    auto file = create_memory_file("memory");
//    file->truncate(block_size);

//    {
//        block_engine engine(*file, block_size, 16);
//        block_data<allocator_t::anchor> anchor = construct<allocator_t::anchor>(engine.read_zero(block_index(0)));
//        allocator_t allocator(anchor, engine, 2);

//        REQUIRE(file->file_size() == block_size);

//        auto i1 = allocator.allocate();
//        REQUIRE(file->file_size() == 3 * block_size);
//        REQUIRE(i1.value() == 1);

//        auto i2 = allocator.allocate();
//        REQUIRE(file->file_size() == 3 * block_size);
//        REQUIRE(i2.value() == 2);

//        allocator.free(i2);
//        allocator.free(i1);
//    }

//    {
//        block_engine engine(*file, block_size, 16);
//        block_data<allocator_t::anchor> anchor = cast<allocator_t::anchor>(engine.read(block_index(0)));
//        allocator_t allocator(anchor, engine, 3);

//        auto i1 = allocator.allocate();
//        REQUIRE(file->file_size() == block_size * 3);
//        REQUIRE(i1.value() == 1);

//        auto i2 = allocator.allocate();
//        REQUIRE(file->file_size() == block_size * 3);
//        REQUIRE(i2.value() == 2);

//        auto i3 = allocator.allocate();
//        REQUIRE(file->file_size() == block_size * 6);
//        REQUIRE(i3.value() == 3);
//    }
//}

//TEST_CASE("block allocator order", "[block-allocator]") {
//    const u32 block_size = 4096;

//    auto file = create_memory_file("memory");
//    file->truncate(block_size);
//    block_engine engine(*file, block_size, 16);
//    block_data<allocator_t::anchor> anchor = construct<allocator_t::anchor>(engine.read_zero(block_index(0)));
//    allocator_t allocator(anchor, engine);

//    std::vector<block_index> indices;

//    for (int i = 0; i < 10000; ++i) {
//        indices.push_back(allocator.allocate());
//    }
//    for (int i = 0; i < 10000; ++i) {
//        allocator.free(indices[i]);
//    }
//    for (int i = 0; i < 10000; ++i) {
//        auto expected = indices[10000 - 1 - i];
//        auto index = allocator.allocate();

//        if (expected != index) {
//            FAIL("expected index " << expected.value() << ", but got " << index.value());
//        }
//    }
//}
