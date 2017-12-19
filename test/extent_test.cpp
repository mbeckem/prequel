#include <catch.hpp>

#include <extpp/extent.hpp>

#include "./test_file.hpp"

#include <iostream>

using namespace extpp;

constexpr u32 block_size = 4096;

using extent_t = extent<block_size>;

struct header {
    std::array<extent_t::anchor, 20> extents;
};

TEST_CASE("extent", "[extent]") {
    test_file<header, block_size> file;
    file.open();

    auto extent_anchor = [&](u32 index) {
        EXTPP_ASSERT(index < file.get_anchor()->extents.size(), "index out of bounds");
        return file.get_anchor().neighbor(&file.get_anchor()->extents[index]);
    };

    SECTION("basic usage") {
        extent_t e1(extent_anchor(0), file.get_allocator());
        REQUIRE(e1.empty());
        REQUIRE(e1.size() == 0);

        e1.resize(2);
        REQUIRE(e1.size() == 2);
        REQUIRE(!e1.empty());
        REQUIRE(e1.data());
        REQUIRE(e1.get(0).get_block_index() + 1 == e1.get(1).get_block_index());
        {
            auto h = e1.zeroed(0);
            for (u32 i = 0; i < 256; ++i)
                h.data()[i] = byte(i);
            h = e1.zeroed(1);
            for (u32 i = 0; i < 256; ++i)
                h.data()[i] = byte(255 - i);
        }

        extent_t e2(extent_anchor(1), file.get_allocator());
        e2.resize(8);

        e1.resize(4);
        REQUIRE(e1.size() == 4);
        {
            auto h = e1.access(0);
            for (u32 i = 0; i < 256; ++i) {
                if (h.data()[i] != byte(i))
                    FAIL("Unexpected value at index " << i << ": " << h.data()[i]);
            }
            h = e1.access(1);
            for (u32 i = 0; i < 256; ++i) {
                if (h.data()[i] != byte(255 - i))
                    FAIL("Unexpected value at index " << i << ": " << h.data()[i]);
            }
        }

        e1.clear();
        e2.clear();
        REQUIRE(e1.size() == 0);
        REQUIRE(e1.empty());
        REQUIRE(!e1.data());
    }
}
