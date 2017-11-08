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
        EXTPP_ASSERT(index < file.anchor()->extents.size(), "index out of bounds");
        return file.anchor().neighbor(&file.anchor()->extents[index]);
    };

    {
        extent_t e1(extent_anchor(0), file.engine(), file.alloc());
        REQUIRE(e1.empty());
        REQUIRE(e1.size() == 0);

        e1.resize(2);
        REQUIRE(e1.size() == 2);
        REQUIRE(!e1.empty());
        {
            auto h = e1.overwrite(0);
            for (u32 i = 0; i < 256; ++i)
                h.data()[i] = byte(i);
            h = e1.overwrite(1);
            for (u32 i = 0; i < 256; ++i)
                h.data()[i] = byte(255 - i);
        }

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

        e1.resize(400);
        REQUIRE(e1.size() == 400);

        e1.clear();
        REQUIRE(e1.size() == 0);
        REQUIRE(e1.empty());
        REQUIRE(!e1.data());
    }
}
