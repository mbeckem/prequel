#include <catch.hpp>

#include <extpp/default_allocator.hpp>
#include <extpp/extent.hpp>

#include "./test_file.hpp"

#include <iostream>

using namespace extpp;


namespace {

constexpr u32 block_size = 4096;

}

TEST_CASE("extent", "[extent]") {
    test_file file(4096);
    file.open();

    default_allocator::anchor alloc_anchor;
    std::vector<extent::anchor> extent_anchors(20);

    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    auto extent_anchor = [&](u32 index) {
        return make_anchor_handle(extent_anchors[index]);
    };

    SECTION("basic usage") {
        extent e1(extent_anchor(0), alloc);
        REQUIRE(e1.empty());
        REQUIRE(e1.size() == 0);

        e1.resize(2);
        REQUIRE(e1.size() == 2);
        REQUIRE(!e1.empty());
        REQUIRE(e1.data());
        REQUIRE(e1.get(0) + 1 == e1.get(1));
        {
            auto h = e1.zeroed(0);
            auto data = h.writable_data();
            for (u32 i = 0; i < 256; ++i)
                data[i] = byte(i);

            h = e1.zeroed(1);
            data = h.writable_data();
            for (u32 i = 0; i < 256; ++i)
                data[i] = byte(255 - i);
        }

        extent e2(extent_anchor(1), alloc);
        e2.resize(8);

        e1.resize(4);
        REQUIRE(e1.size() == 4);
        {
            auto h = e1.read(0);
            for (u32 i = 0; i < 256; ++i) {
                if (h.data()[i] != byte(i))
                    FAIL("Unexpected value at index " << i << ": " << h.data()[i]);
            }
            h = e1.read(1);
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
