#include <catch.hpp>

#include <extpp/extent.hpp>
#include <extpp/node_allocator.hpp>

#include "./test_file.hpp"

#include <iostream>

using namespace extpp;

constexpr u32 block_size = 4096;

namespace {

struct header {
    node_allocator::anchor alloc;
    std::array<extent::anchor, 20> extents;

    static constexpr auto get_binary_format() {
        return make_binary_format(&header::alloc, &header::extents);
    }
};

}

// TODO: Does not pass yet because the allocator is missing.
TEST_CASE("extent", "[extent][!mayfail]") {
    test_file<header> file(4096);
    file.open();

    // TODO real alloc
    node_allocator alloc(file.get_anchor().member<&header::alloc>(), file.get_engine());

    auto extent_anchor = [&](u32 index) {
        EXTPP_ASSERT(index < file.get_anchor().get<&header::extents>().size(), "index out of bounds");

        auto hdr = file.get_anchor();
        u32 offset = serialized_offset<&header::extents>() + index * serialized_size<extent::anchor>();
        return handle<extent::anchor>(hdr.block(), hdr.offset() + offset);
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
