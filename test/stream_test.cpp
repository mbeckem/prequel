#include <catch.hpp>

#include <extpp/stream.hpp>

#include "./test_file.hpp"

using namespace extpp;

static constexpr u32 block_size = 512;

using stream_t = stream<int, block_size>;
static_assert(stream_t::block_capacity() == (block_size / sizeof(int)), "no space wasted.");

using file_t = test_file<stream_t::anchor, block_size>;

TEST_CASE("stream basics", "[stream]") {
    file_t file;
    file.open();

    stream_t stream(file.anchor(), file.engine(), file.alloc());

    SECTION("empty stream") {
        REQUIRE(stream.size() == 0);
        REQUIRE(stream.capacity() == 0);
        REQUIRE(stream.empty());
        REQUIRE(stream.begin() == stream.end());
        REQUIRE(stream.begin() - stream.end() == 0);
    }

    SECTION("stream grows when inserting") {
        for (int i = 0; i < 1000; ++i)
            stream.push_back(i);

        REQUIRE(stream.size() == 1000);
        REQUIRE(stream.capacity() >= 1000);
        REQUIRE(stream.end() - stream.begin() == 1000);
        REQUIRE(stream.begin() - stream.end() == -1000);
        REQUIRE(stream.begin() + 1000 == stream.end());
        REQUIRE(stream.end() - 1000 == stream.begin());

        int expected = 0;
        for (auto i = stream.begin(), e = stream.end(); i != e; ++i) {
            if (*i != expected)
                FAIL("Unexpected value " << *i << ", expected " << expected);
            ++expected;
        }

        for (int i = 0; i < 1000; ++i) {
            if (i != stream[i])
                FAIL("Unexpected value " << stream[i] << ", expected " << i);
        }

        for (int i = 0; i < 500; ++i)
            stream.pop_back();

        REQUIRE(stream[stream.size() - 1] == 499);
    }

    SECTION("stream reserve") {
        stream.reserve(5555);
        REQUIRE(stream.size() == 0);
        REQUIRE(stream.capacity() >= 5555);

        const u64 cap = stream.capacity();

        stream.reserve(5555);
        REQUIRE(cap == stream.capacity());

        stream.reserve(0);
        REQUIRE(cap == stream.capacity());
    }

    SECTION("mutation api") {
        stream.reserve(5000);
        for (int i = 0; i < 5000; ++i)
            stream.push_back(i);

        for (auto i = stream.begin(), e = stream.end(); i != e; ++i)
            stream.modify(i, [](int& v) { v *= 2; });

        for (int i = 0; i < 5000; ++i) {
            int expected = i * 2;
            if (stream[i] != expected)
                FAIL("Unexpected value " << stream[i] << ", expected " << expected);
        }

        for (auto i = stream.begin(), e = stream.end(); i != e; ++i)
            stream.replace(i, *i + 1);

        for (int i = 0; i < 5000; ++i) {
            int expected = i * 2 + 1;
            if (stream[i] != expected)
                FAIL("Unexpected value " << stream[i] << ", expected " << expected);
        }
    }
}

#include <iostream>

TEST_CASE("stream state is persistent", "[stream]") {
    file_t file;

    file.open();
    {
        stream_t stream(file.anchor(), file.engine(), file.alloc());
        stream.reserve(100000);
        for (int i = 0; i < 100000; ++i)
            stream.push_back(i);
    }
    file.close();

    file.open();
    {
        stream_t stream(file.anchor(), file.engine(), file.alloc());
        REQUIRE(stream.size() == 100000);
        for (int i = 0; i < 100000; ++i) {
            if (stream[i] != i)
                FAIL("Unexpected value " << stream[i] << ", expected " << i);
        }
    }
    file.close();
}
