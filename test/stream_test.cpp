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

    SECTION("resizing") {
        stream.resize(12345, 1122334455);

        REQUIRE(stream.size() == 12345);
        for (auto i : stream) {
            if (i != 1122334455)
                FAIL("Unexpected value: " << i);
        }

        stream.resize(123);
        REQUIRE(stream.size() == 123);

        stream.resize(123456);
        REQUIRE(stream.size() == 123456);
        for (u64 i = 0; i < 123; ++i) {
            if (stream[i] != 1122334455)
                FAIL("Unexpected value: " << stream[i]);
        }

        for (u64 i = 123; i < 123456; ++i) {
            if (stream[i] != 0)
                FAIL("Unexpected value: " << stream[i]);
        }
    }
}

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

TEST_CASE("customizable stream growth", "[stream]") {
    file_t file;
    file.open();
    {
        stream_t stream(file.anchor(), file.engine(), file.alloc());
        REQUIRE(std::holds_alternative<exponential_growth>(stream.growth()));

        SECTION("exponential") {
            stream.resize(1);
            REQUIRE(stream.blocks() == 1);

            stream.resize(stream.block_capacity() * 10);
            REQUIRE(stream.blocks() == 16);

            stream.resize(stream.block_capacity() * 127);
            REQUIRE(stream.blocks() == 128);
            REQUIRE(stream.capacity() == stream.blocks() * stream.block_capacity());
        }

        SECTION("linear") {
            stream.growth(linear_growth(5));

            stream.resize(0);
            REQUIRE(stream.blocks() == 0);

            stream.resize(1);
            REQUIRE(stream.blocks() == 5);
            REQUIRE(stream.capacity() == stream.block_capacity() * 5);

            stream.resize(24 * stream.block_capacity());
            REQUIRE(stream.blocks() == 25);

            stream.growth(linear_growth(1));
            stream.resize(101 * stream.block_capacity());
            REQUIRE(stream.blocks() == 101);

            stream.growth(linear_growth(12345));
            stream.resize(101 * stream.block_capacity() + 1);
            REQUIRE(stream.blocks() == (12345 + 101));
        }
    }
    file.close();
}
