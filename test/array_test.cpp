#include <catch.hpp>

#include <prequel/container/array.hpp>
#include <prequel/container/default_allocator.hpp>
#include <prequel/exception.hpp>
#include <prequel/formatting.hpp>

#include "./test_file.hpp"

using namespace prequel;

static constexpr u32 block_size = 512;

using array_t = array<i32>;

TEST_CASE("array basics", "[array]") {
    test_file file(block_size);

    default_allocator::anchor alloc_anchor;
    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    array_t::anchor array_anchor;
    array_t array(make_anchor_handle(array_anchor), alloc);

    SECTION("array wastes no space") {
        REQUIRE(array.block_capacity() == block_size / serialized_size<i32>());
    }

    SECTION("empty array") {
        REQUIRE(array.size() == 0);
        REQUIRE(array.capacity() == 0);
        REQUIRE(array.empty());
        REQUIRE_THROWS_AS(array.get(0), bad_argument);
        REQUIRE_THROWS_AS(array.set(0, 1), bad_argument);
    }

    SECTION("array grows when inserting") {
        for (int i = 0; i < 1000; ++i)
            array.push_back(i);

        REQUIRE(array.size() == 1000);
        REQUIRE(array.capacity() >= 1000);

        for (i32 i = 0; i < 1000; ++i) {
            i32 v = array[u64(i)];
            if (i != v)
                FAIL("Unexpected value " << v << ", expected " << i);
        }

        for (int i = 0; i < 500; ++i)
            array.pop_back();

        REQUIRE(array[array.size() - 1] == 499);
    }

    SECTION("array reserve") {
        array.reserve(5555);
        REQUIRE(array.size() == 0);
        REQUIRE(array.capacity() >= 5555);

        const u64 cap = array.capacity();

        array.reserve(5555);
        REQUIRE(cap == array.capacity());

        array.reserve(0);
        REQUIRE(cap == array.capacity());
    }

    SECTION("mutate array") {
        array.reserve(5000);
        for (i32 i = 0; i < 5000; ++i)
            array.push_back(i);

        for (i32 i = 0; i < 5000; ++i)
            array.set(u64(i), array.get(u64(i)) * 2);

        for (i32 i = 0; i < 5000; ++i) {
            i32 expected = i * 2;
            i32 observed = array[u64(i)];
            if (observed != expected)
                FAIL("Unexpected value " << observed << ", expected " << expected);
        };
    }

    SECTION("resizing") {
        REQUIRE(array.empty());

        array.resize(12345, 1122334455);

        REQUIRE(array.size() == 12345);
        for (i32 i = 0, e = array.size(); i != e; ++i) {
            if (array[u64(i)] != 1122334455)
                FAIL("Unexpected value: " << array[u64(i)]);
        }

        array.resize(123);
        REQUIRE(array.size() == 123);
        REQUIRE(array.capacity() >= 123);

        array.resize(123456);
        REQUIRE(array.size() == 123456);
        REQUIRE(array.capacity() >= 123456);
        for (u64 i = 0; i < 123; ++i) {
            if (array[i] != 1122334455)
                FAIL("Unexpected value: " << array[i]);
        }

        for (u64 i = 123; i < 123456; ++i) {
            if (array[i] != 0)
                FAIL("Unexpected value: " << array[i]);
        }
    }
}

TEST_CASE("array state is persistent", "[array]") {
    test_file file(block_size);

    default_allocator::anchor alloc_anchor;
    array_t::anchor array_anchor;

    {
        default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
        array_t array(make_anchor_handle(array_anchor), alloc);

        array.reserve(100000);
        for (int i = 0; i < 100000; ++i)
            array.push_back(i);
    }

    {
        default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
        array_t array(make_anchor_handle(array_anchor), alloc);

        REQUIRE(array.size() == 100000);
        for (int i = 0; i < 100000; ++i) {
            i32 v = array[u64(i)];
            if (v != i)
                FAIL("Unexpected value " << v << ", expected " << i);
        }
    }
}

TEST_CASE("customizable array growth", "[array]") {
    test_file file(block_size);
    {
        default_allocator::anchor alloc_anchor;
        default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
        array_t::anchor array_anchor;
        array_t array(make_anchor_handle(array_anchor), alloc);

        REQUIRE(std::holds_alternative<exponential_growth>(array.growth()));

        SECTION("exponential") {
            array.resize(1);
            REQUIRE(array.blocks() == 1);

            array.resize(array.block_capacity() * 10);
            REQUIRE(array.blocks() == 16);

            array.resize(array.block_capacity() * 127);
            REQUIRE(array.blocks() == 128);
            REQUIRE(array.capacity() == array.blocks() * array.block_capacity());
        }

        SECTION("linear") {
            array.growth(linear_growth(5));

            array.resize(0);
            REQUIRE(array.blocks() == 0);

            array.resize(1);
            REQUIRE(array.blocks() == 5);
            REQUIRE(array.capacity() == array.block_capacity() * 5);

            array.resize(24 * array.block_capacity());
            REQUIRE(array.blocks() == 25);

            array.growth(linear_growth(1));
            array.resize(101 * array.block_capacity());
            REQUIRE(array.blocks() == 101);

            array.growth(linear_growth(12345));
            array.resize(101 * array.block_capacity() + 1);
            REQUIRE(array.blocks() == 12345);
        }
    }
}
