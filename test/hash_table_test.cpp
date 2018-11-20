#include <catch.hpp>

#include <prequel/default_allocator.hpp>
#include <prequel/formatting.hpp>
#include <prequel/hash.hpp>
#include <prequel/hash_table.hpp>
#include <prequel/raw_hash_table.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace prequel;

static constexpr u32 block_size = 512;

// TODO: Error messages, other operations.
TEST_CASE("hash table basic operations", "[hash-table]") {
    u32 block_sizes[] = { 128, 512, 4096 };

    static constexpr u32 key_size = 4;      // i32
    static constexpr u32 value_size = 8;    // i32 + i32

    raw_hash_table_options options;
    options.value_size = value_size;
    options.key_size = key_size;
    options.derive_key = [](const byte* value, byte* key, void*) {
        std::memmove(key, value, key_size);
    };
    options.key_equal = [](const byte* left, const byte* right, void*) {
        return std::memcmp(left, right, key_size) == 0;
    };
    options.key_hash = [](const byte* key, void*) -> u64 {
        u64 hash = deserialized_value<i32>(key);
        hash *= 64; // Simulate aligned storage
        return fnv_1a(hash);
    };

    const i32 count = 20000;

    for (u32 block_size : block_sizes) {
        test_file file(block_size);

        default_allocator::anchor alloc_anchor;
        default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

        raw_hash_table::anchor table_anchor;
        raw_hash_table table(make_anchor_handle(table_anchor), options, alloc);
        table.validate();

        for (i32 key = 1; key <= count; ++key) {
            CAPTURE(key);

            i32 value = key * 2 + 1;

            std::array<byte, value_size> buffer = serialized_value(std::tuple(key, value));
            bool inserted = table.insert(buffer.data());
            if (!inserted)
                FAIL();
        }
        table.validate();

        for (i32 key = 1; key <= count; ++key) {
            CAPTURE(key);

            i32 expected_value = key * 2 + 1;

            std::array<byte, key_size> buffer = serialized_value(key);
            std::array<byte, value_size> result_buffer;

            bool found = table.find(buffer.data(), result_buffer.data());
            if (!found)
                FAIL();

            std::tuple<i32, i32> value;
            deserialize(value, result_buffer.data(), result_buffer.size());

            if (value != std::tuple(key, expected_value))
                FAIL();
        }
        table.validate();

        // Erase most elements.
        for (i32 key = 1; key <= count; ++key) {
            CAPTURE(key);

            if (key % 500 == 0)
                continue;

            std::array<byte, key_size> key_buffer = serialized_value(key);

            bool erased = table.erase(key_buffer.data());
            if (!erased)
                FAIL("Failed to erase");
        }
        table.validate();

        for (i32 key = 1; key <= count; ++key) {
            CAPTURE(key);

            if (key % 500 != 0)
                continue;

            i32 expected_value = key * 2 + 1;

            std::array<byte, key_size> buffer = serialized_value(key);
            std::array<byte, value_size> result_buffer;

            bool found = table.find(buffer.data(), result_buffer.data());
            if (!found)
                FAIL();

            std::tuple<i32, i32> value;
            deserialize(value, result_buffer.data(), result_buffer.size());

            if (value != std::tuple(key, expected_value))
                FAIL();
        }
        table.validate();

        table.reset();
        table.validate();
        REQUIRE(table.size() == 0);
        REQUIRE(table.byte_size() == 0);
    }
}

TEST_CASE("hash table works well for integer keys", "[hash-table]") {
    test_file file(block_size);

    default_allocator::anchor alloc_anchor;
    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    struct entry {
        i64 key = 0;
        i64 value = 0;

        entry() = default;
        entry(i64 key, i64 value): key(key), value(value) {}

        struct derive_key {
            i64 operator()(const entry& e) const {
                return e.key;
            }
        };

        static constexpr auto get_binary_format() {
            return make_binary_format(&entry::key, &entry::value);
        }
    };

    using hash_table_t = hash_table<entry, entry::derive_key>;

    hash_table_t::anchor anchor;
    hash_table_t table(make_anchor_handle(anchor), alloc);

    REQUIRE(table.size() == 0);
    REQUIRE(table.empty());
    REQUIRE(table.byte_size() == 0);
    REQUIRE(table.allocated_buckets() == 0);
    REQUIRE(table.fill_factor() == 0);
    REQUIRE(table.overhead() == 1);

    static constexpr i64 count = 10000;

    for (i64 i = 0; i < count; ++i) {
        CAPTURE(i);

        entry e(i * 64, (i * i) / 2);
        bool inserted = table.insert(e);
        if (!inserted)
            FAIL("insertion failed: key not unique?");
    }

    for (i64 i = count - 1; i >= 0; --i) {
        CAPTURE(i);

        i64 key = i * 64;
        entry e;
        if (!table.find(key, e)) {
            FAIL("find failed: key does not exist.");
        }

        if (e.key != key) {
            FAIL("found the wrong value.");
        }
    }

    table.visit([&](const hash_table_t::node_view& node) {
        // This works with the fnv-1a hash and the given number of values.
        // Should be like that on every platform.
        if (node.size() == 0) {
            FAIL("Zero sized node at " << node.bucket_index());
        }
        return iteration_control::next;
    });
}

TEST_CASE("compatible hash functions", "[hash-table]") {
    test_file file(256);

    default_allocator::anchor alloc_anchor;
    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    /*
     * Create a hash table of u64 and then querying using a byte array of the same size,
     * using the same hash function.
     */

    hash_table<u64>::anchor anchor;
    hash_table<u64> table(make_anchor_handle(anchor), alloc);

    for (u64 i = 0; i < 1000; ++i) {
        CAPTURE(i);

        u64 value = i * 777;
        bool inserted = table.insert(value);
        if (!inserted)
            FAIL();
    }

    const u64 search = 777 * 888;
    REQUIRE(table.contains(search));

    std::array<byte, 8> compatible = serialized_value(search);

    u64 found_value = 0;
    bool found = table.find_compatible(
        compatible,
        [&](const auto& key_array) {
            return fnv_1a(key_array.data(), key_array.size());
        },
        [&](const auto& key_array, u64 rhs) {
            u64 lhs = deserialized_value<u64>(key_array.data());
            return lhs == rhs;
        },
        found_value
    );

    REQUIRE(found);
    REQUIRE(found_value == search);
}

// TODO Test node visitation.
