#include <catch.hpp>

#include <prequel/node_allocator.hpp>
#include <prequel/raw_btree.hpp>
#include <prequel/btree.hpp>
#include <prequel/formatting.hpp>

#include "./test_file.hpp"

#include <iostream>
#include <random>
#include <unordered_set>
#include <vector>

using namespace prequel;

namespace {

struct raw_anchor {
    node_allocator::anchor alloc;
    raw_btree::anchor tree;

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_anchor::alloc, &raw_anchor::tree);
    }
};

struct raw_value {
    u32 key = 0;
    u32 count = 0;

    raw_value() = default;
    explicit raw_value(u32 key): key(key) {}
    explicit raw_value(u32 key, u32 count): key(key), count(count) {}

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_value::key, &raw_value::count);
    }

    bool operator==(const raw_value& other) const {
        return key == other.key && count == other.count;
    }

    bool operator!=(const raw_value& other) const {
        return !(*this == other);
    }

    friend std::ostream& operator<<(std::ostream& os, const raw_value& v) {
        return os << "(" << v.key << ", " << v.count << ")";
    }
};

struct derive_key {
    u32 operator()(const raw_value& v) const {
        return v.key;
    }
};

template<typename Value, typename Container>
void check_tree_equals_container(const raw_btree& tree, Container&& c) {
    auto ci = c.begin();
    auto ce = c.end();

    u64 index = 0;
    for (auto c = tree.create_cursor(tree.seek_min); c; c.move_next(), ++index, ++ci) {
        if (ci == ce)
            FAIL("Too many values in tree (element " << index << ")");

        auto v = deserialized_value<Value>(c.get());
        if (v != *ci)
            FAIL("Wrong value, expected " << *ci << " but saw " << v);
    }
    if (ci != ce)
        FAIL("Not enough values in tree (saw " << index << " values)");
}

template<typename Value, typename Container>
void check_tree_equals_container_reverse(const raw_btree& tree, Container&& c) {
    auto ci = c.rbegin();
    auto ce = c.rend();

    u64 index = c.size() - 1;
    for (auto c = tree.create_cursor(tree.seek_max); c; c.move_prev(), --index, ++ci) {
        if (ci == ce)
            FAIL("Too many values in tree (element " << index << ")");

        auto v = deserialized_value<Value>(c.get());
        if (v != *ci)
            FAIL("Wrong value, expected " << *ci << " but saw " << v);
    }
    if (ci != ce)
        FAIL("Not enough values in tree (saw " << index << " values)");
}

template<typename T, typename DeriveKey, typename KeyLess, typename Container>
void check_tree_equals_container(const btree<T, DeriveKey, KeyLess>& tree, Container&& c) {
    return check_tree_equals_container<T>(tree.raw(), c);
}

template<typename T, typename DeriveKey, typename KeyLess, typename Container>
void check_tree_equals_container_reverse(const btree<T, DeriveKey, KeyLess>& tree, Container&& c) {
    return check_tree_equals_container_reverse<T>(tree.raw(), c);
}

template<typename Value, typename KeyDerive = identity_t, typename TestFunction>
void simple_tree_test(TestFunction&& test) {
    using tree_type = btree<Value, KeyDerive>;

    u32 block_sizes[] = { 128, 512, 4096 };
    for (u32 block_size : block_sizes) {
        CAPTURE(block_size);

        test_file file(block_size);

        node_allocator::anchor alloc_anchor;
        node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

        typename tree_type::anchor tree_anchor;
        tree_type tree(make_anchor_handle(tree_anchor), alloc);
        test(tree, block_size);
    }
}

template<typename T = i32>
static std::vector<T> generate_numbers(size_t count, i32 seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<T> dist;
    std::unordered_set<T> seen;

    std::vector<T> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        i32 n = dist(rng);
        if (seen.find(n) == seen.end()) {
            seen.insert(n);
            result.push_back(n);
        }
    }
    return result;
}

} // namespace

TEST_CASE("raw btree", "[btree]") {
    static const u32 value_size = serialized_size<raw_value>();
    static const u32 key_size = serialized_size<u32>();
    static const u32 block_size = 128;

    // A tree of raw_value instances index by raw_value::key
    raw_btree_options options;
    options.key_size = key_size;
    options.value_size = value_size;
    options.derive_key = [](const byte* value, byte* key, void* user_data) -> void {
        (void) user_data;
        raw_value v = deserialized_value<raw_value>(value);
        serialize(v.key, key);
    };
    options.key_less = [](const byte* left_key, const byte* right_key, void* user_data) -> bool {
        (void) user_data;
        u32 lhs = deserialized_value<u32>(left_key);
        u32 rhs = deserialized_value<u32>(right_key);
        return lhs < rhs;
    };

    test_file file(block_size);

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    {
        raw_btree::anchor tree_anchor;
        raw_btree tree(make_anchor_handle(tree_anchor), options, alloc);

        // TODO: test bad cursor behaviour

        SECTION("empty tree invariants") {
            REQUIRE(tree.value_size() == serialized_size<raw_value>());
            REQUIRE(tree.key_size() == serialized_size<u32>());
            REQUIRE(tree.leaf_node_capacity() == 15); // (128 - 4) / 8; header is 4 bytes.
            REQUIRE(tree.internal_node_capacity() == 10); // (128 - 4) / (4 + 8); header is 4 bytes.
            REQUIRE(tree.internal_nodes() == 0);
            REQUIRE(tree.leaf_nodes() == 0);
            REQUIRE(tree.nodes() == 0);
            REQUIRE(tree.byte_size() == 0);
            REQUIRE(tree.overhead() == 1.0);

            auto c1 = tree.create_cursor();
            REQUIRE(c1.at_end());

            c1.move_min();
            REQUIRE(c1.at_end());

            c1.move_max();
            REQUIRE(c1.at_end());

            auto key = serialized_value(u32(1234));
            REQUIRE(!c1.lower_bound(key.data()));
            REQUIRE(c1.at_end());

            REQUIRE(!c1.upper_bound(key.data()));
            REQUIRE(c1.at_end());

            tree.validate();
        }

        SECTION("default cursor is invalid") {
            auto c = tree.create_cursor();
            REQUIRE(c.at_end());
            REQUIRE(!c);
            REQUIRE(c.value_size() == value_size);
            REQUIRE(c.key_size() == key_size);

            tree.validate();
        }

        SECTION("inserts forward") {
            auto cursor = tree.create_cursor();

            std::vector<raw_value> expected;
            for (u32 i = 0; i < 73; ++i) {
                CAPTURE(i);
                raw_value v{i, i * 2};
                expected.push_back(v);
                auto buffer = serialized_value(v);
                if (!cursor.insert(buffer.data()))
                    FAIL("Failed to insert value with unique key");
            }
            check_tree_equals_container<raw_value>(tree, expected);
            check_tree_equals_container_reverse<raw_value>(tree, expected);

            tree.validate();
        }

        SECTION("insertion of duplicate") {
            auto cursor = tree.create_cursor();
            for (u32 i = 0; i < 150; ++i) {
                INFO("Insert: " << i);

                raw_value v{i, i};
                auto buffer = serialized_value(v);
                if (!cursor.insert(buffer.data()))
                    FAIL("Failed to insert value");
            }

            for (u32 i = 0; i < 150; ++i) {
                INFO("Reinsert: " << i);

                raw_value v{i, i * 2};
                auto buffer = serialized_value(v);
                if (cursor.insert(buffer.data()))
                    FAIL("Duplicate value was inserted");

                raw_value w = deserialized_value<raw_value>(cursor.get());
                REQUIRE(v.key == w.key);
                REQUIRE(w.key == w.count); // Old value.
            }

            tree.validate();
        }

        SECTION("stable cursors when inserting") {
            struct stable_element_t {
                raw_btree::cursor cursor;
                raw_value expected;
            };

            std::vector<stable_element_t> cursors;

            auto cursor = tree.create_cursor();
            for (u32 i = 1000; i < 1500; i += 5) {
                raw_value v{i, i +1};
                auto buffer = serialized_value(v);
                cursor.insert(buffer.data());
                cursors.push_back({cursor, v});
            }

            auto keep_elem = tree.create_cursor(tree.seek_none);
            {
                auto key_buffer = serialized_value(u32(1255));
                keep_elem.lower_bound(key_buffer.data());
                raw_value v = deserialized_value<raw_value>(keep_elem.get());
                REQUIRE(v == raw_value(1255, 1256));
            }

            auto keep_min = tree.create_cursor(tree.seek_min);
            {
                raw_value v = deserialized_value<raw_value>(keep_min.get());
                REQUIRE(v == raw_value(1000, 1001));
            }

            auto keep_max = tree.create_cursor(tree.seek_max);
            {
                raw_value v = deserialized_value<raw_value>(keep_max.get());
                REQUIRE(v == raw_value(1495, 1496));
            }

            for (u32 i = 900; i < 1600; ++i) {
                auto buffer = serialized_value(raw_value(i, i * 2));
                cursor.insert(buffer.data());
            }

            raw_value old_elem = deserialized_value<raw_value>(keep_elem.get());
            CHECK(old_elem == raw_value(1255, 1256));

            raw_value old_min = deserialized_value<raw_value>(keep_min.get());
            CHECK(old_min == raw_value(1000, 1001));

            raw_value old_max = deserialized_value<raw_value>(keep_max.get());
            CHECK(old_max == raw_value(1495, 1496));

            for (auto& stable : cursors) {
                raw_value found = deserialized_value<raw_value>(stable.cursor.get());
                CHECK(found == stable.expected);
            }

            tree.validate();
        }

        SECTION("forward iteration") {
            std::vector<raw_value> values;
            for (i32 i = 5000; i < 6000; ++i)
                values.push_back(raw_value(i, 10000 + i));

            auto cursor = tree.create_cursor(tree.seek_none);
            for (auto v : values) {
                auto buffer = serialized_value(v);
                if (!cursor.insert(buffer.data()))
                    FAIL("Failed to insert");
            }

            check_tree_equals_container<raw_value>(tree, values);
            check_tree_equals_container_reverse<raw_value>(tree, values);

            tree.validate();
        }
    }
}

TEST_CASE("btree insertion and querying", "[btree]") {
    simple_tree_test<i32>([](auto&& tree, u32 block_size) {
        unused(block_size);

        REQUIRE(tree.size() == 0);
        REQUIRE(tree.empty());
        REQUIRE(tree.height() == 0);
        REQUIRE(tree.nodes() == 0);
        REQUIRE(!tree.find(0));

        for (i32 i = 0; i <= 128; ++i) {
            i32 v = i * 2 + 1;
            INFO("Insertion of " << v);

            auto result = tree.insert(v);

            REQUIRE(result.inserted);
            REQUIRE(result.position.get() == v);
        }

        tree.validate();

        auto b = tree.find(54);
        CHECK(!b);

        auto c = tree.create_cursor();

        c.find(55);
        REQUIRE(c.get() == 55);

        c.lower_bound(57);
        REQUIRE(c.get() == 57);

        c.lower_bound(60);
        REQUIRE(c.get() == 61);

        c.upper_bound(57);
        REQUIRE(c.get() == 59);

        c.upper_bound(257);
        REQUIRE(!c);

        REQUIRE(tree.find(127));
        REQUIRE(tree.size() == 129);
        REQUIRE(tree.height() > 0);
        REQUIRE(tree.nodes() > 0);
    });
}

TEST_CASE("btree detects duplicate keys", "[btree]") {
    simple_tree_test<raw_value, derive_key>([](auto&& tree, u32 block_size) {
        unused(block_size);

        std::vector<i32> numbers = generate_numbers(10000, 12345);
        for (i32 n : numbers)
            tree.insert(raw_value(n, 1));
        tree.validate();

        REQUIRE(tree.size() == numbers.size());
        for (i32 n : numbers) {
            auto [cursor, inserted] = tree.insert(raw_value(n, 2));
            if (cursor.get() != raw_value(n, 1)) {
                FAIL("Unexpected value " << cursor.get() << ", expected " << raw_value(n, 2));
            }
            if (inserted) {
                FAIL("Value " << n << " should already be in the tree.");
            }
        }

        for (i32 n : numbers) {
            auto result = tree.insert_or_update(raw_value(n, 3));
            if (result.position.get() != raw_value(n, 3)) {
                FAIL("Unexpected value " << result.position.get() << ", expected " << raw_value(n, 3));
            }
            if (result.inserted) {
                FAIL("Value " << n << " should have been overwritten.");
            }
        }
        tree.validate();
    });
}

TEST_CASE("btrees are always sorted", "[btree]") {
    simple_tree_test<i64>([](auto&& tree, u32 block_size) {
        unused(block_size);

        std::vector<i64> numbers = generate_numbers<i64>(8000, 0);

        auto cursor = tree.create_cursor();
        for (i64 n : numbers)
            cursor.insert(n);


        std::sort(numbers.begin(), numbers.end());

        for (i64 n : numbers) {
            cursor.find(n);
            if (!cursor || cursor.get() != n)
                FAIL("Find failed");

            cursor.lower_bound(n);
            if (!cursor || cursor.get() != n)
                FAIL("Lower bound failed");
        }


        REQUIRE(tree.size() == numbers.size());
        check_tree_equals_container(tree, numbers);
        check_tree_equals_container_reverse(tree, numbers);
        tree.validate();
    });
}

TEST_CASE("btree deletion", "[btree]") {
    simple_tree_test<i32>([](auto&& tree, u32 block_size) {
        const i32 max = 100000;

        {
            auto cursor = tree.create_cursor();
            for (i32 i = max; i > 0; --i)
                cursor.insert(i);
        }
        tree.validate();

        SECTION("remove ascending/" + std::to_string(block_size)) {
            auto cursor = tree.create_cursor(tree.seek_min);

            i32 expected = 1;
            while (cursor) {
                if (cursor.get() != expected) {
                    FAIL("unexpected value at this position: " << cursor.get() << ", expected " << expected);
                }

                if (auto c = tree.find(expected); !c || c != cursor) {
                    FAIL("failed to find the value");
                }

                cursor.erase();
                if (!cursor.erased()) {
                    FAIL("Cursor not marked as erased.");
                }
                if (!cursor) {
                    FAIL("Cursor at the end.");
                }

                if (auto c = tree.find(expected); c) {
                    FAIL("removed value still in tree");
                }

                cursor.move_next();
                ++expected;
            }

            tree.validate();
            REQUIRE(expected == max + 1);

            REQUIRE(tree.empty());
            REQUIRE(tree.height() == 0);
            REQUIRE(tree.nodes() == 0);
        }

        SECTION("remove descending/" + std::to_string(block_size)) {
            i32 expected = max;
            auto cursor = tree.create_cursor(tree.seek_max);
            while (expected > 0) {
                if (!cursor) {
                    FAIL("Invalid cursor");
                }

                if (cursor.get() != expected) {
                    FAIL("unexpected value at this position: " << cursor.get() << ", expected " << expected);
                }

                if (auto c = tree.find(expected); c != cursor) {
                    FAIL("failed to find the value");
                }

                cursor.erase();
                if (auto c = tree.find(expected); c) {
                    FAIL("removed value still in tree");
                }

                cursor.move_prev();
                --expected;
            }
            tree.validate();

            REQUIRE(tree.empty());
            REQUIRE(tree.height() == 0);
            REQUIRE(tree.nodes() == 0);
        }

        SECTION("remove middle/" + std::to_string(block_size)) {
            i32 mid = max / 2;
            for (auto pos = tree.find(mid); pos; pos.move_next()) {
                pos.erase();
            }

            REQUIRE(tree.size() == u64(mid - 1));
            REQUIRE(tree.create_cursor(tree.seek_max).get() == mid - 1);
            tree.validate();
        }

        SECTION("remove random/" + std::to_string(block_size)) {
            std::mt19937_64 rng;

            std::vector<i32> values;
            for (i32 i = max; i > 0; --i)
                values.push_back(i);
            std::shuffle(values.begin(), values.end(), rng);

            size_t border = (values.size() * 99) / 100;
            for (size_t i = 0; i < border; ++i) {
                i32 v = values[i];
                auto cursor = tree.find(v);
                cursor.erase();
            }

            for (size_t i = border; i < values.size(); ++i) {
                i32 v = values[i];
                auto cursor = tree.find(v);
                if (!cursor) {
                    FAIL("Failed to find " << v);
                }
                if (cursor.get() != v) {
                    FAIL("Unexpected value " << cursor.get() << ", expected " << v);
                }
            }

            tree.validate();
            tree.clear();
        }
    });
}

TEST_CASE("btree cursor stability", "[btree]") {
    simple_tree_test<i32>([](auto&& tree, u32 block_size) {
        unused(block_size);

        using cursor_t = typename std::decay_t<decltype(tree)>::cursor;

        struct stable_cursor {
            cursor_t cursor;
            i32 value = 0;
        };

        auto numbers = generate_numbers(10000, 444666);

        std::vector<stable_cursor> cursors;
        for (i32 value : numbers) {
            stable_cursor c;
            c.cursor = tree.insert(value).position;
            c.value = value;

            cursors.push_back(std::move(c));
        }
        REQUIRE(tree.size() == numbers.size());

        for (auto& c : cursors) {
            if (!c.cursor)
                FAIL("Invalid cursor for value" << c.value);
            if (c.cursor.get() != c.value)
                FAIL("Invalid value for value " << c.value << ": " << c.cursor.get());
        }

        std::mt19937_64 rng(123123);
        std::shuffle(numbers.begin(), numbers.end(), rng);

        for (size_t i = 100; i < numbers.size(); ++i)  {
            tree.find(numbers[i]).erase();
        }
        numbers.resize(100);
        REQUIRE(tree.size() == 100);

        auto has_number = [&](i32 n) {
            return std::find(numbers.begin(), numbers.end(), n) != numbers.end();
        };

        for (auto& c : cursors) {
            CAPTURE(c.value);
            if (c.cursor.erased()) {
                if (has_number(c.value)) {
                    FAIL("Should not have been erased.");
                }
            } else {
                if (!has_number(c.value)) {
                    FAIL("Should have been erased.");
                }
                if (c.cursor.get() != c.value) {
                    FAIL("Invalid value: " << c.cursor.get());
                }
            }
        }

        for (i32 n : numbers) {
            tree.find(n).erase();
        }

        for (auto& c : cursors) {
            CAPTURE(c.value);

            if (!c.cursor.erased())
                FAIL("Should have been erased.");
        }

        REQUIRE(tree.size() == 0);
    });
}

TEST_CASE("btree fuzzy tests", "[btree][.slow]") {
    using tree_t = btree<u64>;

    test_file file(4096);

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    {
        tree_t::anchor tree_anchor;
        tree_t tree(make_anchor_handle(tree_anchor), alloc);

        std::vector<u64> numbers =  generate_numbers<u64>(1000000, 23546);

        u64 count = 0;
        for (u64 n : numbers) {
            INFO("Inserting number " << n << " << at index " << count);

            auto result = tree.insert(n);
            if (!result.inserted) {
                FAIL("Failed to insert");
            }
            if (!result.position) {
                FAIL("Got the invalid cursor");
            }
            if (result.position.get() != n) {
                FAIL("Cursor points to wrong value " << result.position.get());
            }

            ++count;
        }

        tree.validate();

        std::mt19937_64 rng(12345);
        std::shuffle(numbers.begin(), numbers.end(), rng);
        for (u64 n : numbers) {
            INFO("Searching for number " << n);

            auto pos = tree.find(n);
            if (!pos) {
                FAIL("Failed to find the number");
            }
            if (pos.get() != n) {
                FAIL("Cursor points to wrong value " << pos.get());
            }
        }

        for (u64 n : numbers) {
            tree.find(n).erase();
        }

        REQUIRE(tree.size() == 0);
        REQUIRE(tree.height() == 0);
        REQUIRE(tree.nodes() == 0);
    }
}

TEST_CASE("btree bulk loading", "[btree][bulk-loading]") {
    simple_tree_test<i64>([](auto&& tree, u32 block_size) {
        SECTION("fails for non-empty trees/" + std::to_string(block_size)) {
            tree.insert(12345);

            REQUIRE_THROWS_AS(tree.bulk_load(), bad_operation);

            tree.clear();

            auto loader = tree.bulk_load();

            tree.insert(12345);
            REQUIRE_THROWS_AS(loader.finish(), bad_operation);
        }

        SECTION("tree construction/" + std::to_string(block_size)) {
            const i64 max = 25000;

            auto loader = tree.bulk_load();

            for (i64 i = 0; i < max; ++i)
                loader.insert(i);

            loader.finish();

            REQUIRE(tree.size() == max);

            i64 expected = 0;
            for (auto c = tree.create_cursor(tree.seek_min); c; c.move_next()) {
                if (c.get() != expected)
                    FAIL("Unexpected value " << c.get() << " expected " << expected);
                ++expected;
            }
            if (expected != max)
                FAIL("Did not see all values.");

            REQUIRE_NOTHROW(tree.validate());
        }

        SECTION("discard partial load/" + std::to_string(block_size)) {
            node_allocator& alloc = dynamic_cast<node_allocator&>(tree.get_allocator());

            const i64 max = 25000;
            auto loader = tree.bulk_load();
            for (i64 i = 0; i < max; ++i)
                loader.insert(i);

            loader.discard();

            REQUIRE(alloc.data_used() == 0);
            REQUIRE(tree.empty());
            REQUIRE_NOTHROW(tree.validate());
        }
    });
}
