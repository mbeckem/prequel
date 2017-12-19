#include <catch.hpp>

#include <extpp/btree.hpp>
#include <extpp/identity_key.hpp>
#include <extpp/detail/fix.hpp>

#include <algorithm>
#include <iostream>
#include <random>
#include <unordered_set>

#include "./test_file.hpp"

using namespace extpp;

using small_tree = btree<int, identity_key, std::less<>, 256>;

template<typename BTree>
void dump_tree(const BTree& tree, std::ostream& out = std::cout) {
    using visitor_t = typename BTree::visitor;

    int indent = 0;
    auto line = [&]() -> std::ostream& {
        for (int i = 0; i < indent; ++i)
            out << "  ";
        return out;
    };

    auto visit = detail::fix([&](auto& self, visitor_t& v) -> void {
        if (v.is_internal()) {
            line() << "Internal node @" << v.address() << "\n";
            line() << "Parent: @" << v.parent_address() << "\n";
            line() << "Children " << v.size() << ":" << "\n";

            ++indent;
            for (u32 i = 0; i < v.size() - 1; ++i) {
                line() << i << ": " << "(<= " << v.key(i) << ")" << "\n";
                ++indent;
                v.visit_child(i);
                self(v);
                v.visit_parent();
                --indent;
            }

            line() << (v.size() - 1) << ": " << "\n";
            ++indent;
            v.visit_child(v.size() - 1);
            self(v);
            v.visit_parent();
            --indent;

            --indent;
        } else {
            line() << "Leaf node @" << v.address() << "\n";
            line() << "Parent: @" << v.parent_address() << "\n";
            line() << "Predecessor: @" << v.predecessor_address() << "\n";
            line() << "Successor: @" << v.successor_address() << "\n";
            line() << "Values (" << v.size() << "):" << "\n";

            ++indent;
            for (u32 i = 0; i < v.size(); ++i) {
                line() << i << ": " << v.value(i) << "\n";
            }
            --indent;
        }
    });

    line() << "Internal fanout: " << tree.internal_fanout() << " (children)" << "\n";
    line() << "Leaf fanout: " << tree.leaf_fanout() << " (values)" << "\n";
    line() << "Height: " << tree.height() << "\n";
    line() << "Nodes: " << tree.nodes() << "\n";
    line() << "Size: " << tree.size() << "\n";
    line() << "\n";

    if (!tree.empty()) {
        visitor_t v = tree.visit();
        visit(v);
    }
}

template<typename TreeTest>
void simple_tree_test(TreeTest&& test) {
    test_file<small_tree::anchor, 256> file;
    file.open();
    {
        small_tree tree(file.anchor(), file.engine(), file.alloc());
        test(tree);
    }
    file.close();
}

static std::vector<int> generate_numbers(size_t count, int seed = 0) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> dist;
    std::unordered_set<int> seen;

    std::vector<int> result;
    result.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        int n = dist(rng);
        if (seen.find(n) == seen.end()) {
            seen.insert(n);
            result.push_back(n);
        }
    }
    return result;
}


TEST_CASE("btree basics", "[btree]") {
    simple_tree_test([](auto&& tree) {
        REQUIRE(tree.size() == 0);
        REQUIRE(tree.empty());
        REQUIRE(tree.begin() == tree.end());
        REQUIRE(tree.height() == 0);
        REQUIRE(tree.nodes() == 0);

        REQUIRE(tree.find(0) == tree.end());

        for (int i = 0; i <= 128; ++i) {
            int v = i * 2 + 1;
            INFO("Insertion of " << v);

            auto pair = tree.insert(v);

            REQUIRE(pair.second);
            REQUIRE(*pair.first == v);
        }

        REQUIRE(tree.find(54) == tree.end());

        auto pos = tree.find(55);
        REQUIRE(pos != tree.end());
        REQUIRE(*pos == 55);

        pos = tree.lower_bound(57);
        REQUIRE(pos != tree.end());
        REQUIRE(*pos == 57);

        pos = tree.lower_bound(60);
        REQUIRE(pos != tree.end());
        REQUIRE(*pos == 61);

        pos = tree.upper_bound(57);
        REQUIRE(pos != tree.end());
        REQUIRE(*pos == 59);

        pos = tree.upper_bound(257);
        REQUIRE(pos == tree.end());

        auto range = tree.equal_range(61);
        REQUIRE(range.first != range.second);
        REQUIRE(*range.first == 61);
        REQUIRE(*range.second == 63);

        range = tree.equal_range(64);
        REQUIRE(range.first == range.second);
        REQUIRE(*range.first == 65);

        range = tree.equal_range(257);
        REQUIRE(range.first != range.second);
        REQUIRE(range.second == tree.end());
        REQUIRE(*range.first == 257);

        REQUIRE(tree.find(127) != tree.end());
        REQUIRE(tree.size() == 129);
        REQUIRE(tree.height() > 0);
        REQUIRE(tree.nodes() > 0);

        tree.verify();
    });
}

TEST_CASE("duplicate keys are detected", "[btree]") {
    simple_tree_test([](auto&& tree) {
        std::vector<int> numbers = generate_numbers(10000);
        for (int n : numbers)
            tree.insert(n);

        REQUIRE(tree.size() == numbers.size());
        for (int n : numbers) {
            auto [pos, inserted] = tree.insert(n);
            if (*pos != n) {
                FAIL("Unexpected value " << *pos << ", expected " << n);
            }
            if (inserted) {
                FAIL("Value " << n << " should already be in the tree.");
            }
        }
    });
}

TEST_CASE("btrees are always sorted", "[btree]") {
    simple_tree_test([](auto&& tree) {
        std::vector<int> numbers = generate_numbers(4000);
        for (int n : numbers)
            tree.insert(n);
        tree.verify();

        REQUIRE(tree.size() == numbers.size());
        REQUIRE(std::is_sorted(tree.begin(), tree.end()));
        REQUIRE(u64(std::distance(tree.begin(), tree.end())) == tree.size());
    });
}

TEST_CASE("btree deletion", "[btree]") {
    simple_tree_test([](auto&& tree) {
        const int max = 100000;

        for (int i = max; i > 0; --i)
            tree.insert(i);

        SECTION("remove ascending") {
            auto pos = tree.begin();
            auto end = tree.end();
            int expected = 1;
            while (pos != end) {
                if (*pos != expected) {
                    FAIL("unexpected value at this position: " << *pos << ", expected " << expected);
                }

                if (tree.find(expected) != pos) {
                    FAIL("failed to find the value");
                }
                pos = tree.erase(pos);
                if (tree.find(expected) != end) {
                    FAIL("removed value still in tree");
                }

                ++expected;
            }
            REQUIRE(expected == max + 1);
        }

        SECTION("remove descending") {
            int expected = max;
            while (expected > 0) {
                auto pos = --tree.end();
                auto end = tree.end();
                if (*pos != expected) {
                    FAIL("unexpected value at this position: " << *pos << ", expected " << expected);
                }

                if (tree.find(expected) != pos) {
                    FAIL("failed to find the value");
                }
                pos = tree.erase(pos);
                if (tree.find(expected) != end) {
                    FAIL("removed value still in tree");
                }

                if (pos != end) {
                    FAIL("unexpected iterator, expected end");
                }
                --expected;
            }
        }

        SECTION("remove middle") {
            int mid = max / 2;
            auto pos = tree.find(mid);
            auto end = tree.end();
            while (pos != end)
                pos = tree.erase(pos);

            tree.verify();

            REQUIRE(tree.size() == u64(mid - 1));
            REQUIRE(*std::prev(tree.end()) == mid - 1);

            tree.clear();
        }

        SECTION("remove random") {
            std::mt19937_64 rng;

            std::vector<int> values;
            for (int i = max; i > 0; --i)
                values.push_back(i);
            std::shuffle(values.begin(), values.end(), rng);

            size_t border = (values.size() * 99) / 100;
            for (size_t i = 0; i < border; ++i) {
                int v = values[i];
                if (!tree.erase(v)) {
                    FAIL("failed to remove "  << v);
                }
            }

            tree.verify();

            for (size_t i = border; i < values.size(); ++i) {
                int v = values[i];
                auto pos = tree.find(v);
                if (pos == tree.end()) {
                    FAIL("failed to find " << v);
                }
            }

            tree.clear();
        }

        REQUIRE(tree.empty());
        REQUIRE(tree.height() == 0);
        REQUIRE(tree.nodes() == 0);
    });
}

// Generates a large number of (unique) random integers and inserts them into a btree in memory.
TEST_CASE("btree-fuzzy", "[btree][.slow]") {
    using tree_t = btree<u64, identity_key, std::less<>, 4096>;

    test_file<tree_t::anchor, 4096> file;
    file.open();
    {
        tree_t tree(file.anchor(), file.engine(), file.alloc());

        std::vector<u64> numbers;
        std::unordered_set<u64> seen;

        std::mt19937_64 rng(std::random_device{}());
        std::uniform_int_distribution<u64> dist(0, std::numeric_limits<u64>::max());
        for (int i = 0; i < 1000000; ++i) {
            while (1) {
                u64 num = dist(rng);
                if (seen.find(num) == seen.end()) {
                    seen.insert(num);
                    numbers.push_back(num);
                    break;
                }
            }
        }

        u64 count = 0;
        for (u64 n : numbers) {
            INFO("Inserting number " << n << " << at index " << count);

            tree_t::iterator pos;
            bool inserted;
            std::tie(pos, inserted) = tree.insert(n);
            if (!inserted) {
                FAIL("Failed to insert");
            }
            if (pos == tree.end()) {
                FAIL("Got the end iterator");
            }
            if (*pos != n) {
                FAIL("Iterator points to wrong value " << *pos);
            }

            ++count;
        }

        tree.verify();


        std::shuffle(numbers.begin(), numbers.end(), rng);
        for (u64 n : numbers) {
            INFO("Searching for number " << n);

            auto pos = tree.find(n);
            if (pos == tree.end()) {
                FAIL("Failed to find the number");
            }
            if (*pos != n) {
                FAIL("Iterator points to wrong value " << *pos);
            }
        }

        for (u64 n : numbers) {
            bool removed = tree.erase(n);
            if (!removed) {
                FAIL("Failed to remove " << n);
            }
        }
        REQUIRE(tree.size() == 0);
        REQUIRE(tree.height() == 0);
        REQUIRE(tree.nodes() == 0);
    }
}

TEST_CASE("btree cursor stability", "[btree]") {
    simple_tree_test([](auto&& tree) {
        using cursor_t = typename std::decay_t<decltype(tree)>::cursor;

        auto numbers = generate_numbers(10000);

        std::vector<cursor_t> cursors;
        for (size_t i = 0; i < 100; ++i) {
            cursors.emplace_back(tree.insert(numbers[i]).first);
            if (*cursors.back() != numbers[i])
                FAIL("Invalid cursor at index " << i << ", expected value " << numbers[i] << " but saw " << *cursors.back());
        }

        for (size_t i = 100; i < numbers.size(); ++i)
            tree.insert(numbers[i]);
        REQUIRE(tree.size() == numbers.size());

        for (size_t i = 0; i < 100; ++i) {
            auto& cursor = cursors[i];
            if (!cursor.valid())
                FAIL("Invalid cursor at index " << i);
            if (*cursor != numbers[i])
                FAIL("Invalid value at index " << i << ", expected " << numbers[i] << " but saw " << *cursor);
        }

        for (size_t i = 100; i < numbers.size(); ++i)
            tree.erase(numbers[i]);
        REQUIRE(tree.size() == 100);

        for (size_t i = 0; i < 100; ++i) {
            auto& cursor = cursors[i];
            if (!cursor.valid())
                FAIL("Invalid cursor at index " << i);
            if (*cursor != numbers[i])
                FAIL("Invalid value at index " << i << ", expected " << numbers[i] << " but saw " << *cursor);
        }

        for (size_t i = 0; i < 100; ++i) {
            auto& cursor = cursors[i];
            if (!cursor.valid())
                FAIL("The cursor at index " << i << " is already invalid.");
            tree.erase(cursor);
            if (cursor.valid())
                FAIL("The cursor at index " << i << " did not become invalid after its erasure");
        }

        REQUIRE(tree.size() == 0);
    });
}
