#include <catch.hpp>

#include <extpp/exception.hpp>
#include <extpp/formatting.hpp>
#include <extpp/list.hpp>
#include <extpp/node_allocator.hpp>
#include <extpp/raw_list.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

namespace {

template<typename T>
auto serialized(T val) {
    std::array<byte, serialized_size<T>()> result;
    serialize(val, result.data(), result.size());
    return result;
}

template<typename T>
bool empty_cursor(T&& list) {
    auto cursor = list.create_cursor();
    if (cursor)
        return false;
    cursor.move_first();
    return !cursor;
}

template<typename List, typename Container>
void check_list_equals_container(List&& ls, Container&& c) {
    auto ci = c.begin();
    auto ce = c.end();

    u64 index = 0;
    for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next(), index++, ci++) {
        if (ci == ce)
            FAIL("Too many values in list (index " << index << ")");

        auto v = c.get();
        if (v != *ci)
            FAIL("Wrong value, expected " << *ci << " but saw " << v);
    }
    if (ci != ce)
        FAIL("Not enough values in list (saw " << index << " values)");
}

}

TEST_CASE("raw list", "[list]") {
    static const u32 value_size = 4;
    static const u32 block_size = 64;

    test_file file(block_size);
    file.open();

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    {
        raw_list::anchor list_anchor;
        raw_list list(make_anchor_handle(list_anchor), value_size, alloc);

        SECTION("Empty list") {
            REQUIRE(list.value_size() == 4);
            REQUIRE(list.node_capacity() == 11); // (64 - 20) / 4, header is 20 bytes.
            REQUIRE(list.empty());
            REQUIRE(list.size() == 0);
            REQUIRE(empty_cursor(list));
        }

        SECTION("Insert and remove") {
            std::vector<std::array<byte, 4>> values;
            for (u32 i = 14; i-- > 0;)
                values.push_back(serialized(-i));

            for (u32 i = 0; i < 32; ++i)
                values.push_back(serialized(i));

            for (u32 i = 14; i < values.size(); ++i)
                list.push_back(values[i].data());

            for (u32 i = 14; i-- > 0;)
                list.push_front(values[i].data());

            auto compare_content = [&]() {
                u32 index = 0;
                auto cursor = list.create_cursor();
                cursor.move_first();
                for (; cursor; cursor.move_next(), ++index) {
                    const byte* data_list = static_cast<const byte*>(cursor.get());
                    const byte* data_expected = values[index].data();
                    if (std::memcmp(data_list, data_expected, value_size) != 0) {
                        FAIL("Unexpected data at index " << index
                             << ", expected " << format_hex(data_expected, value_size)
                             << ", but saw " << format_hex(data_list, value_size));
                    }
                }
                REQUIRE(index == values.size());
            };


            {
                INFO("Step 1");
                REQUIRE(!list.empty());
                REQUIRE(list.size() == 46);
                compare_content();
            }

            for (int i = 0; i < 20; ++i)
                list.pop_front();
            values.erase(values.begin(), values.begin() + 20);

            {
                INFO("Step 2");
                REQUIRE(list.size() == values.size());
                compare_content();
            }

            for (int i = 0; i < 18; ++i)
                list.pop_back();
            values.erase(values.end() - 18, values.end());

            {
                INFO("Step 3");
                REQUIRE(list.size() == values.size());
                compare_content();
            }

            for (int i = 0; i < 8; ++i)
                list.pop_back();

            REQUIRE(list.empty());
            REQUIRE(list.size() == 0);
            REQUIRE(empty_cursor(list));
        }
    }
}

TEST_CASE("front and back insertion produce dense nodes", "[list]") {
    using value_t = i32;

    test_file file(64);
    file.open();

    node_allocator::anchor node_anchor;
    node_allocator alloc(make_anchor_handle(node_anchor), file.get_engine());

    list<value_t>::anchor list_anchor;
    list<value_t> ls(make_anchor_handle(list_anchor), alloc);

    const i32 COUNT = 1024;
    std::vector<i32> comp;
    for (i32 i = 0; i < COUNT; ++i) {
        if (i % 2 == 0 || i % 5 == 0) {
            ls.push_back(i);
            comp.push_back(i);
        } else {
            ls.push_front(i);
            comp.insert(comp.begin(), i);
        }
    }

    // Check content.
    {
        REQUIRE(ls.size() == COUNT);

        auto compi = comp.begin();
        auto compe = comp.end();

        u32 index = 0;
        for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next(), index++, compi++) {
            if (compi == compe)
                FAIL("Too many values in cursor (index " << index << ")");

            i32 v = c.get();
            if (v != *compi)
                FAIL("Wrong value, expected " << *compi << " but saw " << v);
        }
        if (compi != compe)
            FAIL("Not enough values in cursor (saw " << index << " values)");
    }

    // All nodes (except for the last and first one) must be full.
    {
        u32 nodes = 0;
        ls.visit([&](auto& view) {
            bool first = !view.prev_address().valid();
            bool last = !view.next_address().valid();

            if (!first && !last) {
                if (view.value_count() < ls.node_capacity()) {
                    FAIL("Node at " << view.address() << " is not full");
                }
            }
            ++nodes;
            return true;
        });
        REQUIRE(nodes == ls.nodes());
    }
}

TEST_CASE("basic cursor usage iteration", "[list]") {
    using value_t = i32;

    test_file file(64);
    file.open();

    node_allocator::anchor node_anchor;
    node_allocator alloc(make_anchor_handle(node_anchor), file.get_engine());

    list<value_t>::anchor list_anchor;
    list<value_t> ls(make_anchor_handle(list_anchor), alloc);

    SECTION("empty list") {
        auto c1 = ls.create_cursor(ls.seek_first);
        REQUIRE(!c1);

        auto c2 = ls.create_cursor(ls.seek_last);
        REQUIRE(!c2);

        auto c3 = ls.create_cursor(ls.seek_none);
        REQUIRE(!c3);
    }

    const i32 COUNT = 1024;
    std::vector<i32> comp;
    for (i32 i = 0; i < COUNT; ++i) {
        if (i % 2 == 0 || i % 5 == 0) {
            ls.push_back(i);
            comp.push_back(i);
        } else {
            ls.push_front(i);
            comp.insert(comp.begin(), i);
        }
    }

    SECTION("forward iteration") {
        auto i = comp.begin();
        auto e = comp.end();
        for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next()) {
            if (i == e)
                FAIL("Too many values in cursor");

            if (*i != c.get())
                FAIL("Unexpected value: expected " << *i << " but saw " << c.get());

            ++i;
        }
        REQUIRE(i == e);
    }

    SECTION("reverse iteration") {
        auto i = comp.rbegin();
        auto b = comp.rend();
        for (auto c = ls.create_cursor(ls.seek_last); c; c.move_prev()) {
            if (i == b)
                FAIL("Too many values in cursor");

            if (*i != c.get())
                FAIL("Unexpected value: expected " << *i << " but saw " << c.get());

            ++i;
        }
        REQUIRE(i == b);
    }
}

TEST_CASE("invalid cursor behaviour", "[list]") {
    // Almost all operations fail on invalid cursors.
    // Default constructed cursors behave (nearly) the same way
    // then normal invalidated iterators.

    using value_t = i32;

    test_file file(64);
    file.open();

    node_allocator::anchor node_anchor;
    node_allocator alloc(make_anchor_handle(node_anchor), file.get_engine());

    list<value_t>::anchor list_anchor;
    list<value_t> ls(make_anchor_handle(list_anchor), alloc);

    auto checks = [&](auto&& c) {
        REQUIRE(!c);
        REQUIRE(c.invalid());
        REQUIRE(!c.erased());

        REQUIRE_THROWS_AS(c.get(), bad_cursor);
        REQUIRE_THROWS_AS(c.set(0), bad_cursor);
        REQUIRE_THROWS_AS(c.erase(), bad_cursor);
        REQUIRE_THROWS_AS(c.insert_after(0), bad_cursor);
        REQUIRE_THROWS_AS(c.insert_before(0), bad_cursor);
        REQUIRE_THROWS_AS(c.move_next(), bad_cursor);
        REQUIRE_THROWS_AS(c.move_prev(), bad_cursor);
    };

    {
        INFO("Created invalid");
        auto c = ls.create_cursor(ls.seek_none);
        REQUIRE(c.raw().value_size() == serialized_size<value_t>());
        checks(c);
    }

    {
        INFO("Default constructed");
        list<value_t>::cursor c;
        REQUIRE_THROWS_AS(c.raw().value_size(), bad_cursor); // No impl
        checks(c);
    }
}

struct point_t {
    i32 x = 0;
    i32 y = 0;

    point_t() = default;
    point_t(i32 x, i32 y): x(x), y(y) {}

    static constexpr auto get_binary_format() {
        return make_binary_format(&point_t::x, &point_t::y);
    }

    bool operator==(const point_t& v) const { return x == v.x && y == v.y; }
    bool operator!=(const point_t& v) const { return !(*this == v); }

    friend std::ostream& operator<<(std::ostream& o, const point_t& v) {
        return o << "(" << v.x << ", " << v.y << ")";
    }
};


TEST_CASE("Iterating and deleting using list cursors", "[list]") {
    test_file file(64);
    file.open();

    node_allocator::anchor node_anchor;
    node_allocator alloc(make_anchor_handle(node_anchor), file.get_engine());

    list<point_t>::anchor list_anchor;
    list<point_t> ls(make_anchor_handle(list_anchor), alloc);

    const i32 COUNT = 1024;
    std::vector<point_t> comp;
    for (i32 i = 1; i <= COUNT; ++i) {
        point_t v;
        if (i % 3 == 0) {
            v.x = -1;
            v.y = i * 3;
        } else {
            v.x = i;
            v.y = i + 1;
        }

        ls.push_back(v);
        comp.push_back(v);
    }

    SECTION("deleted cursor properties") {
        auto c = ls.create_cursor(ls.seek_first);
        c.erase();

        REQUIRE(c);
        REQUIRE(c.erased());
        REQUIRE_THROWS_AS(c.erase(), bad_cursor);
        REQUIRE_THROWS_AS(c.get(), bad_cursor);
        REQUIRE_THROWS_AS(c.set({1, 2}), bad_cursor);
        REQUIRE_THROWS_AS(c.insert_after({1, 2}), bad_cursor);
        REQUIRE_THROWS_AS(c.insert_before({1, 2}), bad_cursor);

        SECTION("back again produces invalid cursor") {
            c.move_prev();
            REQUIRE(c.invalid());
            REQUIRE(!c.erased());
        }

        SECTION("forward points to next element") {
            c.move_next();
            REQUIRE(c);
            REQUIRE(!c.erased());

            point_t v = c.get();
            REQUIRE(v.x == 2);
            REQUIRE(v.y == 3);
        }
    }

    SECTION("clear forward") {
        for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next())
            c.erase();

        REQUIRE(ls.empty());
    }

    SECTION("clear backward") {
        for (auto c = ls.create_cursor(ls.seek_last); c; c.move_prev())
            c.erase();

        REQUIRE(ls.empty());
    }

    SECTION("remove forward") {
        comp.erase(std::remove_if(comp.begin(), comp.end(), [](auto& v) {
            return v.x == -1;
        }), comp.end());

        i32 removals = 0;
        for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next()) {
            point_t v = c.get();
            if (v.x == -1) {
                c.erase();
                ++removals;
            }
        }

        REQUIRE(removals == (COUNT / 3));
        REQUIRE(ls.size() == COUNT - removals);
        check_list_equals_container(ls, comp);
    }

    SECTION("remove backward") {
        comp.erase(std::remove_if(comp.begin(), comp.end(), [](auto& v) {
            return v.x != -1;
        }), comp.end());

        i32 removals = 0;
        for (auto c = ls.create_cursor(ls.seek_last); c; c.move_prev()) {
            point_t v = c.get();
            if (v.x != -1) {
                c.erase();
                ++removals;
            }
        }

        REQUIRE(removals == COUNT - (COUNT / 3));
        REQUIRE(ls.size() == COUNT - removals);
        check_list_equals_container(ls, comp);
    }
}

TEST_CASE("List destruction -> Cursor invalidation", "[list]") {
    test_file file(64);
    file.open();

    list<i32>::cursor pos;

    {
        node_allocator::anchor alloc_anchor;
        node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
        list<i32>::anchor list_anchor;
        list<i32> ls(make_anchor_handle(list_anchor), alloc);
        ls.push_back(1);
        pos = ls.create_cursor(ls.seek_first);
        REQUIRE(pos);
        REQUIRE(pos.get() == 1);
    }

    REQUIRE(!pos);
    REQUIRE_THROWS_AS(pos.get(), bad_cursor);
}

TEST_CASE("List cursors to deleted elements change state", "[list]") {
    test_file file(64);
    file.open();

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    list<i32>::anchor list_anchor;
    list<i32> ls(make_anchor_handle(list_anchor), alloc);

    ls.push_back(1);

    auto c = ls.create_cursor(ls.seek_first);
    REQUIRE(c.get() == 1);

    auto d = c;
    d.erase();
    REQUIRE(d.erased());

    REQUIRE(c.erased());
    REQUIRE_THROWS_AS(c.get(), bad_cursor);
}

TEST_CASE("List cursors are stable", "[list]") {
    test_file file(64);
    file.open();

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());
    list<i32>::anchor list_anchor;
    list<i32> ls(make_anchor_handle(list_anchor), alloc);

    struct expectation {
        list<i32>::cursor cursor;
        i32 value;
    };

    for (i32 i = 0; i < 1024; ++i) {
        ls.push_back(i);
    }

    std::vector<expectation> cursors;
    for (auto c = ls.create_cursor(ls.seek_first); c; c.move_next()) {
        if (c.get() % 4 == 0)
            cursors.push_back({c, c.get()});
    }

    for (auto c = ls.create_cursor(ls.seek_last); c; c.move_prev()) {
        if (c.get() % 4 != 0) {
            c.erase();
        }
    }

    REQUIRE(cursors.size() == ls.size());

    for (auto& e : cursors) {
        auto &c = e.cursor;

        if (!c)
            FAIL("Expected cursor for value " << e.value << " to be valid");

        if (c.erased())
            FAIL("Cursor for value " << e.value << " was mistakenly erased");

        if (c.get() != e.value)
            FAIL("Expected cursor for value " << e.value << " to be unchanged, but saw value " << c.get());
    }

    {
        i32 i = 1;
        for (auto& e : cursors) {
            auto& c = e.cursor;
            c.insert_before(-(i * 2));
            c.insert_after(-(i * 2) + 1);

            ++i;
        }
    }

    for (auto& e : cursors) {
        auto& c = e.cursor;
        if (!c)
            FAIL("Expected cursor for value " << e.value << " to be valid");

        if (c.get() != e.value)
            FAIL("Expected cursor for value " << e.value << " to be unchanged, but saw value " << c.get());
    }

    REQUIRE(ls.size() == (3 * cursors.size()));

    ls.clear();
    for (auto& e : cursors) {
        auto& c = e.cursor;
        if (!c.erased())
            FAIL("Expected cursor for value " << e.value << " to be erased");

        c.move_next();
        if (c)
            FAIL("Expected cursor for value " << e.value << " to become invalid after increment");
    }
}
