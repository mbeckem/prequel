#include <catch.hpp>

#include <extpp/node_allocator.hpp>
#include <extpp/raw_list.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "./test_file.hpp"

using namespace extpp;

struct anchor {
    node_allocator::anchor alloc;
    raw_list::anchor list;

    static constexpr auto get_binary_format() {
        return make_binary_format(&anchor::alloc, &anchor::list);
    }
};

using file_type = test_file<anchor>;

constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                           '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

std::string hex_str(const byte* data, size_t size) {
    std::string s;
    s.reserve(3 * size);
    for (size_t i = 0;;) {
        s += hexmap[(data[i] & 0xF0) >> 4];
        s += hexmap[data[i] & 0x0F];

        if (++i < size) {
            s += ' ';
        } else {
            break;
        }
    }
    return s;
}

void dump_list(const raw_list& list) {
    fmt::print(
        "Raw list:\n"
        "  Value size: {}\n"
        "  Block size: {}\n"
        "  Node Capacity: {}\n"
        "  Size: {}\n"
        "  Nodes: {}\n"
        "\n",
        list.value_size(),
        list.get_engine().block_size(),
        list.node_capacity(),
        list.size(),
        list.nodes());

    for (auto node = list.visit(); node; node.move_next()) {
        fmt::print(
            "  Node @{}:\n"
            "    Previous: @{}\n"
            "    Next: @{}\n"
            "    Size: {}\n",
            node.address(),
            node.prev_address(),
            node.next_address(),
            node.size());

        u32 size = node.size();
        u32 value_size = list.value_size();
        for (u32 i = 0; i < size; ++i) {
            const byte* data = static_cast<const byte*>(node.value(i));
            fmt::print("    {:>4}: {}\n", i, hex_str(data, value_size));
        }
        fmt::print("\n");
    }
}

TEST_CASE("raw list", "[list]") {
    static const u32 value_size = 4;
    static const u32 block_size = 64;

    file_type file(block_size);
    file.open();

    node_allocator alloc(file.get_anchor().member<&anchor::alloc>(), file.get_engine());
    {
        raw_list list(file.get_anchor().member<&anchor::list>(), value_size, alloc);
        REQUIRE(list.value_size() == 4);
        REQUIRE(list.node_capacity() == 11); // (64 - 20) / 4, header is 20 bytes.
        REQUIRE(list.empty());

        for (i32 i = 0; i < 32; ++i) {
            byte buffer[4];
            serialize(i, buffer, sizeof(buffer));
            list.push_back(buffer);
        }

        byte front[4] = { 0xff, 0xff, 0xff, 0xff };
        for (int i = 0; i < 10; ++i)
            list.push_front(front);

        dump_list(list);

        REQUIRE(!list.empty());
        REQUIRE(list.size() == 42);
        REQUIRE(list.begin() != list.end());

        int index = 0;
        for (auto i = list.begin(), e = list.end(); i != e; i.increment()) {
            const byte* data = static_cast<const byte*>(i.get());
            fmt::print("    {:>4}: {}\n", index++, hex_str(data, value_size));
        }
    }
}


