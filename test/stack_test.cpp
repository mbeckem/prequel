#include <catch.hpp>

#include <extpp/stack.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

using small_stack = stack<int, 128>;

template<typename Stack>
void dump_stack(const Stack& stack) {
    using visitor_t = typename Stack::visitor;

    std::ostream& out = std::cout;

    int indent = 0;
    auto line = [&]() -> std::ostream& {
        for (int i = 0; i < indent; ++i)
            out << "  ";
        return out;
    };

    auto buf = stack.buffered();

    line() << "Node capacity: " << stack.node_capacity() << "\n";
    line() << "Size: " << stack.size() << "\n";
    line() << "Nodes: " << stack.nodes() << "\n";
    line() << "Buffered: @" << buf[0] << ", @" << buf[1] << "\n";
    line() << "\n";

    for (visitor_t v = stack.visit(); v; v.move_previous()) {
        line() << "Node @" << v.address() << ":\n";

        ++indent;
        line() << "Previous: @" << v.previous_address() << "\n";
        line() << "Values: " << v.size() << "\n";

        ++indent;
        for (u32 i = 0; i < v.size(); ++i) {
            line() << i << ": " << v.value(i) << "\n";
        }
        --indent;

        --indent;
    }
    line() << "End of stack.\n";
}

TEST_CASE("stack", "[stack]") {
    test_file<small_stack::anchor, 128> file;
    file.open();

    const int max = (small_stack::node_capacity() * 7) / 2;

    {
        small_stack stack(file.anchor(), file.engine(), file.alloc());

        REQUIRE(stack.empty());
        REQUIRE(stack.size() == 0);
        REQUIRE(stack.nodes() == 0);

        for (int i = 0; i < max; ++i)
            stack.push(i);

        REQUIRE(stack.size() == max);
        REQUIRE(!stack.empty());
        REQUIRE(stack.top() == (max - 1));
    }
    file.close();

    file.open();
    {
        small_stack stack(file.anchor(), file.engine(), file.alloc());

        REQUIRE(stack.size() == max);
        REQUIRE(std::distance(stack.begin(), stack.end()) == max);

        int expected = max - 1;
        for (int i : stack) {
            if (i != expected) {
                FAIL("Unexpected value at the top, expected " << expected << " but got " << i);
            }
            expected--;
        }
        REQUIRE(expected == -1);

        for (int i = max; i-- > 0; ) {
            int t = stack.top();
            stack.pop();
            if (i != t) {
                FAIL("Unexpected value at the top, expected " << i << " but got " << t);
            }
        }

        REQUIRE(stack.empty());
        REQUIRE(stack.size() == 0);
        REQUIRE(stack.nodes() == 0);
    }
}
