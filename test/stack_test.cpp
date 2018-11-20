#include <catch.hpp>

#include <prequel/node_allocator.hpp>
#include <prequel/raw_stack.hpp>
#include <prequel/stack.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace prequel;

namespace {

constexpr u32 block_size = 256;
}

TEST_CASE("stack", "[stack]") {
    using stack_t = stack<i32>;

    test_file file(block_size);

    node_allocator::anchor alloc_anchor;
    node_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    stack_t::anchor stack_anchor;

    int max = 0;
    {
        stack_t stack(make_anchor_handle(stack_anchor), alloc);
        max = (stack.node_capacity() * 7) / 2;

        REQUIRE(stack.empty());
        REQUIRE(stack.size() == 0);
        REQUIRE(stack.nodes() == 0);

        for (int i = 0; i < max; ++i)
            stack.push(i);

        REQUIRE(stack.size() == max);
        REQUIRE(!stack.empty());
        REQUIRE(stack.top() == (max - 1));

        for (int i = 0; i < 35; ++i)
            stack.pop();
        max -= 35;

        REQUIRE(stack.top() == (max - 1));

        // stack.raw().dump(std::cout);
    }

    {
        stack_t stack(make_anchor_handle(stack_anchor), alloc);

        REQUIRE(stack.size() == max);

        for (int i = max; i-- > 0;) {
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
