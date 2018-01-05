#include <catch.hpp>

#include <extpp/detail/iter_tools.hpp>

#include <sstream>
#include <tuple>

using namespace extpp;
using namespace extpp::detail;

TEST_CASE("tuple for each", "[iter_tools]") {
    std::stringstream stream;
    auto visitor = [&](const auto& e) {
        stream << e << ".";
    };

    SECTION("empty") {
        tuple_for_each(std::tuple<>(), visitor);
        REQUIRE(stream.str().empty());
    }
    SECTION("non-empty") {
        tuple_for_each(std::tuple(1, 2, "Hello World"), visitor);
        REQUIRE(stream.str() == "1.2.Hello World.");
    }
}

template<typename... T>
static constexpr int add(std::tuple<T...> v) {
    int result = 0;
    tuple_for_each(v, [&](auto n) {
        result += n;
    });
    return result;
}

TEST_CASE("constexpr tuple for each", "[iter_tools]") {
    constexpr auto n1 = add(std::tuple(1, 2, 3));
    REQUIRE(n1 == 6);

    constexpr auto n2 = add(std::tuple<>());
    REQUIRE(n2 == 0);
}
