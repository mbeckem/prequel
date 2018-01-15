#include <catch.hpp>

#include <extpp/detail/inlined_any.hpp>

#include <memory>

using namespace extpp;
using namespace extpp::detail;

using any_t = inlined_any<64>;

TEST_CASE("inlined any basic usage", "[inlined-any]") {
    any_t a;
    REQUIRE(!a.has_value());
    REQUIRE(a.type() == typeid(void));

    any_t b(3);
    REQUIRE(b.has_value());
    REQUIRE(b.type() == typeid(int));
    REQUIRE(b.get<int>() == 3);

    const double& x = 3.0;
    any_t c(x);
    REQUIRE(c.has_value());
    REQUIRE(c.type() == typeid(double));
    REQUIRE(c.get<double>() == 3);

    struct complex {
        double a;
        double b;
    };

    any_t d(complex{3, 4});
    REQUIRE(d.has_value());
    REQUIRE(d.type() == typeid(complex));
    REQUIRE(d.get<complex>().a == 3);

    a = d;
    REQUIRE(a.has_value());
    REQUIRE(a.type() == typeid(complex));
    REQUIRE(a.get<complex>().b == 4);

    b.reset();
    REQUIRE(!b.has_value());
    b = std::string("123");
    REQUIRE(b.get<std::string>() == "123");

    c = std::move(b);
    REQUIRE(b.type() == typeid(std::string));
    REQUIRE(c.get<std::string>() == "123");

    any_t e = std::make_shared<int>(7);
    REQUIRE(e.type() == typeid(std::shared_ptr<int>));

    any_t f = std::move(e);
    REQUIRE(f.type() == typeid(std::shared_ptr<int>));
    REQUIRE(*f.get<std::shared_ptr<int>>() == 7);
    REQUIRE(!e.get<std::shared_ptr<int>>());

    any_t g = f;
    REQUIRE(g.get<std::shared_ptr<int>>().use_count() == 2);
}
