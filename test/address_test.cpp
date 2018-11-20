#include <catch.hpp>

#include <prequel/address.hpp>
#include <prequel/file_engine.hpp>

#include <type_traits>

using namespace prequel;

TEST_CASE("address comparisons", "[address]") {
    raw_address a1;
    raw_address a2(128);
    raw_address a3(256 + 5);
    raw_address a4(1024 + 1);

    REQUIRE(a1 < a2);
    REQUIRE(a1 < a3);
    REQUIRE(a1 < a4);

    REQUIRE(!(a1 >= a2));
    REQUIRE(!(a1 >= a3));
    REQUIRE(!(a1 >= a4));

    REQUIRE(a2 < a3);
    REQUIRE(a2 < a4);

    REQUIRE(a3 < a4);
}

TEST_CASE("instance <-> member", "[address]") {
    struct test_t {
        u32 a = 0;
        u16 b = 1;
        u16 c = 2;
        u64 d = 3;
        std::tuple<u8, u16> e{5, 6};
        u32 f[3] = {7, 8, 9};

        static constexpr auto get_binary_format() {
            return make_binary_format(&test_t::a, &test_t::b, &test_t::c, &test_t::d, &test_t::e,
                                      &test_t::f);
        }
    };

    address<test_t> base(raw_address(0));
    address<u32> a = base.member<&test_t::a>();
    address<u16> b = base.member<&test_t::b>();
    address<u16> c = base.member<&test_t::c>();
    address<u64> d = base.member<&test_t::d>();
    address<std::tuple<u8, u16>> e = base.member<&test_t::e>();
    address<u32[3]> f = base.member<&test_t::f>();

    REQUIRE(a.raw().value() == 0);
    REQUIRE(b.raw().value() == 4);
    REQUIRE(c.raw().value() == 6);
    REQUIRE(d.raw().value() == 8);
    REQUIRE(e.raw().value() == 16);
    REQUIRE(f.raw().value() == 19);

    REQUIRE(a.parent<&test_t::a>() == base);
    REQUIRE(b.parent<&test_t::b>() == base);
    REQUIRE(c.parent<&test_t::c>() == base);
    REQUIRE(d.parent<&test_t::d>() == base);
    REQUIRE(e.parent<&test_t::e>() == base);
    REQUIRE(f.parent<&test_t::f>() == base);
}

namespace {
static constexpr u32 block_size = 32;
}

TEST_CASE("copy", "[address]") {
    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file->truncate(50 * block_size);

    file_engine e(*file, block_size, 2);

    std::vector<byte> test_data(256);
    for (int i = 0; i < 256; ++i)
        test_data[i] = i;

    std::vector<byte> mem_file(50 * block_size); // Mirrors file on "disk".

    auto write = [&](size_t dest, const byte* data, size_t size) {
        std::memmove(&mem_file[dest], data, size);
        prequel::write(e, raw_address(dest), data, size);
    };

    auto copy = [&](size_t dest, size_t source, size_t n) {
        std::memmove(&mem_file[dest], &mem_file[source], n);
        prequel::copy(e, raw_address(source), raw_address(dest), n);
    };

    auto equal = [&]() {
        std::vector<byte> verify(50 * block_size);
        read(e, raw_address(0), verify.data(), verify.size());
        REQUIRE(std::equal(mem_file.begin(), mem_file.end(), verify.begin(), verify.end()));
    };

    SECTION("non-overlapping copy (after)") {
        write(36, test_data.data(), test_data.size());
        copy(367, 36, 256);
        equal();
    }

    SECTION("non-overlapping copy (before)") {
        write(477, test_data.data(), test_data.size());
        copy(61, 477, 256);
        equal();
    }

    SECTION("overlapping copy (before, 1)") {
        write(320, test_data.data(), 113);
        copy(319, 320, 113);
        equal();
    }

    SECTION("overlapping copy (before, 2)") {
        write(320, test_data.data(), 113);
        copy(260, 320, 113);
        equal();
    }

    SECTION("overlapping copy (after, 1)") {
        write(32, test_data.data(), 113);
        copy(321, 320, 113);
        equal();
    }

    SECTION("overlapping copy (after, 2)") {
        write(320, test_data.data(), 113);
        copy(380, 320, 113);
        equal();
    }

    SECTION("overlap with one block distance (after)") {
        write(32, test_data.data(), 256);
        copy(64, 32, 100);
        equal();
    }

    SECTION("overlap with one block distance, before") {
        write(64, test_data.data(), 256);
        copy(32, 64, 100);
        equal();
    }
}
