#include <catch.hpp>

#include <prequel/address.hpp>
#include <prequel/formatting.hpp>
#include <prequel/serialization.hpp>

#include <array>

using namespace prequel;

struct no_format {
    u32 a, b, c, d;
};

struct has_format {
    u32 a, b, c, d;

private:
    friend prequel::binary_format_access;

    static constexpr auto get_binary_format() {
        return binary_format(&has_format::a, &has_format::b, &has_format::c, &has_format::d);
    }
};

static_assert(!has_binary_format<no_format>(), "Sanity check.");
static_assert(has_binary_format<has_format>(), "Sanity check.");

struct test1 {
    i8 v1 = 0;
    u64 v2 = 0;

    bool operator==(const test1& other) const { return v1 == other.v1 && v2 == other.v2; }

    static constexpr auto get_binary_format() { return binary_format(&test1::v1, &test1::v2); }
};

// Trivial serialization for bytes and byte containers. The compile down to a single memcpy call.
static_assert(detail::use_trivial_serializer<char>());
static_assert(detail::use_trivial_serializer<unsigned char>());
static_assert(detail::use_trivial_serializer<signed char>());
static_assert(detail::use_trivial_serializer<i8>());
static_assert(detail::use_trivial_serializer<u8>());
static_assert(detail::use_trivial_serializer<char[32]>());
static_assert(detail::use_trivial_serializer<std::array<char, 16>>());
static_assert(detail::use_trivial_serializer<std::array<std::array<std::array<char, 16>, 16>, 16>>());
static_assert(detail::use_trivial_serializer<std::array<char[32], 5>>());
static_assert(!detail::use_trivial_serializer<u32>());
static_assert(!detail::use_trivial_serializer<test1>());

TEST_CASE("roundtrips", "[serialization]") {
    auto rt = [](auto value) {
        return deserialize_from_buffer<decltype(value)>(serialize_to_buffer(value));
    };

#define ROUND_TRIP(type, v) REQUIRE(type(v) == rt(type(v)));

    ROUND_TRIP(u8, 0);
    ROUND_TRIP(u8, 1);
    ROUND_TRIP(u8, 127);
    ROUND_TRIP(u8, 255);

    ROUND_TRIP(u16, 0);
    ROUND_TRIP(u16, 1);
    ROUND_TRIP(u16, (123 << 8) + 122);
    ROUND_TRIP(u16, 51579);
    ROUND_TRIP(u16, -1);

    ROUND_TRIP(u32, 0);
    ROUND_TRIP(u32, 1);
    ROUND_TRIP(u32, -1);
    ROUND_TRIP(u32, (u32(123) << 24) + (u32(122) << 16) + (u32(121) << 8) + (u32(120) << 0));

    ROUND_TRIP(u64, 0);
    ROUND_TRIP(u64, -1);
    ROUND_TRIP(u64, (u64(123) << 56) + (u64(122) << 48) + (u64(121) << 40) + (u64(120) << 32)
                        + (u64(119) << 24) + (u64(118) << 16) + (u64(117) << 8) + (u64(116) << 0));

    ROUND_TRIP(i8, 0);
    ROUND_TRIP(i8, 1);
    ROUND_TRIP(i8, 127);
    ROUND_TRIP(i8, -56);

    ROUND_TRIP(i16, 0);
    ROUND_TRIP(i16, 1);
    ROUND_TRIP(i16, 32000);
    ROUND_TRIP(i16, -16555);
    ROUND_TRIP(i16, -1);

    ROUND_TRIP(i32, 0);
    ROUND_TRIP(i32, 1);
    ROUND_TRIP(i32, -1);
    ROUND_TRIP(i32, 56465455);
    ROUND_TRIP(i32, -56465455);

    ROUND_TRIP(i64, 0);
    ROUND_TRIP(i64, -1);
    ROUND_TRIP(i64, 6546465446511);
    ROUND_TRIP(i64, -6546465446511);

    ROUND_TRIP(float, float(0));
    ROUND_TRIP(float, float(-0));
    ROUND_TRIP(float, float(1.1e22));
    ROUND_TRIP(float, float(-1));
    ROUND_TRIP(float, float(-100.5));
    ROUND_TRIP(float, float(123456789));
    ROUND_TRIP(float, (std::numeric_limits<float>::infinity()));
    ROUND_TRIP(float, (-std::numeric_limits<float>::infinity()));

    ROUND_TRIP(double, double(0));
    ROUND_TRIP(double, double(-0));
    ROUND_TRIP(double, double(1.1e52));
    ROUND_TRIP(double, double(-1));
    ROUND_TRIP(double, double(-100.5));
    ROUND_TRIP(double, double(123456789));
    ROUND_TRIP(double, (std::numeric_limits<double>::infinity()));
    ROUND_TRIP(double, (-std::numeric_limits<double>::infinity()));

    ROUND_TRIP(bool, true);
    ROUND_TRIP(bool, false);

    ROUND_TRIP(test1, (test1{123, 123456}));
    ROUND_TRIP(test1, (test1{0, 1}));

    ROUND_TRIP((std::tuple<u32, u64>), (std::tuple<u32, u64>(123, 123456)));

    ROUND_TRIP(raw_address, raw_address(1234));
    ROUND_TRIP(raw_address, raw_address());

    ROUND_TRIP((address<i64>), (address<i64>()));
    ROUND_TRIP((address<i64>), (address<i64>(raw_address(8000))));

#undef ROUND_TRIP
}

TEST_CASE("binary representation", "[serialization]") {
    // Test little endian representation

    SECTION("8 bit") {
        auto buffer = serialize_to_buffer(u8(0xe7));
        REQUIRE(buffer.size() == 1);
        REQUIRE(buffer[0] == 0xe7);
    }
    SECTION("16 bit") {
        auto buffer = serialize_to_buffer(u16(0xc97b));
        REQUIRE(buffer.size() == 2);
        REQUIRE(buffer[0] == 0xc9);
        REQUIRE(buffer[1] == 0x7b);
    }
    SECTION("32 bit") {
        auto buffer = serialize_to_buffer(u32(0x7b7c7d7e));
        REQUIRE(buffer.size() == 4);
        REQUIRE(buffer[0] == 0x7b);
        REQUIRE(buffer[1] == 0x7c);
        REQUIRE(buffer[2] == 0x7d);
        REQUIRE(buffer[3] == 0x7e);
    }
    SECTION("64 bit") {
        auto buffer = serialize_to_buffer(u64(0x7b7c7d7e7f808182ULL));
        REQUIRE(buffer.size() == 8);
        REQUIRE(buffer[0] == 0x7b);
        REQUIRE(buffer[1] == 0x7c);
        REQUIRE(buffer[2] == 0x7d);
        REQUIRE(buffer[3] == 0x7e);
        REQUIRE(buffer[4] == 0x7f);
        REQUIRE(buffer[5] == 0x80);
        REQUIRE(buffer[6] == 0x81);
        REQUIRE(buffer[7] == 0x82);
    }
    SECTION("bool (1 byte)") {
        auto buffer = serialize_to_buffer(true);
        REQUIRE(buffer.size() == 1);
        REQUIRE(buffer[0] == 1);

        buffer = serialize_to_buffer(false);
        REQUIRE(buffer[0] == 0);
    }
    SECTION("8 bit (signed)") {
        auto buffer = serialize_to_buffer(i8(-25));
        REQUIRE(buffer.size() == 1);
        REQUIRE(buffer[0] == 0xe7);
    }
    SECTION("16 bit (signed)") {
        auto buffer = serialize_to_buffer(i16(-13957));
        REQUIRE(buffer.size() == 2);
        REQUIRE(buffer[0] == 0xc9);
        REQUIRE(buffer[1] == 0x7b);
    }
    SECTION("32 bit (signed)") {
        auto buffer = serialize_to_buffer(i32(-881033858));
        REQUIRE(buffer.size() == 4);
        REQUIRE(buffer[0] == 0xcb);
        REQUIRE(buffer[1] == 0x7c);
        REQUIRE(buffer[2] == 0x7d);
        REQUIRE(buffer[3] == 0x7e);
    }
    SECTION("64 bit (signed)") {
        auto buffer = serialize_to_buffer(i64(-3784011604639579774LL));
        REQUIRE(buffer.size() == 8);
        REQUIRE(buffer[0] == 0xcb);
        REQUIRE(buffer[1] == 0x7c);
        REQUIRE(buffer[2] == 0x7d);
        REQUIRE(buffer[3] == 0x7e);
        REQUIRE(buffer[4] == 0x7f);
        REQUIRE(buffer[5] == 0x80);
        REQUIRE(buffer[6] == 0x81);
        REQUIRE(buffer[7] == 0x82);
    }
}

TEST_CASE("tuple serialization", "[serialization]") {
    auto buffer = serialize_to_buffer(std::tuple<u8, u32, u8>(0xa0, 0x7b7c7d7e, 0xa1));
    REQUIRE(buffer.size() == 6);

    auto m1 = serialize_to_buffer(u8(0xa0));
    auto m2 = serialize_to_buffer(u32(0x7b7c7d7e));
    auto m3 = serialize_to_buffer(u8(0xa1));

    REQUIRE(buffer[0] == m1.at(0));
    REQUIRE(std::equal(&buffer[1], &buffer[5], m2.begin(), m2.end()));
    REQUIRE(buffer[5] == m3.at(0));
}

TEST_CASE("optional serialization", "[serialization]") {
    struct test_t {
        i32 a = 0;
        i64 b = 1;

        static constexpr auto get_binary_format() { return binary_format(&test_t::a, &test_t::b); }

        bool operator==(const test_t& other) const { return a == other.a && b == other.b; }
    };

    using opt_t = std::optional<test_t>;

    REQUIRE(prequel::serialized_size<opt_t>() == 13); // 1 byte + size(test_t)

    {
        auto buffer = prequel::serialize_to_buffer(opt_t());
        REQUIRE(buffer.size() == 13);

        std::array<byte, 13> expected{};
        REQUIRE(std::equal(buffer.begin(), buffer.end(), expected.begin(), expected.end()));

        opt_t parsed = prequel::deserialize<opt_t>(buffer.data(), buffer.size());
        REQUIRE(!parsed);
    }

    {
        test_t test;
        test.a = 5;
        test.b = 1982738911232LL;

        auto buffer = prequel::serialize_to_buffer(opt_t(test));
        REQUIRE(buffer.size() == 13);

        std::array<byte, 13> expected{};
        expected[0] = 1;
        prequel::serialize(test, expected.data() + 1);
        REQUIRE(std::equal(buffer.begin(), buffer.end(), expected.begin(), expected.end()));

        opt_t parsed = prequel::deserialize<opt_t>(buffer.data());
        REQUIRE(parsed);
        REQUIRE(*parsed == test);
    }
}

TEST_CASE("variant serialization", "[serialization]") {
    struct point {
        i32 x = 0;
        i32 y = 0;
        i32 z = 0;

        static constexpr auto get_binary_format() {
            return binary_format(&point::x, &point::y, &point::z);
        }

        bool operator==(const point& p) const { return x == p.x && y == p.y && z == p.z; }
    };

    REQUIRE(prequel::serialized_size<std::variant<i32, double>>()
            == 9); // 1 + max(size(i32), size(double))
    REQUIRE(prequel::serialized_size<std::variant<bool, char>>() == 2); // 1 + max(1, 1)

    using variant_t = std::variant<i32, double, point>;
    REQUIRE(prequel::serialized_size<variant_t>() == 13); // 1 + serialized_size(point)

    {
        auto buffer = prequel::serialize_to_buffer(variant_t(point{1, 2, -1}));
        REQUIRE(buffer.size() == 13);

        std::array<byte, 13> expected{};
        expected[0] = 2; // point
        prequel::serialize(point{1, 2, -1}, expected.data() + 1);

        REQUIRE(std::equal(buffer.begin(), buffer.end(), expected.begin(), expected.end()));

        variant_t v = deserialize<variant_t>(buffer.data(), buffer.size());
        REQUIRE(std::get<point>(v) == point{1, 2, -1});
    }

    {
        auto buffer = prequel::serialize_to_buffer(variant_t(i32(-55)));
        REQUIRE(buffer.size() == 13);

        std::array<byte, 13> expected{};
        expected[0] = 0; // i32
        prequel::serialize(i32(-55), expected.data() + 1);

        REQUIRE(std::equal(buffer.begin(), buffer.end(), expected.begin(), expected.end()));

        variant_t v = deserialize<variant_t>(buffer.data(), buffer.size());
        REQUIRE(std::get<i32>(v) == -55);
    }

    {
        auto buffer = prequel::serialize_to_buffer(variant_t(double(123.1234)));
        REQUIRE(buffer.size() == 13);

        std::array<byte, 13> expected{};
        expected[0] = 1; // double
        prequel::serialize(double(123.1234), expected.data() + 1);

        REQUIRE(std::equal(buffer.begin(), buffer.end(), expected.begin(), expected.end()));

        variant_t v = deserialize<variant_t>(buffer.data(), buffer.size());
        REQUIRE(std::get<double>(v) == 123.1234);
    }
}

TEST_CASE("array serialization", "[serialization]") {
    SECTION("c style array") {
        u32 data[] = {
            0xa0b0c0d0,
            0xa1b1c1d1,
            0xa2b2d2d2,
        };

        auto buffer = serialize_to_buffer(data);
        REQUIRE(buffer.size() == 12);

        auto m1 = serialize_to_buffer(u32(0xa0b0c0d0));
        auto m2 = serialize_to_buffer(u32(0xa1b1c1d1));
        auto m3 = serialize_to_buffer(u32(0xa2b2d2d2));

        REQUIRE(std::equal(&buffer[0], &buffer[4], m1.begin(), m1.end()));
        REQUIRE(std::equal(&buffer[4], &buffer[8], m2.begin(), m2.end()));
        REQUIRE(std::equal(&buffer[8], &buffer[12], m3.begin(), m3.end()));
    }

    SECTION("c++ style array") {
        std::array<u16, 4> data{
            0xa0b0,
            0xa1b1,
            0xa2b2,
            0xa3b3,
        };

        auto buffer = serialize_to_buffer(data);
        REQUIRE(buffer.size() == 8);

        auto m1 = serialize_to_buffer(u16(0xa0b0));
        auto m2 = serialize_to_buffer(u16(0xa1b1));
        auto m3 = serialize_to_buffer(u16(0xa2b2));
        auto m4 = serialize_to_buffer(u16(0xa3b3));

        REQUIRE(std::equal(&buffer[0], &buffer[2], m1.begin(), m1.end()));
        REQUIRE(std::equal(&buffer[2], &buffer[4], m2.begin(), m2.end()));
        REQUIRE(std::equal(&buffer[4], &buffer[6], m3.begin(), m3.end()));
        REQUIRE(std::equal(&buffer[6], &buffer[8], m4.begin(), m4.end()));
    }
}

TEST_CASE("struct serialization", "[serialization]") {
    struct empty_t {
        static constexpr auto get_binary_format() { return binary_format<empty_t>(); }
    } empty;

    struct simple_t {
        i32 x = -1;
        i32 y = 1;

        static constexpr auto get_binary_format() {
            return binary_format(&simple_t::x, &simple_t::y);
        }
    } simple;

    struct complex_t {
        simple_t v1;
        simple_t v2;
        u8 v3 = u8(-1);

        static constexpr auto get_binary_format() {
            return binary_format(&complex_t::v1, &complex_t::v2, &complex_t::v3);
        }
    } complex;

    REQUIRE(serialized_size(empty) == 0);
    REQUIRE(serialized_size(simple) == 8);
    REQUIRE(serialized_size(complex) == 17);

    SECTION("empty") {
        auto buffer = serialize_to_buffer(empty);
        REQUIRE(buffer.size() == 0);
    }

    SECTION("simple") {
        auto buffer = serialize_to_buffer(simple);
        auto back = deserialize_from_buffer<simple_t>(buffer);

        REQUIRE(back.x == simple.x);
        REQUIRE(back.y == simple.y);

        auto minus_one = serialize_to_buffer(i32(-1));
        auto one = serialize_to_buffer(i32(1));

        REQUIRE(std::equal(&buffer[0], &buffer[4], minus_one.begin(), minus_one.end()));
        REQUIRE(std::equal(&buffer[4], &buffer[8], one.begin(), one.end()));
    }

    SECTION("complex") {
        auto buffer = serialize_to_buffer(complex);

        auto s = serialize_to_buffer(simple);
        auto m = serialize_to_buffer(u8(-1));

        REQUIRE(std::equal(&buffer[0], &buffer[8], s.begin(), s.end()));
        REQUIRE(std::equal(&buffer[8], &buffer[16], s.begin(), s.end()));
        REQUIRE(buffer[16] == m[0]);
    }

    SECTION("address") {
        auto buffer = serialize_to_buffer(address<u64>(raw_address(0x1000)));
        REQUIRE(buffer.size() == 8);
    }
}

TEST_CASE("serialized offset of", "[serialization]") {
    struct s1 {
        u32 x;
        u8 y;
        u32 z;

        static constexpr auto get_binary_format() { return binary_format(&s1::x, &s1::y, &s1::z); }
    };

    struct s2 {
        u64 a;
        u64 b;
        s1 c;
        u8 d;

        static constexpr auto get_binary_format() {
            return binary_format(&s2::a, &s2::b, &s2::c, &s2::d);
        }
    };

    static constexpr size_t ox = serialized_offset<&s1::x>();
    static constexpr size_t oy = serialized_offset<&s1::y>();
    static constexpr size_t oz = serialized_offset<&s1::z>();

    REQUIRE(ox == 0);
    REQUIRE(oy == 4);
    REQUIRE(oz == 5);

    static constexpr size_t oa = serialized_offset<&s2::a>();
    static constexpr size_t ob = serialized_offset<&s2::b>();
    static constexpr size_t oc = serialized_offset<&s2::c>();
    static constexpr size_t od = serialized_offset<&s2::d>();

    REQUIRE(oa == 0);
    REQUIRE(ob == 8);
    REQUIRE(oc == 16);
    REQUIRE(od == 25);
}

TEST_CASE("complex struct", "[serialization]") {
    /*
        The sqlite 3 file header (from https://www.sqlite.org/fileformat2.html#the_database_header)

        0	16	The header string: "SQLite format 3\000"
        16	2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
        18	1	File format write version. 1 for legacy; 2 for WAL.
        19	1	File format read version. 1 for legacy; 2 for WAL.
        20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
        21	1	Maximum embedded payload fraction. Must be 64.
        22	1	Minimum embedded payload fraction. Must be 32.
        23	1	Leaf payload fraction. Must be 32.
        24	4	File change counter.
        28	4	Size of the database file in pages. The "in-header database size".
        32	4	Page number of the first freelist trunk page.
        36	4	Total number of freelist pages.
        40	4	The schema cookie.
        44	4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
        48	4	Default page cache size.
        52	4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
        56	4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
        60	4	The "user version" as read and set by the user_version pragma.
        64	4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
        68	4	The "Application ID" set by PRAGMA application_id.
        72	20	Reserved for expansion. Must be zero.
        92	4	The version-valid-for number.
        96	4	SQLITE_VERSION_NUMBER
    */

    struct sqlite_header_t {
        static std::array<u8, 16> sqlite_magic() {
            std::array<u8, 16> buffer{};
            memcpy(buffer.data(), "SQLite format 3", strlen("SQLite format 3"));
            return buffer;
        }

        std::array<u8, 16> magic = sqlite_magic();

        u16 page_size = 0;
        u8 write_version = 0;
        u8 read_version = 0;
        u8 reserved_at_end = 0;

        u8 max_embedded_payload_fraction = 64;
        u8 min_embedded_payload_fraction = 32;
        u8 leaf_payload_fraction = 32;

        u32 file_change_counter = 0;
        u32 file_size = 0;

        u32 first_freelist_page = 0;
        u32 freelist_pages = 0;

        u32 schema_cookie = 0;
        u32 schema_format = 0;

        u32 default_page_cache_size = 0;
        u32 largest_btree_root_page = 0;
        u32 text_encoding = 0;
        u32 user_version = 0;
        u32 incremental_vacuum = 0;
        u32 application_id = 0;
        u8 reserved[20]{};

        u32 version_valid_for = 0;
        u32 sqlite_version_number = 0;

        static constexpr auto get_binary_format() {
            return binary_format(
                &sqlite_header_t::magic, &sqlite_header_t::page_size,
                &sqlite_header_t::write_version, &sqlite_header_t::read_version,
                &sqlite_header_t::reserved_at_end,

                &sqlite_header_t::max_embedded_payload_fraction,
                &sqlite_header_t::min_embedded_payload_fraction,
                &sqlite_header_t::leaf_payload_fraction,

                &sqlite_header_t::file_change_counter, &sqlite_header_t::file_size,

                &sqlite_header_t::first_freelist_page, &sqlite_header_t::freelist_pages,

                &sqlite_header_t::schema_cookie, &sqlite_header_t::schema_format,

                &sqlite_header_t::default_page_cache_size,
                &sqlite_header_t::largest_btree_root_page, &sqlite_header_t::text_encoding,
                &sqlite_header_t::user_version, &sqlite_header_t::incremental_vacuum,
                &sqlite_header_t::application_id, &sqlite_header_t::reserved,

                &sqlite_header_t::version_valid_for, &sqlite_header_t::sqlite_version_number);
        }
    };

    REQUIRE(serialized_size<sqlite_header_t>() == 100);

    sqlite_header_t hdr;
    hdr.page_size = 4096;
    hdr.file_size = 12345;
    hdr.default_page_cache_size = 128;
    hdr.text_encoding = 1;
    hdr.user_version = 42;
    hdr.application_id = 777;

    auto buffer = serialize_to_buffer(hdr);

    size_t size_offset = serialized_offset<&sqlite_header_t::default_page_cache_size>();
    REQUIRE(size_offset == 48);

    u32 page_cache_size = deserialize<u32>(buffer.data() + size_offset);
    REQUIRE(page_cache_size == 128);
}

TEST_CASE("custom serializer", "[serialization]") {
    struct free_t {
        u64 free : 1; // 1
        u64 next : 63;
    };
    struct object_t {
        u64 free : 1; // 0
        u64 marked : 1;
        u64 addr : 62;
    };

    static constexpr u64 free_bit = u64(1) << 63;
    static constexpr u64 marked_bit = u64(1) << 62;

    union entry_t {
        free_t free;
        object_t object;

        static entry_t make_free(u64 next) {
            PREQUEL_ASSERT(next < (u64(1) << 63), "next too large.");

            entry_t e;
            e.free.free = 1;
            e.free.next = next;
            return e;
        }

        static entry_t make_object(bool marked, u64 addr) {
            PREQUEL_ASSERT(addr < (u64(1) << 62), "addr too large.");

            entry_t e;
            e.object.free = 0;
            e.object.marked = marked;
            e.object.addr = addr;
            return e;
        }

        bool operator==(const entry_t& other) const {
            if (free.free != other.free.free) {
                return false;
            }

            if (free.free) {
                return free.next == other.free.next;
            }
            return object.marked == other.object.marked && object.addr == other.object.addr;
        }

        struct binary_serializer {
            constexpr static size_t serialized_size() { return prequel::serialized_size<u64>(); }

            static void serialize(const entry_t& e, byte* b) {
                u64 val = 0;
                if (e.free.free) {
                    val |= free_bit;
                    val |= e.free.next;
                } else {
                    val |= e.object.marked ? marked_bit : 0;
                    val |= e.object.addr;
                }
                prequel::serialize(val, b);
            }

            static void deserialize(entry_t& e, const byte* b) {
                u64 val = prequel::deserialize<u64>(b);
                if (val & free_bit) {
                    e.free.free = 1;
                    e.free.next = val;
                } else {
                    e.object.free = 0;
                    e.object.marked = val & marked_bit ? 1 : 0;
                    e.object.addr = val;
                }
            }
        };
    };
    static_assert(detail::has_explicit_serializer<entry_t>::value, "");

    REQUIRE(sizeof(entry_t) == sizeof(u64));
    REQUIRE(serialized_size<entry_t>() == serialized_size<u64>());

    auto rt = [](auto value) {
        return deserialize_from_buffer<decltype(value)>(serialize_to_buffer(value));
    };

#define ROUND_TRIP(v) REQUIRE(v == rt(v));

    ROUND_TRIP(entry_t::make_free(0));
    ROUND_TRIP(entry_t::make_free(1));
    ROUND_TRIP(entry_t::make_free(u64(-1) + (u64(1) << 63)));

    ROUND_TRIP(entry_t::make_object(true, 0));
    ROUND_TRIP(entry_t::make_object(false, 0));
    ROUND_TRIP(entry_t::make_object(true, u64(-1) + (u64(1) << 62)));
    ROUND_TRIP(entry_t::make_object(false, u64(-1) + (u64(1) << 62)));
    ROUND_TRIP(entry_t::make_object(true, 123456789ULL));

#undef ROUND_TRIP
}

TEST_CASE("nested objects", "[serialization]") {
    struct v1 {
        u32 a = u32(-4);

        struct v2 {
            u32 b = 99;
            byte c = 1;
            struct v3 {
                u64 d = 5;
                u64 e = u64(-444);

                static constexpr auto get_binary_format() { return binary_format(&v3::d, &v3::e); }
            } v3_;

            static constexpr auto get_binary_format() {
                return binary_format(&v2::b, &v2::c, &v2::v3_);
            }
        } v2_;

        static constexpr auto get_binary_format() { return binary_format(&v1::a, &v1::v2_); }
    } v1_;

    constexpr size_t offset = serialized_offset<&v1::v2_, &v1::v2::v3_, &v1::v2::v3::e>();
    REQUIRE(offset == 17);

    auto buffer = serialize_to_buffer(v1_);

    REQUIRE(deserialize_member<&v1::a>(buffer.data(), buffer.size()) == u32(-4));
    REQUIRE(deserialize_member<&v1::v2_, &v1::v2::c>(buffer.data(), buffer.size()) == byte(1));
    REQUIRE(deserialize_member<&v1::v2_, &v1::v2::v3_, &v1::v2::v3::e>(buffer.data(), buffer.size())
            == u64(-444));

    serialize_member<&v1::v2_, &v1::v2::b>(u32(-1), buffer.data(), buffer.size());

    auto value = deserialize<v1>(buffer.data());
    REQUIRE(value.v2_.b == u32(-1));
}

TEST_CASE("non default constructible", "[serialization]") {
    struct test_inner {
        i32 y = 0;

        test_inner(i32 y_)
            : y(y_) {}

        test_inner(deserialization_tag) {}

        static constexpr auto get_binary_format() { return binary_format(&test_inner::y); }
    };

    struct test_outer {
        i32 x = 0;
        test_inner inner;

        test_outer(i32 x_, i32 y)
            : x(x_)
            , inner(y) {}

        test_outer(deserialization_tag t)
            : inner(t) {}

        static constexpr auto get_binary_format() {
            return binary_format(&test_outer::x, &test_outer::inner);
        }
    };

    REQUIRE(serialized_size<test_outer>() == 8);

    auto buffer = serialize_to_buffer(test_outer(3, -1234));
    auto value = deserialize_from_buffer<test_outer>(buffer);

    REQUIRE(value.x == 3);
    REQUIRE(value.inner.y == -1234);
}

TEST_CASE("enums are supported", "[serialization]") {
    enum test_enum_1 : i64 { test_1 = 5, test_2 = 1337 };

    static_assert(serialized_size<test_enum_1>() == 8, "serialized as i64");

    enum class test_enum_2 : u32 { first = 999, second = 55555, last = u32(-1) };

    static_assert(serialized_size<test_enum_2>() == 4, "serialized as u32");

    {
        auto buffer = serialize_to_buffer(test_1);
        auto value = deserialize_from_buffer<test_enum_1>(buffer);
        REQUIRE(value == test_1);
    }

    {
        auto buffer = serialize_to_buffer(test_2);
        auto value = deserialize_from_buffer<test_enum_1>(buffer);
        REQUIRE(value == test_2);
    }

    {
        auto buffer = serialize_to_buffer(test_enum_2::first);
        auto value = deserialize_from_buffer<test_enum_2>(buffer);
        REQUIRE(value == test_enum_2::first);
    }

    {
        auto buffer = serialize_to_buffer(test_enum_2::second);
        auto value = deserialize_from_buffer<test_enum_2>(buffer);
        REQUIRE(value == test_enum_2::second);
    }

    {
        auto buffer = serialize_to_buffer(test_enum_2::last);
        auto value = deserialize_from_buffer<test_enum_2>(buffer);
        REQUIRE(value == test_enum_2::last);
    }
}
