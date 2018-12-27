#include <catch.hpp>

#include <prequel/fixed_string.hpp>
#include <prequel/serialization.hpp>

using namespace prequel;

TEST_CASE("fixed cstring", "[fixed-string]") {
    using string_t = fixed_cstring<16>;
    static_assert(serialized_size<string_t>() == 16);

    const char normal[] = "hello world";
    const char too_large[] = "0123456789ABCDEF!";

    {
        string_t empty;
        REQUIRE(empty.size() == 0);
        REQUIRE(empty.view().empty());
        REQUIRE(empty.begin() == empty.end());

        byte buffer[16];
        serialize(empty, buffer, sizeof(buffer));

        byte expected_buffer[16]{};
        REQUIRE(std::memcmp(buffer, expected_buffer, sizeof(buffer)) == 0);
    }

    {
        string_t str(normal);
        REQUIRE(str.size() == strlen(normal));
        REQUIRE(str.begin() + strlen(normal) == str.end());
        REQUIRE(str.view() == std::string_view("hello world"));

        byte expected_buffer[16]{};
        std::memcpy(expected_buffer, normal, strlen(normal));

        byte buffer[16];
        serialize(str, buffer, sizeof(buffer));

        REQUIRE(std::memcmp(buffer, expected_buffer, sizeof(buffer)) == 0);
    }

    REQUIRE_THROWS_AS(string_t(too_large), bad_argument);
}

TEST_CASE("fixed string", "[fixed-string]") {
    using string_t = fixed_string<16>;
    static constexpr u32 size = serialized_size<string_t>();
    static_assert(size == 17);

    const char normal[] = "hello world";
    const char too_large[] = "0123456789ABCDEF!";
    const char with_nul[] = {'h', '\0', 'e', 'l', 'l'};

    {
        string_t empty;
        REQUIRE(empty.size() == 0);
        REQUIRE(empty.view().empty());
        REQUIRE(empty.begin() == empty.end());

        serialized_buffer<string_t> buffer = serialize_to_buffer(empty);
        serialized_buffer<string_t> expected_buffer{};

        REQUIRE(buffer == expected_buffer);
    }

    {
        string_t str(normal);
        REQUIRE(str.size() == strlen(normal));
        REQUIRE(str.begin() + strlen(normal) == str.end());
        REQUIRE(str.view() == std::string_view("hello world"));

        serialized_buffer<string_t> buffer = serialize_to_buffer(str);

        serialized_buffer<string_t> expected_buffer{};
        expected_buffer[0] = str.size();
        std::memcpy(expected_buffer.data() + 1, normal, strlen(normal));

        REQUIRE(buffer == expected_buffer);
    }

    {
        string_t str(with_nul, sizeof(with_nul));
        REQUIRE(str.size() == sizeof(with_nul));
        REQUIRE(str.begin() + sizeof(with_nul) == str.end());
        REQUIRE(str.view() == std::string_view(with_nul, sizeof(with_nul)));

        serialized_buffer<string_t> buffer = serialize_to_buffer(str);

        serialized_buffer<string_t> expected_buffer{};
        expected_buffer[0] = sizeof(with_nul);
        std::memcpy(expected_buffer.data() + 1, with_nul, sizeof(with_nul));

        REQUIRE(buffer == expected_buffer);
    }

    REQUIRE_THROWS_AS(string_t(too_large), bad_argument);
}

TEST_CASE("large fixed strings", "[fixed-string]") {
    using string_t = fixed_string<256>;
    static_assert(serialized_size<string_t>() == 258); // 2 bytes for size

    std::array<char, 256> blob;
    for (auto& c : blob) {
        c = 1;
    }

    string_t str(blob.data(), blob.size());
    REQUIRE(str.size() == blob.size());

    serialized_buffer<string_t> buffer = serialize_to_buffer(str);

    serialized_buffer<string_t> expected_buffer;
    static_assert(expected_buffer.size() == 258);
    serialize(u16(256), expected_buffer.data());
    std::memcpy(expected_buffer.data() + 2, blob.data(), blob.size());

    REQUIRE(buffer == expected_buffer);
}
