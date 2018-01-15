#include <catch.hpp>

#include <extpp/io.hpp>
#include <extpp/file_engine.hpp>
#include <extpp/handle.hpp>

using namespace extpp;

static constexpr u32 bs = 32;

TEST_CASE("copy", "[handle]") {
    auto file = memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create);
    file->truncate(100 * bs);

    file_engine e(*file, bs, 2);

    std::vector<byte> test_data(256);
    for (int i = 0; i < 256; ++i)
        test_data[i] = i;

    SECTION("non-overlapping copy (after)") {
        write(e, raw_address::byte_address(36), test_data.data(), test_data.size());
        copy(e, raw_address::byte_address(367), raw_address::byte_address(36), 256);

        std::vector<byte> result(256);
        read(e, raw_address::byte_address(367), result.data(), result.size());
        REQUIRE(result == test_data);
    }

    SECTION("non-overlapping copy (before)") {
        write(e, raw_address::byte_address(477), test_data.data(), test_data.size());
        copy(e, raw_address::byte_address(61), raw_address::byte_address(477), 256);

        std::vector<byte> result(256);
        read(e, raw_address::byte_address(61), result.data(), result.size());
        REQUIRE(result == test_data);
    }

    SECTION("overlapping copy (before, 1)") {
        write(e, raw_address::byte_address(320), test_data.data(), 113);
        copy(e, raw_address::byte_address(319), raw_address::byte_address(320), 113);

        std::vector<byte> result(113);
        read(e, raw_address::byte_address(319), result.data(), result.size());
        REQUIRE(std::equal(result.begin(), result.end(), test_data.begin(), test_data.begin() + 113));

        std::vector<byte> trailer(1);
        read(e, raw_address::byte_address(320) + 112, trailer.data(), trailer.size());
        REQUIRE(trailer[0] == 112); // Non-overlapping part is intact.
    }

    SECTION("overlapping copy (before, 2)") {
        write(e, raw_address::byte_address(320), test_data.data(), 113);
        copy(e, raw_address::byte_address(260), raw_address::byte_address(320), 113);

        std::vector<byte> result(113);
        read(e, raw_address::byte_address(260), result.data(), result.size());
        REQUIRE(std::equal(result.begin(), result.end(), test_data.begin(), test_data.begin() + 113));

        std::vector<byte> trailer(60);
        read(e, raw_address::byte_address(320) + 53, trailer.data(), trailer.size());
        REQUIRE(std::equal(trailer.begin(), trailer.end(), test_data.begin() + 53, test_data.begin() + 113));
    }

    SECTION("overlapping copy (after, 1)") {
        write(e, raw_address::byte_address(320), test_data.data(), 113);
        copy(e, raw_address::byte_address(321), raw_address::byte_address(320), 113);

        std::vector<byte> result(113);
        read(e, raw_address::byte_address(321), result.data(), result.size());
        REQUIRE(std::equal(result.begin(), result.end(), test_data.begin(), test_data.begin() + 113));

        std::vector<byte> front(1);
        read(e, raw_address::byte_address(320), front.data(), front.size());
        REQUIRE(front[0] == 0); // Non-overlapping part is intact.
    }

    SECTION("overlapping copy (after, 2)") {
        write(e, raw_address::byte_address(320), test_data.data(), 113);
        copy(e, raw_address::byte_address(380), raw_address::byte_address(320), 113);

        std::vector<byte> result(113);
        read(e, raw_address::byte_address(380), result.data(), result.size());
        REQUIRE(std::equal(result.begin(), result.end(), test_data.begin(), test_data.begin() + 113));

        std::vector<byte> front(60);
        read(e, raw_address::byte_address(320), front.data(), front.size());
        REQUIRE(std::equal(front.begin(), front.end(), test_data.begin(), test_data.begin() + 60)); // Non-overlapping part is intact.
    }
}
