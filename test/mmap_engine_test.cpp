#include <catch.hpp>

#include <prequel/mmap_engine.hpp>

using namespace prequel;

static constexpr u32 block_size = 512;

TEST_CASE("mmap engine test", "[mmap-engine]") {
    // TODO: Not implemented on windows..

    auto file = system_vfs().create_temp();

    mmap_engine engine(*file, block_size);
    REQUIRE(engine.size() == 0);

    const int blocks = 1024;
    engine.grow(blocks);
    REQUIRE(engine.size() == blocks);

    for (int i = 0; i < blocks; ++i) {
        CAPTURE(i);

        // Blocks really should not be in ram without derefencing them.
        if (engine.access(block_index(i))) {
            FAIL("Block loaded?");
        }
    }

    std::vector<byte> content(block_size);
    for (int i = 0; i < (int) block_size; ++i) {
        content[i] = (byte) i;
    }

    for (int i = 0; i < blocks; ++i) {
        engine.overwrite(block_index(i), content.data(), content.size());
    }

    for (int i = 0; i < blocks; ++i) {
        CAPTURE(i);

        block_handle handle = engine.access(block_index(i));
        if (!handle) {
            FAIL("Block not in ram.");
        }

        if (!std::equal(content.begin(), content.end(), handle.data(), handle.data() + block_size)) {
            FAIL("Content corrupted.");
        }
    }

    for (int i = 0; i < blocks / 2; ++i) {
        engine.overwrite_zero(block_index(i));
    }

    for (int i = 0; i < blocks; ++i) {
        block_handle handle = engine.read(block_index(i));

        const byte* data = handle.data();
        if (i < blocks / 2) {
            for (u32 i = 0; i < block_size; ++i) {
                if (data[i] != 0)
                    FAIL("Not zero.");
            }
        } else {
            if (!std::equal(content.begin(), content.end(), data, data + block_size)) {
                FAIL("Content corrupted.");
            }
        }
    }
}
