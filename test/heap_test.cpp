#include <catch.hpp>

#include <extpp/default_allocator.hpp>
#include <extpp/heap.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

static constexpr u32 block_size = 512;

TEST_CASE("heap of small objects", "[heap]") {
    test_file file(block_size);
    file.open();

    default_allocator::anchor alloc_anchor;
    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    struct expected_t {
        heap_reference ref;
        std::string expected;
    };

    {
        heap::anchor heap_anchor;
        heap h(make_anchor_handle(heap_anchor), alloc);

        std::vector<expected_t> refs;
        for (int i = 0; i < 1000; ++i) {
            CAPTURE(i);

            // Every string is 20 bytes
            std::string str = fmt::format("Test String {:6}!\n", i);

            heap_reference ref = h.allocate((const byte*) str.data(), str.size());

            if (!ref.valid())
                FAIL("Invalid reference.");

            if (h.size(ref) != str.size())
                FAIL("Unexpected size: " << h.size(ref) << " expected " << str.size());

            std::string data(str.size(), 0);
            h.load(ref, (byte*) &data[0], data.size());

            if (data != str)
                FAIL("Unexpected string: " << data << " expected " << str);

            refs.push_back({ref, std::move(str)});
        }
        h.validate();

        REQUIRE(h.objects_count() == 1000);
        REQUIRE(h.objects_size() == 20000);

        const u32 original_size = h.heap_size();
        REQUIRE(original_size >= 20000);

        {
            std::vector<expected_t> new_refs;
            for (int i = 0; i < 1000; ++i) {
                heap_reference ref = refs[i].ref;
                if (i % 5 == 0) {
                    new_refs.push_back(refs[i]);
                    continue;
                }

                h.free(ref);
            }
            refs = std::move(new_refs);
        }
        h.validate();

        REQUIRE(h.objects_count() == 200);
        REQUIRE(h.objects_size() == 4000);

        {
            for (int i = 0; i < 800; ++i) {
                // Every string is 20 bytes
                std::string str = fmt::format("Test String {:6}!\n", i + 1000);

                refs.push_back({
                    h.allocate((const byte*) str.data(), str.size()),
                    str
                });
            }
        }

        for (const auto& entry : refs) {
            u32 size = h.size(entry.ref);
            if (size != entry.expected.size())
                FAIL("Unexpected size: " << size << " expected " << entry.expected.size());

            std::string data(size, 0);
            h.load(entry.ref, (byte*) &data[0], size);

            if (data != entry.expected)
                FAIL("Unexpected string: " << data << " expected " << entry.expected);
        }

        // Must have reused the space freed previously
        REQUIRE(h.heap_size() == original_size);
        h.validate();
    }
}

TEST_CASE("heap supports large objects", "[heap]") {
    test_file file(block_size);
    file.open();

    default_allocator::anchor alloc_anchor;
    default_allocator alloc(make_anchor_handle(alloc_anchor), file.get_engine());

    struct expected_t {
        heap_reference ref;
        std::string expected;
    };

    {
        heap::anchor heap_anchor;
        heap h(make_anchor_handle(heap_anchor), alloc);

        std::vector<expected_t> refs;

        size_t total_size = 0;
        for (int i = 0; i < 100; ++i) {
            std::string str(block_size * 2 + (100 * i + i), 0);
            for (size_t j = 0; j < str.size(); ++j) {
                str[j] = (byte) j;
            }

            heap_reference ref = h.allocate((const byte*) str.data(), str.size());

            if (!ref.valid())
                FAIL("Invalid reference.");

            if (h.size(ref) != str.size())
                FAIL("Unexpected size: " << h.size(ref) << " expected " << str.size());

            std::string data(str.size(), 0);
            h.load(ref, (byte*) &data[0], data.size());

            if (data != str)
                FAIL("Unexpected string: " << data << " expected " << str);

            total_size += str.size();
            refs.push_back({ ref, std::move(str) });
        }
        h.validate();

        REQUIRE(h.objects_count() == 100);
        REQUIRE(h.objects_size() == total_size);
        REQUIRE(h.heap_size() >= total_size);

        for (int i = 0; i < 100; ++i) {
            if (i % 3 == 0)
                continue;

            auto& entry = refs[i];
            total_size -= entry.expected.size();
            h.free(entry.ref);
        }
        h.validate();

        REQUIRE(h.objects_count() == 34);
        REQUIRE(h.objects_size() == total_size);
        REQUIRE(h.heap_size() >= total_size);
    }
}
