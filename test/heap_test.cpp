#include <catch.hpp>

#include <extpp/heap.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 block_size = 4096;
using heap_t = heap<block_size>;
using file_t = test_file<heap_t::anchor, block_size>;

const type_index blob(1);

void register_types(heap_t& heap) {
    type_info blob_type;
    blob_type.index = blob;
    blob_type.contains_references = false;
    blob_type.dynamic_size = true;
    heap.register_type(blob_type);
}

TEST_CASE("heap", "[heap]") {
    file_t file;
    file.open();
    {
        heap_t heap(file.anchor(), file.engine(), file.alloc());
        register_types(heap);
        heap.chunk_size(128);

        std::vector<reference> refs;
        for (int i = 0; i < 1000; ++i) {
            std::string str = fmt::format("Hello World {}!!!1", i);
            refs.push_back(heap.insert(blob, str.data(), str.size()));
        }

        {
            std::vector<reference> new_refs;
            for (int i = 0; i < 1000; i += 5)
                new_refs.push_back(refs[i]);
            refs = new_refs;
        }

        auto collector = heap.begin_compaction();
        for (auto ref : refs)
            collector.visit(ref);
        collector();

        //heap.debug_print(std::cout);
        std::cout << "\n";
    }
    // file.alloc().debug_print(std::cout);
    file.close();
}
