#include <catch.hpp>

#include <extpp/bin.hpp>

#include <iostream>

#include "./test_file.hpp"

using namespace extpp;

constexpr u32 block_size = 512;
using bin_t = bin<block_size>;
using file_t = test_file<bin_t::anchor, block_size>;

TEST_CASE("bin", "[bin]") {
    file_t file;
    file.open();
    {
        bin_t bin(file.anchor(), file.engine(), file.alloc());

        auto raw = [](const std::string& s) { return reinterpret_cast<const byte*>(s.data()); };

        std::vector<bin_t::reference> refs;

        std::string str1;
        str1.resize(72, 'a');
        for (size_t i = 0; i < 819; ++i)
            refs.push_back(bin.insert(raw(str1), str1.size()));

        auto c = bin.collect();
        for (size_t i = 0; i < refs.size(); i += 2)
            c.visit(refs[i]);

        c();

        refs.push_back(bin.insert(raw(str1), str1.size()));

        bin.debug_stats(std::cout);
    }
    //file.alloc().debug_stats(std::cout);
    file.close();
}

// TODO
