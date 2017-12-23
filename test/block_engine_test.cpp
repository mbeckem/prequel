#include <catch.hpp>

#include <extpp/io.hpp>
#include <extpp/engine.hpp>

namespace extpp {
namespace detail {

class block_test {
protected:
    std::unique_ptr<file> fd;
    block_engine engine;
    u64 next_address;
    std::vector<block*> allocated;

    block_test()
        : fd(memory_vfs().open("testfile.bin", vfs::read_write, vfs::open_create))
        , engine(*fd, 4096, 8)
        , next_address(0)
    {}

    ~block_test() {
        for (block* blk : allocated) {
            delete blk;
        }
    }

    block& new_block(int index) {
        block* blk = new block(&engine, engine.m_block_size);
        blk->m_index = index;
        blk->ref(); // Manual management.
        allocated.push_back(blk);
        return *blk;
    }

    u32 refcount(block& blk) const {
        return blk.m_refcount;
    }
};

} // namespace detail
} // namespace extpp

using namespace extpp;
using namespace extpp::detail;

template<typename Seq, typename T>
bool contains(const Seq& s, const T& t) {
    return std::find(s.begin(), s.end(), t) != s.end();
}

TEST_CASE_METHOD(block_test, "Block map", "[block engine]") {
    block_map map(4);
    REQUIRE(map.size() == 0);
    REQUIRE(map.begin() == map.end());

    block& blk1 = new_block(1);
    block& blk2 = new_block(2);
    block& blk3 = new_block(3);

    map.insert(blk1);
    REQUIRE(map.contains(blk1));
    REQUIRE(map.size() == 1);

    map.insert(blk2);
    REQUIRE(map.contains(blk2));
    REQUIRE(map.size() == 2);

    map.insert(blk3);
    REQUIRE(map.contains(blk3));
    REQUIRE(map.size() == 3);

    REQUIRE(map.find(1) == &blk1);
    REQUIRE(map.find(2) == &blk2);
    REQUIRE(map.find(3) == &blk3);

    std::vector<block*> all;
    for (block& blk : map) {
        all.push_back(&blk);
    }
    REQUIRE(all.size() == 3);
    REQUIRE(contains(all, &blk1));
    REQUIRE(contains(all, &blk2));
    REQUIRE(contains(all, &blk3));

    map.remove(blk2);
    REQUIRE(!map.contains(blk2));

    map.remove(blk3);
    REQUIRE(!map.contains(blk3));

    map.remove(blk1);
    REQUIRE(!map.contains(blk1));

    REQUIRE(map.size() == 0);
    REQUIRE(map.begin() == map.end());
}

TEST_CASE_METHOD(block_test, "Block cache", "[block engine]") {
    block_cache cache(3);

    std::vector<block*> blocks{
        &new_block(0), &new_block(1),
        &new_block(2), &new_block(3)
    };

    REQUIRE(cache.max_size() == 3);
    REQUIRE(std::none_of(blocks.begin(), blocks.end(), [&](block* b) {
        return cache.contains(*b);
    }));


    cache.use(*blocks[0]);
    cache.use(*blocks[1]);
    cache.use(*blocks[2]);
    // 2 1 0

    REQUIRE(cache.contains(*blocks[0]));
    REQUIRE(cache.contains(*blocks[1]));
    REQUIRE(cache.contains(*blocks[2]));
    REQUIRE(refcount(*blocks[0]) == 2);
    REQUIRE(refcount(*blocks[1]) == 2);
    REQUIRE(refcount(*blocks[2]) == 2);
    REQUIRE(refcount(*blocks[3]) == 1);

    cache.use(*blocks[3]);
    // 3 2 1

    REQUIRE(!cache.contains(*blocks[0]));
    REQUIRE(cache.contains(*blocks[3]));
    REQUIRE(refcount(*blocks[0]) == 1);
    REQUIRE(refcount(*blocks[3]) == 2);

    cache.use(*blocks[1]);
    // 1 3 2

    cache.use(*blocks[0]);
    // 0 1 3
    REQUIRE(!cache.contains(*blocks[2]));
    REQUIRE(cache.contains(*blocks[0]));
    REQUIRE(cache.contains(*blocks[1]));
    REQUIRE(cache.contains(*blocks[3]));

    REQUIRE(cache.size() == 3);
    cache.clear();
}

TEST_CASE_METHOD(block_test, "Block dirty set", "[block-engine]") {
    block_dirty_set set;

    std::vector<block*> blocks{
        &new_block(0), &new_block(1), &new_block(2)
    };

    REQUIRE(!set.contains(*blocks[0]));
    REQUIRE(!set.contains(*blocks[1]));
    REQUIRE(!set.contains(*blocks[2]));

    set.add(*blocks[1]);
    REQUIRE(set.contains(*blocks[1]));

    set.add(*blocks[2]);
    REQUIRE(set.contains(*blocks[2]));

    {
        std::vector<block*> expected{blocks[1], blocks[2]};
        REQUIRE(std::equal(expected.begin(), expected.end(), set.begin(), set.end(),
                [](auto* blockptr, auto& block) {
                    return &block == blockptr;
                }
        ));
    }

    set.remove(*blocks[2]);
    REQUIRE(!set.contains(*blocks[2]));

    set.clear();
    REQUIRE(set.begin() == set.end());
    REQUIRE(!set.contains(*blocks[0]));
    REQUIRE(!set.contains(*blocks[1]));
    REQUIRE(!set.contains(*blocks[2]));
}
