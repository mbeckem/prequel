#include <catch.hpp>

#include "engine/journal.hpp"
#include "engine/journal.ipp"

#include <prequel/container/btree.hpp>
#include <prequel/container/default_allocator.hpp>
#include <prequel/formatting.hpp>
#include <prequel/transaction_engine.hpp>
#include <prequel/vfs.hpp>

#include <fmt/format.h>

using namespace prequel;
using detail::engine_impl::journal;

static void dump_file(file& fd) {
    static constexpr u32 buffer_size = 32;
    byte buffer[buffer_size];

    u64 offset = 0;
    u64 remaining = fd.file_size();
    while (remaining > 0) {
        u32 chunk = buffer_size;
        if (chunk > remaining)
            chunk = remaining;

        fd.read(offset, buffer, chunk);

        fmt::print("{:5} - {}\n", offset, format_hex(buffer, chunk));

        offset += chunk;
        remaining -= chunk;
    }
}

static std::vector<byte> test_block(u32 block_size, byte unique) {
    std::vector<byte> data(block_size);
    data[block_size / 2] = unique;
    return data;
}

TEST_CASE("journal functionality", "[transaction-engine]") {
    static constexpr u32 block_size = 256;

    auto logfd = memory_vfs().open("log-temp", vfs::read_write, vfs::open_create);
    journal jn(*logfd, block_size, 1 << 16);

    std::vector<byte> block(block_size);
    const std::vector<byte> block0 = test_block(block_size, 11);
    const std::vector<byte> block1 = test_block(block_size, 21);
    const std::vector<byte> block2 = test_block(block_size, 31);

    REQUIRE_FALSE(jn.read(block_index(1), block.data()));
    REQUIRE(!jn.database_size());

    SECTION("aborted transaction") {
        REQUIRE_FALSE(jn.in_transaction());
        jn.begin();
        REQUIRE(jn.in_transaction());

        // Normal write
        jn.write(block_index(1), block0.data());
        REQUIRE(jn.read(block_index(1), block.data()));
        REQUIRE(block == block0);

        // Overwrite
        jn.write(block_index(1), block1.data());
        REQUIRE(jn.read(block_index(1), block.data()));
        REQUIRE(block == block1);

        // Normal write
        jn.write(block_index(2), block2.data());
        REQUIRE(jn.read(block_index(2), block.data()));
        REQUIRE(block == block2);

        jn.abort();
        REQUIRE_FALSE(jn.in_transaction());

        // State was thrown away.
        REQUIRE_FALSE(jn.read(block_index(0), block.data()));
        REQUIRE_FALSE(jn.read(block_index(1), block.data()));
        REQUIRE_FALSE(jn.read(block_index(2), block.data()));
        REQUIRE(!jn.database_size());
    }

    SECTION("committed transaction") {
        REQUIRE_FALSE(jn.in_transaction());
        {
            jn.begin();
            REQUIRE(jn.in_transaction());

            // Normal write
            jn.write(block_index(1), block0.data());
            REQUIRE(jn.read(block_index(1), block.data()));
            REQUIRE(block == block0);

            jn.commit(2);
            REQUIRE_FALSE(jn.in_transaction());
            REQUIRE(jn.database_size() == 2);
        }

        // Read the value from the last transaction.
        REQUIRE(jn.read(block_index(1), block.data()));
        REQUIRE(block == block0);

        {
            jn.begin();
            jn.write(block_index(1), block1.data());

            REQUIRE(jn.read(block_index(1), block.data()));
            REQUIRE(block == block1);

            jn.abort();
        }

        // Observe the old value since the transaction was aborted.
        REQUIRE(jn.read(block_index(1), block.data()));
        REQUIRE(block == block0);
        REQUIRE(jn.database_size() == 2);
    }

    unused(dump_file);
}

TEST_CASE("journal checkpoint", "[transaction-engine]") {
    static constexpr u32 block_size = 256;

    auto logfd = memory_vfs().open("test.journal", vfs::read_write, vfs::open_create);
    auto dbfd = memory_vfs().open("test.db", vfs::read_write, vfs::open_create);
    journal jn(*logfd, block_size, 1 << 16);

    std::vector<byte> block(block_size);
    const std::vector<byte> block0 = test_block(block_size, 11);
    const std::vector<byte> block1 = test_block(block_size, 21);
    const std::vector<byte> block2 = test_block(block_size, 31);

    SECTION("no changes") {
        REQUIRE(!jn.database_size());
        REQUIRE_FALSE(jn.has_committed_changes());

        bool changes = jn.checkpoint(*dbfd);

        REQUIRE_FALSE(changes);
        REQUIRE(dbfd->file_size() == 0);
    }

    SECTION("aborted / in progress transaction -> no changes") {
        jn.begin();
        jn.write(block_index(0), block0.data());
        REQUIRE_FALSE(jn.has_committed_changes());

        jn.abort();
        REQUIRE_FALSE(jn.has_committed_changes());

        bool changes = jn.checkpoint(*dbfd);
        REQUIRE_FALSE(changes);
        REQUIRE(logfd->file_size() == jn.log_header_size());
        REQUIRE(!jn.database_size());
    }

    SECTION("committed transactions alter db") {
        jn.begin();
        jn.write(block_index(0), block0.data());
        jn.commit(1);
        REQUIRE(jn.has_committed_changes());
        REQUIRE(jn.database_size() == 1);

        jn.begin();
        jn.write(block_index(1), block1.data());
        jn.write(block_index(0), block2.data());
        jn.write(block_index(55), block0.data());
        jn.commit(99);
        REQUIRE(jn.has_committed_changes());
        REQUIRE(jn.database_size() == 99);

        bool changes = jn.checkpoint(*dbfd);
        REQUIRE(changes);

        // Log is truncated on checkpoint:
        REQUIRE_FALSE(jn.has_committed_changes());
        REQUIRE(!jn.database_size());

        // Verify file content
        REQUIRE(dbfd->file_size() == block_size * 99);

        dbfd->read(block_size * 0, block.data(), block.size());
        REQUIRE(block == block2);

        dbfd->read(block_size * 1, block.data(), block.size());
        REQUIRE(block == block1);

        dbfd->read(block_size * 55, block.data(), block.size());
        REQUIRE(block == block0);
    }

    SECTION("can continue after checkpoint") {
        jn.begin();
        jn.write(block_index(0), block0.data());
        jn.commit(1);
        REQUIRE(jn.checkpoint(*dbfd));

        jn.begin();
        jn.write(block_index(1), block1.data());
        jn.write(block_index(2), block2.data());
        jn.commit(2); // this cuts off block index 2

        jn.begin();
        jn.write(block_index(0), block2.data());
        jn.abort();

        REQUIRE(jn.database_size() == 2);
        REQUIRE(jn.has_committed_changes());

        bool changes = jn.checkpoint(*dbfd);
        REQUIRE(changes);

        REQUIRE(dbfd->file_size() == 2 * block_size);

        dbfd->read(block_size * 0, block.data(), block.size());
        REQUIRE(block == block0);

        dbfd->read(block_size * 1, block.data(), block.size());
        REQUIRE(block == block1);
    }
}

TEST_CASE("journal restored", "[transaction-engine]") {
    static constexpr u32 block_size = 256;

    auto logfd = memory_vfs().open("test.journal", vfs::read_write, vfs::open_create);
    auto dbfd = memory_vfs().open("test.db", vfs::read_write, vfs::open_create);

    std::vector<byte> block(block_size);
    const std::vector<byte> block0 = test_block(block_size, 11);
    const std::vector<byte> block1 = test_block(block_size, 21);
    const std::vector<byte> block2 = test_block(block_size, 31);
    const std::vector<byte> block3 = test_block(block_size, 41);

    SECTION("empty log restored") {
        { journal jn(*logfd, block_size, 1 << 16); }

        {
            journal jn(*logfd, block_size, 1 << 16);
            REQUIRE(!jn.has_committed_changes());
            REQUIRE(!jn.database_size());
        }
        REQUIRE(logfd->file_size() == journal::log_header_size());
    }

    SECTION("log with changes restored") {

        {
            journal jn(*logfd, block_size, 1 << 16);
            jn.begin();
            jn.write(block_index(66), block0.data());
            jn.commit(77);

            jn.begin();
            jn.write(block_index(33), block1.data());
            jn.commit(67);

            jn.begin();
            jn.write(block_index(77), block2.data());
            jn.commit(78);

            jn.begin();
            jn.write(block_index(66), block3.data());
            jn.abort();

            jn.begin();
            jn.commit(67);
        }

        REQUIRE(logfd->file_size() > journal::log_header_size());

        {
            journal jn(*logfd, block_size, 1 << 16);

            REQUIRE(jn.log_size() == logfd->file_size());

            REQUIRE(jn.has_committed_changes());
            REQUIRE(jn.database_size() == 67);
            REQUIRE(jn.database_block_size() == block_size);

            REQUIRE(jn.read(block_index(66), block.data()));
            REQUIRE(block == block0);

            REQUIRE(jn.read(block_index(33), block.data()));
            REQUIRE(block == block1);

            REQUIRE_FALSE(jn.read(block_index(77), block.data()));
        }

        {
            journal jn(*logfd, block_size, 1 << 16);

            jn.begin();
            jn.write(block_index(7), block2.data());
            jn.write(block_index(66), block3.data());
            jn.commit(*jn.database_size());
        }

        {
            journal jn(*logfd, block_size, 1 << 16);
            jn.checkpoint(*dbfd);
        }

        REQUIRE(dbfd->file_size() == block_size * 67);

        dbfd->read(block_size * 66, block.data(), block.size());
        REQUIRE(block == block3);

        dbfd->read(block_size * 33, block.data(), block.size());
        REQUIRE(block == block1);

        dbfd->read(block_size * 7, block.data(), block.size());
        REQUIRE(block == block2);
    }

    SECTION("large transaction after crash is reverted") {
        {
            journal jn(*logfd, block_size, 4 * block_size);

            jn.begin();
            for (u32 i = 0; i < 12; ++i) {
                jn.write(block_index(i + 1), block0.data());
            }
        }

        // Some changes were written to disk
        REQUIRE(logfd->file_size() > journal::log_header_size());

        {
            journal jn(*logfd, block_size, 4 * block_size);

            for (u32 i = 0; i < 12; ++i) {
                block_index index(i + 1);
                CAPTURE(index);

                if (jn.read(index, block.data()))
                    FAIL("Transaction was not rolled back.");
            }

            REQUIRE(jn.log_size() == journal::log_header_size());
        }

        REQUIRE(logfd->file_size() == journal::log_header_size());
    }
}

TEST_CASE("high level engine", "[transaction-engine]") {
    static constexpr u32 block_size = 4096;

    auto dbfd = memory_vfs().open("test.db", vfs::read_write, vfs::open_create);
    auto logfd = memory_vfs().open("test.db-journal", vfs::read_write, vfs::open_create);

    struct anchor_type {
        default_allocator::anchor alloc;
        btree<i32>::anchor tree;

        static constexpr auto get_binary_format() {
            return binary_format(&anchor_type::alloc, &anchor_type::tree);
        }
    };

    {
        transaction_engine engine(*dbfd, *logfd, block_size, 1024);
        REQUIRE(&engine.database_fd() == dbfd.get());
        REQUIRE(&engine.journal_fd() == logfd.get());

        {
            REQUIRE_THROWS_AS(engine.commit(), bad_operation);
            REQUIRE_THROWS_AS(engine.rollback(), bad_operation);

            REQUIRE_NOTHROW(engine.begin());
            REQUIRE_NOTHROW(engine.rollback());

            REQUIRE_NOTHROW(engine.begin());
            REQUIRE_NOTHROW(engine.commit());

            REQUIRE(!engine.journal_has_changes());
            REQUIRE_NOTHROW(engine.checkpoint());

            REQUIRE_NOTHROW(engine.begin());
            REQUIRE_THROWS_AS(engine.checkpoint(), bad_operation);
            REQUIRE_NOTHROW(engine.rollback());

            REQUIRE(engine.size() == 0);
            REQUIRE(dbfd->file_size() == 0);
        }

        // Normal committed transaction
        engine.begin();
        REQUIRE(engine.in_transaction());
        {
            // Reserve block 0.
            engine.grow(1);

            anchor_type anchor;
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            tree.insert(1);
            tree.insert(7);

            // Add needless I/O so the block has to be written multiple times.
            engine.flush();

            tree.insert(55);
            tree.insert(-1);

            auto buffer = serialize_to_buffer(anchor);
            engine.overwrite(block_index(0), buffer.data(), buffer.size());
        }
        engine.commit();
        REQUIRE(engine.journal_has_changes());
        REQUIRE(!engine.in_transaction());
        const u64 journal_size_after_init = engine.journal_size();

        // Read only transaction
        engine.begin();
        REQUIRE(engine.in_transaction());
        {
            anchor_type anchor = engine.read(block_index(0)).get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            auto cursor = tree.create_cursor();
            REQUIRE(cursor.find(-1));
            REQUIRE(cursor.find(1));
            REQUIRE(cursor.find(7));
            REQUIRE(cursor.find(55));
            REQUIRE(tree.size() == 4);
        }
        engine.commit();
        REQUIRE(!engine.in_transaction());

        // Read only transactions dont produce log output
        REQUIRE(engine.journal_size() == journal_size_after_init);

        // Rolled back transaction
        engine.begin();
        {
            auto main_block = engine.read(block_index(0));

            anchor_type anchor = main_block.get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            auto cursor = tree.create_cursor();
            for (i32 i = 10000; i < 20000; ++i)
                cursor.insert(i);

            REQUIRE(tree.size() == 10004);
            main_block.set<anchor_type>(0, anchor);

            // This flush produces output to the log and makes sure that not only
            // the dirty blocks in memory are discarded but also those that have been
            // written out to disk, so they were considered "clean" in memory.
            engine.flush();

            // main block still referenced!
            REQUIRE_THROWS_AS(engine.rollback(), bad_operation);
        }
        engine.rollback();

        // Read only transaction to confirm rollback
        engine.begin();
        {
            auto main_block = engine.read(block_index(0));

            anchor_type anchor = main_block.get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            auto cursor = tree.create_cursor();
            REQUIRE(cursor.find(-1));
            REQUIRE(cursor.find(1));
            REQUIRE(cursor.find(7));
            REQUIRE(cursor.find(55));
            REQUIRE(tree.size() == 4);
        }
        engine.commit();

        // Incomplete transaction interrupted by crash (engine destructors dont flush)
        engine.begin();
        {
            auto main_block = engine.read(block_index(0));

            anchor_type anchor = main_block.get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            tree.insert(1337);
        }
    }

    // Reopen log and database file to confirm the committed state (and that the incomplete
    // state was not committed).
    {
        transaction_engine engine(*dbfd, *logfd, block_size, 1024);

        REQUIRE(engine.journal_has_changes());

        engine.begin();
        {
            auto main_block = engine.read(block_index(0));

            anchor_type anchor = main_block.get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            auto cursor = tree.create_cursor();
            REQUIRE(cursor.find(-1));
            REQUIRE(cursor.find(1));
            REQUIRE(cursor.find(7));
            REQUIRE(cursor.find(55));
            REQUIRE_FALSE(cursor.find(1337));
            REQUIRE(tree.size() == 4);
        }
        engine.commit();

        engine.checkpoint();
        REQUIRE(!engine.journal_has_changes());

        engine.begin();
        {
            auto main_block = engine.read(block_index(0));

            anchor_type anchor = main_block.get<anchor_type>(0);
            default_allocator alloc{anchor.alloc, engine};
            btree<i32> tree{anchor.tree, alloc};

            auto cursor = tree.create_cursor();
            REQUIRE(cursor.find(-1));
            REQUIRE(cursor.find(1));
            REQUIRE(cursor.find(7));
            REQUIRE(cursor.find(55));
            REQUIRE_FALSE(cursor.find(1337));
            REQUIRE(tree.size() == 4);
        }
        engine.commit();
    }
}
