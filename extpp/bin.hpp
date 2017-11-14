#ifndef EXTPP_BIN_HPP
#define EXTPP_BIN_HPP

#include <extpp/allocator.hpp>
#include <extpp/assert.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>
#include <extpp/identity_key.hpp>

namespace extpp {

template<u32 BlockSize>
class bin {
public:
    static constexpr u32 block_size = BlockSize;

    static constexpr u32 align = 4;

private:
    using raw_address_t = raw_address<BlockSize>;

private:
    class meta_block_header {

    };

    using extent_t = extent<BlockSize>;
    using extent_anchor_t = typename extent_t::anchor;

    struct chunk_entry {
        extent_anchor_t extent;

        /// The address of the first block in the extent.
        u64 key() const { return extent_t::data(c.extent).value(); }

        struct key_extract {
            u64 operator()(const chunk_entry& c) const { return c.key(); }
        };
    };

    struct free_space_entry {
        u64 free_bytes = 0;     ///< Number of free bytes in the chunk.
        u64 chunk_id;           ///< Points to the chunk in the chunk tree.

        bool operator<(const free_space_entry& other) const {
            if (free_bytes != other.free_bytes)
                return free_bytes < other.free_bytes;
            return chunk_id < other.chunk_id;
        }
    };

    using chunk_tree = btree<chunk_entry, chunk_entry::key_extract, std::less<>, BlockSize>;
    using chunk_iterator = typename chunk_tree::iterator;

    using free_space_tree = btree<free_space_entry, identity_key, std::less<>, BlockSize>;
    using free_space_iterator = typename free_space_tree::iterator;

public:
    class anchor {};

public:
    bin(handle<anchor> h, u32 chunk_size, extpp::engine<BlockSize>& e, extpp::allocator<BlockSize>& a)
        : m_anchor(std::move(h))
        , m_engine(&e)
        , m_alloc(&a)
        , m_chunk_size(chunk_size)
    {}

    bin(const bin&) = delete;
    bin(bin&&) noexcept = default;

    bin& operator=(const bin&) = delete;
    bin& operator=(bin&&) noexcept = default;

    raw_address<BlockSize> insert(const byte* data, u32 size) {
    }

    void load(raw_address<BlockSize> addr, std::vector<byte>& output) {

    }

private:
    raw_address_t allocate(u32 bytes) const {

    }

    free_space_iterator find_free(u32 bytes) const {
        free_space_entry e;
        e.free_bytes = bytes;
        e.chunk_id = raw_address(0);
        return m_free_space.lower_bound(e);
    }

private:
    handle<anchor, BlockSize> m_anchor;
    extpp::engine<BlockSize>* m_engine;
    extpp::allocator<BlockSize>* m_alloc;

    /// Number of blocks that are allocated at once.
    u32 m_chunk_size;

    /// Contains references to chunks, indexed by their starting address.
    chunk_tree m_chunks;

    /// Contains references to chunks, indexed by the free space left in them.
    free_space_tree m_free_space;
};

} // namespace extpp

#endif // EXTPP_BIN_HPP
