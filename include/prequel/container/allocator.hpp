#ifndef PREQUEL_CONTAINER_ALLOCATOR_HPP
#define PREQUEL_CONTAINER_ALLOCATOR_HPP

#include <prequel/block_index.hpp>
#include <prequel/defs.hpp>
#include <prequel/engine.hpp>

namespace prequel {

/// Allocates ranges of blocks from a file.
class allocator {
public:
    explicit allocator(engine& e)
        : m_engine(&e) {}

    virtual ~allocator() = default;

    /// Returns the engine in which the addresses allocated with this
    /// instance can be used.
    engine& get_engine() const { return *m_engine; }

    /// The size of the blocks allocated by this allocator.
    u32 block_size() const { return get_engine().block_size(); }

    /**
     * Allocates a range of `size` consecutive blocks.
     * Returns the address of the first block.
     */
    block_index allocate(u64 size) {
        if (size == 0)
            PREQUEL_THROW(bad_argument("Requested size cannot be zero."));
        auto result = do_allocate(size);
        PREQUEL_ASSERT(result,
                       "do_allocate() returned an invalid block index. "
                       "Throw an exception instead.");
        return result;
    }

    /**
     * Reallocates a range of `size` blocks, starting with `block`, to a new location of size `new_size`.
     * New storage allocated will not be initialized. The new size can be both smaller (in which
     * case excess storage at the end will be freed) and larger (in which case additional storage
     * will be allocated).
     *
     * If `block` is invalid, then `size` must be zero and reallocate is equivalent to `allocate(new_size)`.
     *
     * If `new_size` is zero, then reallocate is equivalent to `free(block, size)` and it will return
     * the invalid block index.
     *
     * Returns the new location.
     */
    block_index reallocate(block_index block, u64 size, u64 new_size) {
        if (!block) {
            if (size != 0) {
                PREQUEL_THROW(bad_argument("Size must be zero if the block is invalid."));
            }
            return allocate(new_size);
        }

        if (size == 0) {
            PREQUEL_THROW(bad_argument("Size of the existing chunk cannot be zero."));
        }

        if (size == new_size) {
            return block;
        }

        if (new_size == 0) {
            free(block, size);
            return {};
        }

        auto result = do_reallocate(block, size, new_size);
        PREQUEL_ASSERT(result,
                       "do_reallocate() returned an invalid block index. "
                       "Throw an exception instead.");
        return result;
    }

    /**
     * Frees blocks previously allocated using `allocate()` or `reallocate()`.
     * Note that partial frees of allocated block ranges are supported.
     */
    void free(block_index block, u64 size) {
        if (!block || size == 0) {
            return;
        }

        do_free(block, size);
    }

    allocator(const allocator&) = delete;
    allocator& operator=(const allocator&) = delete;

protected:
    /// Implements the allocation function. `size` is not zero.
    virtual block_index do_allocate(u64 size) = 0;

    /// Implements the reallocation function. `block` is valid and `size` and `new_size` are not zero.
    virtual block_index do_reallocate(block_index block, u64 size, u64 new_size) = 0;

    /// Implements the free function. `block` is valid and `size` is not zero.
    virtual void do_free(block_index block, u64 size) = 0;

private:
    engine* m_engine;
};

/// Utility base class for containers that keep a reference to an allocator
/// in order to allocate and free dynamic block storage.
class uses_allocator {
public:
    explicit uses_allocator(allocator& alloc)
        : m_allocator(&alloc)
        , m_engine(&alloc.get_engine()) {}

    explicit uses_allocator(allocator& alloc, u32 required_blocksize)
        : m_allocator(&alloc)
        , m_engine(&alloc.get_engine()) {
        PREQUEL_CHECK(is_pow2(required_blocksize), "The required blocksize must be a power of 2.");
        PREQUEL_CHECK(alloc.block_size() >= required_blocksize,
                      "The allocator's blocksize is incompatible.");
    }

    allocator& get_allocator() const { return *m_allocator; }
    engine& get_engine() const { return *m_engine; }

private:
    allocator* m_allocator;
    engine* m_engine;
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_ALLOCATOR_HPP
