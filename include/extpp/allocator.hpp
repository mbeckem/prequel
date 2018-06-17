#ifndef EXTPP_ALLOCATOR_HPP
#define EXTPP_ALLOCATOR_HPP

#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>

namespace extpp {

/// Allocates ranges of blocks from a file.
class allocator {
public:
    explicit allocator(engine& e)
        : m_engine(&e)
    {}

    virtual ~allocator() = default;

    /// Returns the engine in which the addresses allocated with this
    /// instance can be used.
    engine& get_engine() const { return *m_engine; }

    /// The size of the blocks allocated by this allocator.
    u32 block_size() const { return get_engine().block_size(); }

    /// Allocates a range of `n` consecutive blocks.
    /// Returns the address of the first block.
    block_index allocate(u64 n) {
        EXTPP_CHECK(n > 0, "Cannot allocate 0 blocks.");
        auto result = do_allocate(n);
        EXTPP_ASSERT(result, "do_allocate() returned an invalid block index. "
                             "Throw an exception instead.");
        return result;
    }

    /// Changes the size of the allocated range pointed to by `a` to `n` blocks.
    /// In order to achieve that, it might be necessary to move the data to a new location.
    /// The present data will remain unchanged in the range from the start
    /// to the minimum of the old and new sizes. New data will *not* be initialized.
    /// If `reallocate` moves the data to a new location, the old location will be freed.
    ///
    /// \param a
    ///     Points to a range of blocks obtained via `allocate()` or `reallocate()`.
    ///     Can be the invalid block_index, in which case the call is equivalent to `allocate(n)`.
    /// \param n
    ///     The new size of the allocation, in blocks. If `n` is zero,
    ///     then `a` must be valid and the call is equivalent to `free(a)`.
    /// \return
    ///     Returns the block index of the new allocation, unless `n` was 0, in which
    ///     case the invalid address is returned.
    block_index reallocate(block_index a, u64 n) {
        if (!a) {
            return allocate(n);
        }
        EXTPP_CHECK(a, "The block index passed to reallocate() is invalid.");
        if (n == 0) {
            free(a);
            return {};
        }

        auto result = do_reallocate(a, n);
        EXTPP_ASSERT(result, "do_reallocate() returned an invalid block index. "
                             "Throw an exception instead.");
        return result;
    }

    /// Frees blocks previously allocated using `allocate()` or `reallocate()`.
    void free(block_index a) {
        EXTPP_CHECK(a, "The address passed to free() is invalid.");
        do_free(a);
    }

    allocator(const allocator&) = delete;
    allocator& operator=(const allocator&) = delete;

protected:
    /// Implements the allocation function. `n` is not zero.
    virtual block_index do_allocate(u64 n) = 0;

    /// Implements the reallocation function. `a` is valid and `n` is not zero.
    virtual block_index do_reallocate(block_index a, u64 n) = 0 ;

    /// Implements the free function.
    virtual void do_free(block_index a) = 0;

private:
    engine* m_engine;
};

/// Utility base class for containers that keep a reference to an allocator
/// in order to allocate and free dynamic block storage.
class uses_allocator {
public:
    explicit uses_allocator(allocator& alloc)
        : m_allocator(&alloc)
        , m_engine(&alloc.get_engine())
    {}

    explicit uses_allocator(allocator& alloc, u32 required_blocksize)
        : m_allocator(&alloc)
        , m_engine(&alloc.get_engine())
    {
        EXTPP_CHECK(is_pow2(required_blocksize), "The required blocksize must be a power of 2.");
        EXTPP_CHECK(alloc.block_size() >= required_blocksize, "The allocator's blocksize is incompatible.");
    }

    allocator& get_allocator() const { return *m_allocator; }
    engine& get_engine() const { return *m_engine; }

private:
    allocator* m_allocator;
    engine* m_engine;
};

} // namespace extpp

#endif // EXTPP_ALLOCATOR_HPP