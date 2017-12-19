#ifndef EXTPP_ALLOCATOR_HPP
#define EXTPP_ALLOCATOR_HPP

#include <extpp/address.hpp>
#include <extpp/defs.hpp>

namespace extpp {

template<u32 BlockSize>
class allocator {
public:
    allocator() = default;
    virtual ~allocator() = default;

    /// Allocates a range of `n` consecutive blocks.
    /// Returns the address of the first block.
    raw_address<BlockSize> allocate(u64 n) {
        EXTPP_CHECK(n > 0, "Cannot allocate 0 blocks.");
        auto result = do_allocate(n);
        EXTPP_ASSERT(result, "do_allocate() returned an invalid address. "
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
    ///     Can be the invalid address, in which case the call is equivalent to `allocate(n)`.
    /// \param n
    ///     The new size of the allocation, in blocks. If `n` is zero,
    ///     then `a` must be valid and the call is equivalent to `free(a)`.
    /// \return
    ///     Returns the address of the new allocation, unless `n` was 0, in which
    ///     case the invalid address is returned.
    raw_address<BlockSize> reallocate(raw_address<BlockSize> a, u64 n) {
        if (!a) {
            return allocate(n);
        }
        EXTPP_CHECK(a, "The address passed to reallocate() is invalid.");
        EXTPP_CHECK(a.get_offset_in_block() == 0, "The address passed to reallocate() does not point to a block.");
        if (n == 0) {
            free(a);
            return {};
        }

        auto result = do_reallocate(a, n);
        EXTPP_ASSERT(result, "do_reallocate() returned an invalid address. "
                             "Throw an exception instead.");
        return result;
    }

    /// Frees blocks previously allocated using `allocate()` or `reallocate()`.
    void free(raw_address<BlockSize> a) {
        EXTPP_CHECK(a, "The address passed to free() is invalid.");
        EXTPP_CHECK(a.get_offset_in_block() == 0, "The address passed to free() does not point to a block.");
        do_free(a);
    }

    allocator(const allocator&) = delete;
    allocator& operator=(const allocator&) = delete;

protected:
    /// Implements the allocation function. `n` is not zero.
    virtual raw_address<BlockSize> do_allocate(u64 n) = 0;

    /// Implements the reallocation function. `a` is valid and `n` is not zero.
    virtual raw_address<BlockSize> do_reallocate(raw_address<BlockSize> a, u64 n) = 0 ;

    /// Implements the free function.
    virtual void do_free(raw_address<BlockSize> a) = 0;
};

} // namespace extpp

#endif // EXTPP_ALLOCATOR_HPP
