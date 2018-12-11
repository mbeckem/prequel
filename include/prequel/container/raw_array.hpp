#ifndef PREQUEL_CONTAINER_RAW_ARRAY_HPP
#define PREQUEL_CONTAINER_RAW_ARRAY_HPP

#include <prequel/anchor_handle.hpp>
#include <prequel/binary_format.hpp>
#include <prequel/block_index.hpp>
#include <prequel/container/allocator.hpp>
#include <prequel/defs.hpp>
#include <prequel/container/extent.hpp>
#include <prequel/handle.hpp>

#include <memory>
#include <variant>

namespace prequel {

class raw_array;

namespace detail {

class raw_array_impl;

struct raw_array_anchor {
    /// Raw block storage.
    extent::anchor storage;

    /// Number of elements
    u64 size = 0;

    static constexpr auto get_binary_format() {
        return binary_format(&raw_array_anchor::storage, &raw_array_anchor::size);
    }
};

} // namespace detail

using raw_array_anchor = detail::raw_array_anchor;

/// The stream allocates new blocks in chunks of the given size.
struct linear_growth {
    linear_growth(u64 chunk_size = 1)
        : m_chunk_size(chunk_size) {
        PREQUEL_ASSERT(chunk_size >= 1, "Invalid chunk size.");
    }

    u64 chunk_size() const { return m_chunk_size; }

private:
    u64 m_chunk_size;
};

/// The stream is resized exponentially (to 2^n blocks).
struct exponential_growth {};

/// Specify the growth strategy of a stream.
using growth_strategy = std::variant<linear_growth, exponential_growth>;

/**
 * A dynamic array for fixed-size values.
 * The size of values can be determined at runtime (e.g. through user input)
 * but must remain constant during the use of an array.
 *
 * A array stores a sequence of fixed-size values in contiguous storage on disk.
 * The stream can reserve capacity ahead of time to prepare for future insertions,
 * very similar to `std::vector<T>`.
 */
class raw_array {
public:
    using anchor = raw_array_anchor;

public:
    /**
     * Accesses a raw array rooted at the given anchor.
     * `value_size` and `alloc` must be equivalent every time the raw array is loaded.
     */
    explicit raw_array(anchor_handle<anchor> _anchor, u32 value_size, allocator& alloc);
    ~raw_array();

    raw_array(const raw_array&) = delete;
    raw_array& operator=(const raw_array&) = delete;

    raw_array(raw_array&&) noexcept;
    raw_array& operator=(raw_array&&) noexcept;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    /**
     * Returns the size of a serialized value on disk.
     */
    u32 value_size() const;

    /**
     * Returns the number of serialized values that fit into a single block on disk.
     */
    u32 block_capacity() const;

    /**
     * Returns true iff the stream is empty, i.e. contains zero values.
     */
    bool empty() const;

    /**
     * Returns the number of values in this stream.
     */
    u64 size() const;

    /**
     * Returns the capacity of this stream, i.e. the maximum number of values
     * that can currently be stored without reallocating the storage on disk.
     * The stream will allocate storage according to its growth strategy when
     * an element is inserted into a full stream.
     *
     * @note `capacity() * value_size() == byte_size()` will always be true.
     */
    u64 capacity() const;

    /**
     * Returns the number of disk blocks currently allocated by the stream.
     */
    u64 blocks() const;

    /**
     * Returns the relative fill factor, i.e. the size divided by the capacity.
     */
    double fill_factor() const;

    /**
     * Returns the total size of this datastructure on disk, in bytes.
     */
    u64 byte_size() const;

    /**
     * Returns the relative overhead of this datastructure compared to a linear file, i.e.
     * the allocated storage (see capacity()) dividied by the used storage (see size()).
     */
    double overhead() const;

    /**
     * Retrieves the element at the given index and writes it into the `value` buffer,
     * which must be at least `value_size()` bytes large.
     */
    void get(u64 index, byte* value) const;

    /**
     * Sets the value at the given index to the content of `value`, which must
     * have at least `value_size()` readable bytes.
     */
    void set(u64 index, const byte* value);

    /**
     * Frees all storage allocated by the stream.
     *
     * @post `size() == 0 && byte_size() == 0`.
     */
    void reset();

    /**
     * Removes all objects from this stream, but does not
     * free the underlying storage.
     *
     * @post `size() == 0`.
     */
    void clear();

    /**
     * Resizes the stream to the size `n`. New elements are constructed by
     * initializing them with `value`, which must be at least `value_size()` bytes long.
     * @post `size() == n`.
     */
    void resize(u64 n, const byte* value);

    /**
     * Resize the underlying storage so that the stream can store at least `n` values
     * without further resize operations. Uses the current growth strategy to computed the
     * storage that needs to be allocated.
     *
     * @post `capacity() >= n`.
     */
    void reserve(u64 n);

    /**
     * Resize the underlying storage so that the stream can store at least `n` *additional* values
     * without further resize operations. Uses the current growth strategy to computed the
     * storage that needs to be allocated.
     *
     * @post `capacity() >= size() + n`
     */
    void reserve_additional(u64 n);

    /**
     * Reduces the storage space used by the array by releasing unused capacity.
     *
     * It uses the current growth strategy to determine the needed number of blocks
     * and shrinks to that value.
     */
    void shrink();

    /**
     * Reduces the storage space used by the array by releasing all unused capacity.
     *
     * Releases *all* unused blocks to reduce the storage space to the absolute minimum.
     * Ignores the growth strategy.
     */
    void shrink_to_fit();

    /**
     * Inserts a new value at the end of the stream.
     * Allocates new storage in accordance with the current growth strategy
     * if there is no free capacity remaining.
     */
    void push_back(const byte* value);

    /**
     * Removes the last value from this stream.
     *
     * @throws bad_operation If the stream is empty.
     */
    void pop_back();

    /**
     * Changes the current growth strategy. Streams support linear and exponential growth.
     */
    void growth(const growth_strategy& g);

    /**
     * Returns the current growth strategy.
     */
    growth_strategy growth() const;

private:
    detail::raw_array_impl& impl() const;

private:
    std::unique_ptr<detail::raw_array_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_RAW_ARRAY_HPP
