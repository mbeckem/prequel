#ifndef EXTPP_RAW_STREAM_HPP
#define EXTPP_RAW_STREAM_HPP

#include <extpp/allocator.hpp>
#include <extpp/anchor_handle.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>

#include <memory>
#include <variant>

namespace extpp {

class raw_stream;

namespace detail {

class raw_stream_impl;

struct raw_stream_anchor {
    /// Raw block storage.
    extent::anchor storage;

    /// Number of elements
    u64 size = 0;

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_stream_anchor::storage, &raw_stream_anchor::size);
    }
};

} // namespace detail

using raw_stream_anchor = detail::raw_stream_anchor;

/// The stream allocates new blocks in chunks of the given size.
struct linear_growth {
    linear_growth(u64 chunk_size = 1)
        : m_chunk_size(chunk_size)
    {
        EXTPP_ASSERT(chunk_size >= 1, "Invalid chunk size.");
    }

    u64 chunk_size() const { return m_chunk_size; }

private:
    u64 m_chunk_size;
};

/// The stream is resized exponentially (to 2^n blocks).
struct exponential_growth {};

/// Specify the growth strategy of a stream.
using growth_strategy = std::variant<linear_growth, exponential_growth>;

class raw_stream {
public:
    using anchor = raw_stream_anchor;

public:
    explicit raw_stream(anchor_handle<anchor> _anchor, u32 value_size, allocator& alloc);
    ~raw_stream();

    raw_stream(const raw_stream&) = delete;
    raw_stream& operator=(const raw_stream&) = delete;

    raw_stream(raw_stream&&) noexcept;
    raw_stream& operator=(raw_stream&&) noexcept;

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
     * necessarily free the underlying storage.
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
    detail::raw_stream_impl& impl() const;

private:
    std::unique_ptr<detail::raw_stream_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_RAW_STREAM_HPP
