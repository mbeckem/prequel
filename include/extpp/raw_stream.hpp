#ifndef EXTPP_RAW_STREAM_HPP
#define EXTPP_RAW_STREAM_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_index.hpp>
#include <extpp/defs.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>

#include <memory>
#include <variant>

namespace extpp {

class raw_stream;
class raw_stream_impl;

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

class raw_stream_anchor {
    /// Raw block storage.
    extent::anchor storage;

    /// Number of elements
    u64 size = 0;

    static constexpr auto get_binary_format() {
        return make_binary_format(&raw_stream_anchor::storage, &raw_stream_anchor::size);
    }

    friend class raw_stream_impl;
    friend class binary_format_access;
};

class raw_stream {
public:
    using anchor = raw_stream_anchor;

public:
    explicit raw_stream(handle<anchor> _anchor, u32 value_size, allocator& alloc);
    ~raw_stream();

    raw_stream(const raw_stream&) = delete;
    raw_stream& operator=(const raw_stream&) = delete;

    raw_stream(raw_stream&&) noexcept;
    raw_stream& operator=(raw_stream&&) noexcept;

public:
    engine& get_engine() const;
    allocator& get_allocator() const;

    /// Returns the size of a single value.
    u32 value_size() const;

    /// Returns the maximum number of values per block.
    u32 block_capacity() const;

    /// Returns true iff this stream is empty.
    bool empty() const;

    /// Returns the number of values in this stream.
    u64 size() const;

    /// Returns the current maximum number of values that would
    /// fit the stream. The stream is resized in according to its growth
    /// strategy when an element is inserted into a full stream.
    u64 capacity() const;

    /// Returns the number of blocks occupied by this stream.
    u64 blocks() const;

    /// Returns the fill factor of this stream, i.e. the ratio of it's size to it's capacity.
    double fill_factor() const;

    /// Returns the total number of bytes occupied on disk (not including the anchor).
    u64 byte_size() const;

    /// Returns the relative overhead of this stream, compared to storing `size()`
    /// values in a linear file.
    double overhead() const;

    /// Retrieves the element at the given index and writes it into the `value` buffer,
    /// which must be at least `value_size()` bytes large.
    void get(u64 index, byte* value) const;

    /// Sets the value at the given index to the content of `value`, which must
    /// have at least `value_size()` readable bytes.
    void set(u64 index, const byte* value);

    /// Removes all data from this stream. After this operation, the stream will
    /// not occupy any space on disk.
    /// \post `empty() && byte_size() == 0`.
    void reset();

    /// Removes all values from this stream.
    /// \post `empty()`.
    void clear();

    /// Resizes the stream to the size `n`. New elements are constructed by
    /// initializing them with `value`, which must be at least `value_size()` bytes long.
    /// \post `size() == n`.
    void resize(u64 n, const byte* value);

    /// Resize the underlying storage so that the stream can store at least `n` values
    /// without further resize operations.
    /// \post `capacity() >= n`.
    void reserve(u64 n);

    /// Inserts the new value at the end of the stream.
    void push_back(const byte* value);

    /// Removes the last value from the stream. The stream must not be empty.
    void pop_back();

    /// Set the growth strategy for this stream. When the underlying storage
    /// must be grown, it will be done according to this policy.
    /// \{
    void growth(const growth_strategy& g);
    growth_strategy growth() const;
    /// \}

private:
    raw_stream_impl& impl() const;

private:
    std::unique_ptr<raw_stream_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_RAW_STREAM_HPP
