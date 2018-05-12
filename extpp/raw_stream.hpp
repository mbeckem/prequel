#ifndef EXTPP_RAW_STREAM_HPP
#define EXTPP_RAW_STREAM_HPP

#include <extpp/allocator.hpp>
#include <extpp/binary_format.hpp>
#include <extpp/block_handle.hpp>
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

    u32 value_size() const;
    u32 block_capacity() const;

    bool empty() const;
    u64 size() const;
    u64 capacity() const;
    u64 blocks() const;
    double fill_factor() const;
    u64 byte_size() const;
    double overhead() const;

    void get(u64 index, byte* value) const;
    void set(u64 index, const byte* value);

    void clear();
    void resize(u64 n, const byte* value);
    void reserve(u64 n);

    void push_back(const byte* value);
    void pop_back();

    void growth(const growth_strategy& g);
    growth_strategy growth() const;

private:
    raw_stream_impl& impl() const;

private:
    std::unique_ptr<raw_stream_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_RAW_STREAM_HPP
