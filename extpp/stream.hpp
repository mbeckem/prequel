#ifndef EXTPP_STREAM_HPP
#define EXTPP_STREAM_HPP

#include <extpp/assert.hpp>
#include <extpp/block.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>
#include <extpp/math.hpp>
#include <extpp/raw.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <new>

namespace extpp {

template<typename T, u32 BlockSize>
class stream {
public:
    using value_type = T;

    using size_type = u64;

    static constexpr u32 block_size = BlockSize;

    class iterator;
    using const_iterator = iterator;

    static_assert(is_trivial<value_type>::value,
                  "The value type must be trivial.");

private:
    struct block_header {};

    struct block_t : make_array_block_t<block_header, raw<T>, BlockSize> {
        T* get(u32 index) {
            EXTPP_ASSERT(index < block_t::capacity, "Index points outside the array.");
            return this->values[index].ptr();
        }
    };

    using block_type = handle<block_t, BlockSize>;

public:
    class anchor {
        /// Value storage.
        typename extent<BlockSize>::anchor extent;

        /// The current number of items in this stream.
        u64 size = 0;

        friend class stream;
    };

public:
    stream(handle<anchor, BlockSize> h, extpp::engine<BlockSize>& e, extpp::allocator<BlockSize>& a)
        : m_anchor(std::move(h))
        , m_extent(m_anchor.neighbor(&m_anchor->extent), e, a)
    {}

    stream(const stream&) = delete;
    stream(stream&&) noexcept = default;

    stream& operator=(const stream&) = delete;
    stream& operator=(stream&&) noexcept = default;

    extpp::engine<BlockSize>& engine() const { return m_extent.engine(); }
    extpp::allocator<BlockSize>& allocator() const { return m_extent.allocator(); }

    static constexpr u32 block_capacity() { return block_t::capacity; }

    iterator begin() const { return iterator(this, 0); }
    iterator end() const { return iterator(this, size()); }

    bool empty() const { return size() == 0; }
    u64 size() const { return m_anchor->size; }
    u64 capacity() const { return blocks() * block_capacity(); }
    u64 blocks() const { return m_extent.size(); }

    /// The relative space utilization (memory used by elements
    /// divided by the total allocated memory).
    double fill_factor() const {
        return capacity() == 0 ? 0 : double(size()) / double(capacity());
    }

    /// Size of this datastructure in bytes (includes unused capacity).
    u64 byte_size() const { return blocks() * BlockSize; }

    /// The relative overhead of this datastructure compared to storing
    /// all values in a linear file (without additional unused capacity).
    double overhead() const {
        return capacity() == 0 ? 0 : double(byte_size()) / (size * sizeof(value_type));
    }

    /// Returns the element at the given index.
    /// \pre `index < size()`.
    // TODO: This could be a reference if the stream would pin the most recently used block
    // in memory. The reference would stay valid until the next access (or push_back/emplace_back).
    // I don't know if that would be a good idea, though.
    value_type operator[](u64 index) const {
        EXTPP_ASSERT(index < size(), "Index out of bounds.");

        auto block = access(block_index(index));
        return *block->get(block_offset(index));
    }

    /// Reserves enough space for at least `n` elements.
    /// Does nothing if the capacity is already sufficient.
    /// Invalidates all existing iterators and references if new space
    /// has to be allocated.
    ///
    /// \post `capacity() >= n`.
    void reserve(u64 n) {
        u64 blocks = (n + block_capacity() - 1) / block_capacity();
        if (blocks > m_extent.size())
            grow_extent(blocks);
    }

    /// Emplaces a new element at the end of the stream.
    /// Existing iterators and references will be invalidated if a reallocation
    /// is required, i.e. if the current capacity is too small for the new size.
    template<typename... Args>
    void emplace_back(Args&&... args) {
        push_back_impl(std::forward<Args>(args)...);
    }

    /// Inserts a new element at the end of the stream.
    /// Existing iterators and references will be invalidated if a reallocation
    /// is required, i.e. if the current capacity is too small for the new size.
    void push_back(const value_type& value) {
        // We have to make a copy of value here. Otherwise, a reference could
        // point into the current stream itself and would become invalid
        // if a relocation is required. We have no (efficient) way of telling
        // whether a reference points to one of our blocks or not.
        push_back_impl(value_type(value));
    }

    /// Remove the last element from the stream.
    /// \pre `!empty()`.
    void pop_back() {
        EXTPP_ASSERT(!empty(), "Cannot call pop_back() on an empty stream.");
        m_anchor->size--;
        m_anchor.dirty();
    }

    /// Modify the value at the given position. The `op` function
    /// will be called with a mutable reference to the object.
    template<typename Operation>
    void modify(const iterator& pos, Operation&& op) {
        EXTPP_ASSERT(pos.parent_stream() == this, "Iterator is invalid or belongs to a different stream.");
        EXTPP_ASSERT(pos.index() < size(), "Iterator points to an invalid index.");
        op(*pos.block()->get(pos.block_offset()));
        pos.block().dirty();
    }

    /// Replaces the value at the given position.
    void replace(const iterator& pos, const value_type& value) {
        modify(pos, [&](value_type& v) {
            v = value;
        });
    }

private:
    template<typename... Args>
    void push_back_impl(Args&&... args) {
        const u64 blk_index = block_index(m_anchor->size);
        const u32 blk_offset = block_offset(m_anchor->size);
        if (blk_index == m_extent.size())
            grow_extent(m_extent.size() + 1);

        auto block = blk_offset == 0 ? create(blk_index) : access(blk_index);
        new (block->get(blk_offset)) value_type(std::forward<Args>(args)...);
        block.dirty();

        m_anchor->size++;
        m_anchor.dirty();

        EXTPP_ASSERT(size() <= capacity(), "Size invariant violated.");
    }

    block_type create(u64 blk_index) const {
        // The 'construct' call should be a no-op but we do it anyway just to be safe.
        return construct<block_t>(m_extent.overwrite(blk_index));
    }

    block_type access(u64 blk_index) const {
        return cast<block_t>(m_extent.access(blk_index));
    }

    /// Grow the extent to at least `minimum` blocks.
    void grow_extent(u64 minimum) {
        EXTPP_ASSERT(minimum > m_extent.size(), "Cannot shrink the extent.");
        // TODO: Different growth strategies.
        u64 new_size = round_towards_pow2(minimum);
        m_extent.resize(new_size);
    }

    u64 block_index(u64 value_index) const {
        return value_index / block_capacity();
    }

    u32 block_offset(u64 value_index) const {
        return value_index % block_capacity();
    }

private:
    handle<anchor, BlockSize> m_anchor;
    extent<BlockSize> m_extent;
};

template<typename T, u32 BlockSize>
class stream<T, BlockSize>::iterator : public boost::iterator_facade<
    iterator,
    value_type,
    std::random_access_iterator_tag,
    const value_type&,
    i64
>
{
public:
    iterator() = default;

private:
    friend class stream;

    iterator(const stream* s, u64 index)
        : m_stream(s)
        , m_index(index)
    {
        move_to(index);
    }

    const stream* parent_stream() const { return m_stream; }

    u32 index() const { return m_index; }
    u32 block_offset() const { return m_block_offset; }

    const block_type& block() const {
        if (!m_block)
            m_block = m_stream->access(m_block_index);
        return m_block;
    }

private:
    void move_to(u64 index) {
        EXTPP_ASSERT(m_stream, "Cannot change the position of the invalid iterator.");
        EXTPP_ASSERT(index <= m_stream->size(), "Index out of bounds.");

        const u64 old_block_index = m_block_index;
        m_index = index;
        m_block_index = m_stream->block_index(m_index);     // Might be too large if this is now the end.
        m_block_offset = m_stream->block_offset(m_index);

        if (old_block_index != m_block_index)
            m_block.reset();
    }

private:
    friend class boost::iterator_core_access;

    const value_type& dereference() const {
        EXTPP_ASSERT(m_stream, "Cannot dereference the invalid iterator.");
        EXTPP_ASSERT(m_index < m_stream->size(), "Cannot dereference the end iterator.");
        return *block()->get(m_block_offset);
    }

    bool equal(const iterator& other) const {
        EXTPP_ASSERT(m_stream == other.m_stream, "Cannot compare iterators from different containers.");
        return m_index == other.m_index;
    }

    void increment() {
        EXTPP_ASSERT(m_stream, "Cannot increment the invalid iterator.");
        EXTPP_ASSERT(m_index < m_stream->size(), "Cannot increment the past-the-end iterator.");

        m_index++;
        if (++m_block_offset == m_stream->block_capacity()) {
            m_block_index++;
            m_block_offset = 0;
            m_block.reset();
        }
    }

    void decrement() {
        EXTPP_ASSERT(m_stream, "Cannot increment the invalid iterator.");
        EXTPP_ASSERT(m_index > 0, "Cannot decrement the begin iterator.");

        m_index--;
        if (m_block_offset-- == 0) {
            m_block_index--;
            m_block_offset = m_stream->block_capacity() - 1;
            m_block.reset();
        }
    }

    void advance(i64 n) {
        EXTPP_ASSERT(m_stream, "Cannot advance the invalid iterator.");
        EXTPP_ASSERT(m_index + n <= m_stream->size(), "Index is out of bounds.");
        move_to(m_index + n);
    }

    i64 distance_to(const iterator& other) const {
        return signed_difference(other.index(), index());
    }

private:
    const stream* m_stream = nullptr;
    u64 m_index = 0;
    u64 m_block_index = 0;
    u32 m_block_offset = 0;

    // Only lazily initialized when the iterator is dereferenced.
    // Reset to null if the current block changes.
    mutable block_type m_block;
};

} // namespace extpp

#endif // EXTPP_STREAM_HPP
