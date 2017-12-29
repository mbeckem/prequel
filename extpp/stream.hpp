#ifndef EXTPP_STREAM_HPP
#define EXTPP_STREAM_HPP

#include <extpp/anchor_ptr.hpp>
#include <extpp/assert.hpp>
#include <extpp/block.hpp>
#include <extpp/extent.hpp>
#include <extpp/handle.hpp>
#include <extpp/math.hpp>
#include <extpp/raw.hpp>

#include <boost/iterator/iterator_facade.hpp>

#include <new>
#include <variant>

namespace extpp {

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

using growth_strategy = std::variant<linear_growth, exponential_growth>;

/// A stream is a dynamic array of objects in external storage and is
/// similar to `std::vector` in most aspects.
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

    using block_type = handle<block_t>;

public:
    class anchor {
        /// Value storage.
        typename extent<BlockSize>::anchor extent;

        /// The current number of items in this stream.
        u64 size = 0;

        friend class stream;
    };

    // TODO Destroy

public:
    stream(anchor_ptr<anchor> h, allocator<BlockSize>& alloc)
        : m_anchor(std::move(h))
        , m_extent(m_anchor.member(&anchor::extent), alloc)
    {}

    stream(const stream&) = delete;
    stream(stream&&) noexcept = default;

    stream& operator=(const stream&) = delete;
    stream& operator=(stream&&) noexcept = default;

    engine<BlockSize>& get_engine() const { return m_extent.get_engine(); }
    allocator<BlockSize>& get_allocator() const { return m_extent.get_allocator(); }

    /// \name Stream capacity
    /// @{

    /// Returns the maximum number of values per block.
    static constexpr u32 block_capacity() { return block_t::capacity; }

    /// Whether the stream is empty.
    bool empty() const { return size() == 0; }

    /// The number of elements in the stream.
    u64 size() const { return m_anchor->size; }

    /// The maximum number of elements the stream can store
    /// without reallocting its storage.
    u64 capacity() const { return blocks() * block_capacity(); }

    /// The number of blocks occupied by this stream.
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

    /// Returns the current growth strategy. Defaults to exponential growth.
    growth_strategy growth() const { return m_growth; }

    /// Alters the stream's growth strategy.
    void growth(const growth_strategy& g) { m_growth = g; }

    /// Reserves enough space for at least `n` elements.
    /// Does nothing if the capacity is already sufficient.
    /// Invalidates all existing iterators and references if new space
    /// has to be allocated. Storage is expanded as specified
    /// by the current growth strategy (see \ref growth()).
    ///
    /// \post `capacity() >= n`.
    void reserve(u64 n) {
        u64 blocks = ceil_div(n, u64(block_capacity()));
        if (blocks > m_extent.size())
            grow_extent(blocks);
    }

    /// @}

    /// \name Iteration
    /// @{

    /// Returns an iterator to the first element, or end() if the stream is empty.
    iterator begin() const { return iterator(this, 0); }

    /// Returns the past-the-end iterator.
    iterator end() const { return iterator(this, size()); }
    /// @}

    /// \name Element Access
    /// @{

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
    /// @}

    /// \name Modifiers
    /// @{

    /// Removes all elements from this stream. Does not free any space.
    // TODO: Shrink to fit
    void clear() {
        resize(0);
    }

    /// Resizes this stream to size `n`.
    /// New elements are initialized as copies of `value`.
    void resize(u64 n, value_type value = value_type()) {
        if (n == size())
            return;
        if (n < size()) {
            m_anchor->size = n;
            m_anchor.dirty();
            return;
        }

        // Same reason for the copy as in push back.
        const value_type v = value;
        reserve(n);
        {
            u64 remaining = n - size();
            u64 blk_index = block_index(m_anchor->size);
            u32 blk_offset = block_offset(m_anchor->size);

            auto block = blk_offset == 0 ? create(blk_index) : access(blk_index);
            block.dirty();
            while (remaining > 0) {
                new (block->get(blk_offset)) value_type(v);

                remaining--;
                if (remaining == 0)
                    break;

                ++blk_offset;
                if (blk_offset == block_capacity()) {
                    blk_index++;
                    blk_offset = 0;
                    block = create(blk_index);
                }
            }
        }

        m_anchor->size = n;
        m_anchor.dirty();
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
    void push_back(value_type value) {
        push_back_impl(value);
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
    /// @}

    /// Returns a raw handle to the object pointed to by `pos`.
    /// The object can be modified freely.
    ///
    /// \warning Handles are invalidated when the iterators are invalidated, i.e.
    /// when the storage is moved.
    handle<value_type> pointer_to(iterator pos) {
        EXTPP_ASSERT(pos.index() < size(), "Iterator points to an invalid index.");
        block_type h = pos.block();
        return h.neighbor(h->get(pos.block_offset()));
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
        return construct<block_t>(m_extent.zeroed(blk_index));
    }

    block_type access(u64 blk_index) const {
        return cast<block_t>(m_extent.access(blk_index));
    }

    /// Grow the extent to at least `minimum` blocks.
    void grow_extent(u64 minimum) {
        EXTPP_ASSERT(minimum > m_extent.size(), "Cannot shrink the extent.");

        struct visitor_t {
            u64 old_size;
            u64 minimum;

            u64 operator()(const linear_growth& g) {
                // Round up to multiple of chunk size.
                u64 alloc = minimum - old_size;
                return old_size + ceil_div(alloc, g.chunk_size()) * g.chunk_size();
            }

            u64 operator()(const exponential_growth&) {
                return round_towards_pow2(minimum);
            }
        } v{m_extent.size(), minimum};

        u64 new_size = std::visit(v, m_growth);
        m_extent.resize(new_size);
    }

    u64 block_index(u64 value_index) const {
        return value_index / block_capacity();
    }

    u32 block_offset(u64 value_index) const {
        return value_index % block_capacity();
    }

private:
    anchor_ptr<anchor> m_anchor;
    extent<BlockSize> m_extent;
    growth_strategy m_growth = exponential_growth();
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

    u64 index() const { return m_index; }
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
