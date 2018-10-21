#ifndef PREQUEL_ARRAY_HPP
#define PREQUEL_ARRAY_HPP

#include <prequel/raw_array.hpp>
#include <prequel/serialization.hpp>

namespace prequel {

/**
 * A dynamic array of for instances of type `T`.
 *
 * A stream stores a sequence of fixed-size values in contiguous storage on disk.
 * The stream can reserve capacity ahead of time to prepare for future insertions,
 * very similar to `std::vector<T>`.
 */
template<typename T>
class array {
public:
    using value_type = T;

public:
    class anchor {
        raw_array::anchor array;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::array);
        }

        friend class array;
        friend class binary_format_access;
    };

public:
    /**
     * Accesses an array rooted at the given anchor.
     * alloc` must be equivalent every time the raw array is loaded.
     */
    explicit array(anchor_handle<anchor> _anchor, allocator& alloc)
        : inner(std::move(_anchor).template member<&anchor::array>(), value_size(), alloc)
    {}

public:
    engine& get_engine() const { return inner.get_engine(); }
    allocator& get_allocator() const { return inner.get_allocator(); }

    /**
     * Returns the size of a serialized value on disk.
     */
    static constexpr u32 value_size() { return serialized_size<T>(); }

    /**
     * Returns the number of serialized values that fit into a single block on disk.
     */
    u32 block_capacity() const { return inner.block_capacity(); }

    /**
     * Returns true iff the stream is empty, i.e. contains zero values.
     */
    bool empty() const { return inner.empty(); }

    /**
     * Returns the number of values in this stream.
     */
    u64 size() const { return inner.size(); }

    /**
     * Returns the capacity of this stream, i.e. the maximum number of values
     * that can currently be stored without reallocating the storage on disk.
     * The stream will allocate storage according to its growth strategy when
     * an element is inserted into a full stream.
     *
     * @note `capacity() * value_size() == byte_size()` will always be true.
     */
    u64 capacity() const { return inner.capacity(); }

    /**
     * Returns the number of disk blocks currently allocated by the stream.
     */
    u64 blocks() const { return inner.blocks(); }

    /**
     * Returns the relative fill factor, i.e. the size divided by the capacity.
     */
    double fill_factor() const { return inner.fill_factor(); }

    /**
     * Returns the total size of this datastructure on disk, in bytes.
     */
    u64 byte_size() const { return inner.byte_size(); }

    /**
     * Returns the relative overhead of this datastructure compared to a linear file, i.e.
     * the allocated storage (see capacity()) dividied by the used storage (see size()).
     */
    double overhead() const { return inner.overhead(); }

    /**
     * Retrieves the value at the given index.
     *
     * @throws bad_argument     If the index is out of bounds.
     */
    value_type get(u64 index) const {
        serialized_buffer<T> buffer;
        inner.get(index, buffer.data());
        return deserialized_value<value_type>(buffer.data(), buffer.size());
    }

    /**
     * Equivalent to `get(index)`.
     */
    value_type operator[](u64 index) const { return get(index); }

    /**
     * Sets the value at the given index.
     *
     * @throws bad_argument     If the index is out of bounds.
     */
    void set(u64 index, const value_type& value) {
        auto buffer = serialized_value(value);
        inner.set(index, buffer.data());
    }

    /**
     * Frees all storage allocated by the stream.
     *
     * @post `size() == 0 && byte_size() == 0`.
     */
    void reset() { inner.reset(); }

    /**
     * Removes all objects from this stream, but does not
     * necessarily free the underlying storage.
     *
     * @post `size() == 0`.
     */
    void clear() { inner.clear(); }

    /**
     * Resizes the stream to the given size `n`.
     * If `n` is greater than the current size, `value` is used as a default
     * value for new elements.
     *
     * @post `size == n`.
     */
    void resize(u64 n, const value_type& value = value_type()) {
        auto buffer = serialized_value(value);
        inner.resize(n, buffer.data());
    }

    /**
     * Reserves sufficient storage for `n` values, while respecting the current growth strategy.
     * Note that the size remains unchanged.
     *
     * @post `capacity >= n`.
     */
    void reserve(u64 n) { inner.reserve(n); }

    /**
     * Inserts a new value at the end of the stream.
     * Allocates new storage in accordance with the current growth strategy
     * if there is no free capacity remaining.
     */
    void push_back(const value_type& value) {
        auto buffer = serialized_value(value);
        inner.push_back(buffer.data());
    }

    /**
     * Removes the last value from this stream.
     *
     * @throws bad_operation If the stream is empty.
     */
    void pop_back() { inner.pop_back(); }

    /**
     * Changes the current growth strategy. Streams support linear and exponential growth.
     */
    void growth(const growth_strategy& g) { inner.growth(g); }

    /**
     * Returns the current growth strategy.
     */
    growth_strategy growth() const { return inner.growth(); }

    /**
     * Returns the raw, byte oriented inner stream.
     */
    const raw_array& raw() const { return inner; }

private:
    raw_array inner;
};

} // namespace prequel

#endif // PREQUEL_ARRAY_HPP
