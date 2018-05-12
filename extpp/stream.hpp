#ifndef EXTPP_STREAM_HPP
#define EXTPP_STREAM_HPP

#include <extpp/raw_stream.hpp>
#include <extpp/serialization.hpp>

namespace extpp {

template<typename T>
class stream {
public:
    using value_type = T;

public:
    class anchor {
        raw_stream::anchor stream;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::stream);
        }

        friend class stream;
        friend class binary_format_access;
    };

public:
    explicit stream(handle<anchor> _anchor, allocator& alloc)
        : inner(std::move(_anchor).template member<&anchor::stream>(), value_size(), alloc)
    {}

public:
    engine& get_engine() const { return inner.get_engine(); }
    allocator& get_allocator() const { return inner.get_allocator(); }

    static constexpr u32 value_size() { return serialized_size<T>(); }
    u32 block_capacity() const { return inner.block_capacity(); }

    bool empty() const { return inner.empty(); }
    u64 size() const { return inner.size(); }
    u64 capacity() const { return inner.capacity(); }
    u64 blocks() const { return inner.blocks(); }
    double fill_factor() const { return inner.fill_factor(); }
    u64 byte_size() const { return inner.byte_size(); }
    double overhead() const { return inner.overhead(); }

    // TODO: Use data pointer in block directly to avoid one memcopy.
    value_type get(u64 index) const {
        serialized_buffer<T> buffer;
        inner.get(index, buffer.data());
        return deserialized_value<value_type>(buffer.data(), buffer.size());
    }

    value_type operator[](u64 index) const { return get(index); }

    void set(u64 index, const value_type& value) {
        auto buffer = serialized_value(value);
        inner.set(index, buffer.data());
    }

    void clear() { inner.clear(); }

    void resize(u64 n, const value_type& value = value_type()) {
        auto buffer = serialized_value(value);
        inner.resize(n, buffer.data());
    }

    void reserve(u64 n) { inner.reserve(n); }

    void push_back(const value_type& value) {
        auto buffer = serialized_value(value);
        inner.push_back(buffer.data());
    }

    void pop_back() { inner.pop_back(); }

    void growth(const growth_strategy& g) { inner.growth(g); }
    growth_strategy growth() const { return inner.growth(); }

    const raw_stream& raw() const { return inner; }

private:
    raw_stream inner;
};

} // namespace extpp

#endif // EXTPP_STREAM_HPP
