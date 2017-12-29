#ifndef EXTPP_HANDLE_HPP
#define EXTPP_HANDLE_HPP

#include <extpp/address.hpp>
#include <extpp/block_handle.hpp>
#include <extpp/engine.hpp>
#include <extpp/type_traits.hpp>

namespace extpp {

/// A smart pointer for values stored in a block.
///
/// Handles point to data that has been loaded from disk and currently
/// resides in main memory. The objects can be freely modified through handles,
/// but the underlying blocks have to be marked as `dirty()` for the changes
/// to be written to disk again.
///
/// Objects remain in main memory for as long as handles to them (or their underlying blocks)
/// remain in existence.
template<typename T>
class handle {
    static_assert(is_trivial<T>::value, "The type must be trivial.");

public:
    using element_type = T;

public:
    /// Constructs an invalid handle.
    handle() = default;

    /// Constructs a handle to the given `data` object
    /// that must be located within the block referenced by `block`.
    handle(block_handle block, T* data) noexcept
        : m_block(std::move(block))
        , m_data(data)
    {
        EXTPP_ASSERT(!m_block ? m_data == nullptr
                              : ((byte*) m_data >= m_block.data() &&
                                 (byte*) m_data < m_block.data() + m_block.block_size()),
                    "invalid pointer for that handle");
    }

    handle(const handle&) = default;
    handle& operator=(const handle&) = default;

    handle(handle&& other) noexcept
        : m_block(std::move(other.m_block))
        , m_data(std::exchange(other.m_data, nullptr))
    {}

    handle& operator=(handle&& other) noexcept {
        if (this != &other) {
            m_block = std::move(other.m_block);
            m_data = std::exchange(other.m_data, nullptr);
        }
        return *this;
    }

    void reset(block_handle h, T* data) noexcept {
        *this = handle(std::move(h), data);
    }

    void reset() noexcept {
        *this = handle();
    }

    /// Returns a handle that points to some other object
    /// within the same block.
    ///
    /// \pre `*this`.
    /// \pre `ptr` must point into the same block as `data()`.
    template<typename U>
    handle<U> neighbor(U* ptr) const& {
        EXTPP_ASSERT(*this, "invalid pointer");
        return handle<U>(m_block, ptr);
    }

    template<typename U>
    handle<U> neighbor(U* ptr) && {
        EXTPP_ASSERT(*this, "invalid pointer");
        return handle<U>(std::move(m_block), ptr);
    }

    /// Returns a handle to some member of the current object.
    ///
    /// Example:
    ///     struct test { int x; };
    ///
    ///     handle<test> h1 = ...;
    ///     handle<test> h2 = h1.member(&test::x); // points to h1->x.
    template<typename B, typename U, std::enable_if_t<
        std::is_base_of<B, T>::value>* = nullptr
    >
    handle<U> member(U B::*m) const& {
        EXTPP_ASSERT(*this, "invalid pointer");
        return neighbor(std::addressof(get()->*m));
    }

    template<typename B, typename U, std::enable_if_t<
        std::is_base_of<B, T>::value>* = nullptr
    >
    handle<U> member(U B::*m) && {
        EXTPP_ASSERT(*this, "invalid pointer");
        return std::move(*this).neighbor(std::addressof(get()->*m));
    }

    /// Returns the address of this object on disk.
    extpp::address<T> address() const {
        if (!valid())
            return {};

        raw_address raw = m_block.address();
        raw += reinterpret_cast<const byte*>(m_data) - m_block.data();
        return raw_address_cast<T>(raw);
    }

//TODO
//    /// Returns a reference to the block engine.
//    /// \pre `*this`.
//    engine<BlockSize>& get_engine() const {
//        EXTPP_ASSERT(*this, "invalid pointer");
//        return m_block.get_engine();
//    }

    /// The handle to the block that contains this value.
    const block_handle& block() const & { return m_block; }
    block_handle&& block() && { return std::move(m_block); }

    /// Mark the block that contains this value as dirty. \sa block_handle::dirty.
    /// \pre `*this`.
    void dirty() const { m_block.dirty(); }

    /// Returns a pointer to the value.
    /// The memory will stay valid for at least as long as this instants points to it.
    /// Returns `nullptr` if this handle is invalid.
    T* get() const { return m_data; }

    T* operator->() const { EXTPP_ASSERT(*this, "invalid pointer"); return get(); }
    T& operator*() const { EXTPP_ASSERT(*this, "invalid pointer"); return *get(); }

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    bool valid() const { return m_data; }

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    explicit operator bool() const { return valid(); }

    /// Implicitly convertible to base classes.
    template<typename U, std::enable_if_t<std::is_base_of<U, T>::value>* = nullptr>
    operator handle<U>() const & { return handle<U>(m_block, static_cast<U*>(m_data)); }

    template<typename U, std::enable_if_t<std::is_base_of<U, T>::value>* = nullptr>
    operator handle<U>() && {
        return handle<U>(std::move(m_block), static_cast<U*>(std::exchange(m_data, nullptr)));
    }

    /// Implicitly convertible to const.
    operator handle<const T>() const & { return {m_block, m_data}; }
    operator handle<const T>() && { return {std::move(m_block), std::exchange(m_data, nullptr)}; }

private:
    block_handle m_block;
    T* m_data = nullptr;
};

template<typename T, typename U>
bool operator==(const handle<T>& lhs, const handle<U>& rhs) {
    return lhs.get() == rhs.get();
}

template<typename T, typename U, u32 BlockSize>
bool operator!=(const handle<T>& lhs, const handle<U>& rhs) {
    return lhs.get() != rhs.get();
}

/// Cast the block handle to a specific type via reinterpret_cast.
template<typename T>
handle<T> cast(block_handle block) {
    EXTPP_ASSERT(sizeof(T) <= block.block_size(), "Type does not fit into a block.");

    T* ptr = static_cast<T*>(static_cast<void*>(block.data()));
    return handle<T>(std::move(block), ptr);
}

template<typename T, typename U, u32 BlockSize>
handle<T> cast(handle<U> h) {
    T* ptr = static_cast<T*>(h.get());
    return handle<T>(std::move(h).block(), ptr);
}

/// Constructs a new object of type T in the given block.
/// Invokes the constructor of T and passes the provided arguments.
/// The block will be marked as dirty.
template<typename T, typename... Args>
handle<T> construct(block_handle block, Args&&... args) {
    EXTPP_ASSERT(sizeof(T) <= block.block_size(), "Type does not fit into a block.");

    T* ptr = new(block.data()) T(std::forward<Args>(args)...);
    block.dirty();
    return handle<T>(std::move(block), ptr);
}

template<typename T, u32 BlockSize, typename... Args>
handle<T> construct(engine<BlockSize>& e, raw_address addr, Args&&... args) {
    // TODO: Allow non block-aligned addresses?
    EXTPP_ASSERT(addr && addr.get_offset_in_block(BlockSize) == 0, "Address does not point to a valid block.");
    return construct<T>(e.zeroed(addr.get_block_index(BlockSize)), std::forward<Args>(args)...);
}

template<typename T, u32 BlockSize>
handle<T> access(engine<BlockSize>& e, address<T> addr) {
    EXTPP_ASSERT(addr, "Accessing an invalid address.");
    EXTPP_ASSERT(addr.raw().get_offset_in_block(BlockSize) + sizeof(T) <= BlockSize, "Object spans multiple blocks.");

    block_handle block = e.read(addr.raw().get_block_index(BlockSize));
    T* ptr = static_cast<T*>(static_cast<void*>(block.data() + addr.raw().get_offset_in_block(BlockSize)));
    return {std::move(block), ptr};
}

/// Perform a linear write, starting from the given disk address.
/// Will write exactly `size` bytes from `data` to disk to the
/// address range [address, address + size).
template<u32 BlockSize>
void write(engine<BlockSize>& e, raw_address address, const void* data, size_t size) {
    if (size == 0)
        return;

    const byte* buffer = reinterpret_cast<const byte*>(data);
    block_index index = address.get_block_index(BlockSize);

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(BlockSize); offset != 0) {
        auto block = e.read(index);
        size_t n = std::min(size, size_t(BlockSize - offset));
        std::memmove(block.data() + offset, buffer, n);
        block.dirty();

        buffer += n;
        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= BlockSize) {
        e.overwritten(index, buffer);

        buffer += BlockSize;
        size -= BlockSize;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memmove(block.data(), buffer, size);
        block.dirty();
    }
}

/// Perform a linear read, starting from the given disk address.
/// Will read exactly `size` bytes from the address range [address, address + size)
/// on disk into `data`.
template<u32 BlockSize>
void read(engine<BlockSize>& e, raw_address address, void* data, size_t size) {
    if (size == 0)
        return;

    byte* buffer = reinterpret_cast<byte*>(data);
    block_index index = address.get_block_index(BlockSize);

    // Partial read at the start.
    if (u32 offset = address.get_offset_in_block(BlockSize); offset != 0) {
        auto block = e.read(index);
        size_t n = std::min(size, size_t(BlockSize - offset));
        std::memmove(buffer, block.data() + offset, n);

        buffer += n;
        size -= n;
        index += 1;
    }
    // Full block reads.
    while (size >= BlockSize) {
        auto block = e.read(index);
        std::memmove(buffer, block.data(), BlockSize);

        buffer += BlockSize;
        size -= BlockSize;
        index += 1;
    }
    // Partial read at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memmove(buffer, block.data(), size);
    }
}

/// Zeroes `size` bytes, starting from the given address.
template<u32 BlockSize>
void zero(engine<BlockSize>& e, raw_address address, u64 size) {
    if (size == 0)
        return;

    block_index index = address.get_block_index(BlockSize);

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(BlockSize); offset != 0) {
        auto block = e.read(index);
        u64 n = std::min(size, u64(BlockSize - offset));
        std::memset(block.data() + offset, 0, n);
        block.dirty();

        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= BlockSize) {
        e.zeroed(index);

        size -= BlockSize;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memset(block.data(), 0, size);
        block.dirty();
    }
}

namespace detail {

template<u32 BlockSize>
void copy_forward(engine<BlockSize>& e, raw_address dest, raw_address src, u64 size) {
    auto index = [](auto addr) { return addr.get_block_index(BlockSize); };
    auto offset = [](auto addr) { return addr.get_offset_in_block(BlockSize); };

    const bool can_overwrite = distance(src, dest) >= BlockSize;

    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size > BlockSize) {
                dest_handle = e.zeroed(index(dest));
            } else {
                dest_handle = e.read(index(dest));
                dest_handle.dirty();
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src));
        }

        u32 chunk = std::min(BlockSize - offset(src),
                             BlockSize - offset(dest));
        if (chunk > size)
            chunk = size;

        EXTPP_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.data() + offset(dest),
                     src_handle.data() + offset(src), chunk);
        src += chunk;
        dest += chunk;
        size -= chunk;
    }
}

template<u32 BlockSize>
void copy_backward(engine<BlockSize>& e, raw_address dest, raw_address src, u64 size) {
    auto index = [](auto addr) { return addr.get_block_index(BlockSize); };
    auto offset = [](auto addr) { return addr.get_offset_in_block(BlockSize); };

    const bool can_overwrite = distance(src, dest) >= BlockSize;

    src += size;
    dest += size;
    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size > BlockSize) {
                dest_handle = e.zeroed(index(dest-1));
            } else {
                dest_handle = e.read(index(dest-1));
                dest_handle.dirty();
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src-1));
        }

        u32 chunk = std::min(offset(src) ? offset(src) : BlockSize,
                             offset(dest) ? offset(dest) : BlockSize);
        if (chunk > size)
            chunk = size;

        src -= chunk;
        dest -= chunk;
        size -= chunk;

        EXTPP_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.data() + offset(dest),
                     src_handle.data() + offset(src), chunk);
    }
}

}  // namespace detail

/// Copies `size` bytes from `src` to `dest`. The two ranges can overlap.
/// \pre `src` and `dest` are valid addresses.
template<u32 BlockSize>
void copy(engine<BlockSize>& e, raw_address dest, raw_address src, u64 size) {
    EXTPP_ASSERT(dest, "Invalid destination address.");
    EXTPP_ASSERT(src, "Invalid source address.");

    if (dest == src || size == 0)
        return;
    if (src > dest || (src + size <= dest)) {
        return detail::copy_forward(e, dest, src, size);
    }
    return detail::copy_backward(e, dest, src, size);
}

} // namespace extpp

#endif // EXTPP_HANDLE_HPP
