#ifndef EXTPP_HANDLE_HPP
#define EXTPP_HANDLE_HPP

#include <extpp/address.hpp>
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
template<typename T, u32 BlockSize>
class handle {
    static_assert(is_trivial<T>::value, "The type must be trivial.");
    static_assert(sizeof(T) <= BlockSize, "Type too large for BlockSize");

public:
    using element_type = T;

public:
    /// Constructs an invalid handle.
    handle() = default;

    /// Constructs a handle to the given `data` object
    /// that must be located within the block referenced by `block`.
    handle(block_handle<BlockSize> block, T* data) noexcept
        : m_block(std::move(block))
        , m_data(data)
    {
        EXTPP_ASSERT(!m_block ? m_data == nullptr
                              : ((byte*) m_data >= m_block.data() &&
                                 (byte*) m_data < m_block.data() + BlockSize),
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

    void reset(block_handle<BlockSize> h, T* data) noexcept {
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
    handle<U, BlockSize> neighbor(U* ptr) const& {
        EXTPP_ASSERT(*this, "invalid pointer");
        return handle<U, BlockSize>(m_block, ptr);
    }

    template<typename U>
    handle<U, BlockSize> neighbor(U* ptr) && {
        EXTPP_ASSERT(*this, "invalid pointer");
        return handle<U, BlockSize>(std::move(m_block), ptr);
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
    handle<U, BlockSize> member(U B::*m) const& {
        EXTPP_ASSERT(*this, "invalid pointer");
        return neighbor(std::addressof(get()->*m));
    }

    template<typename B, typename U, std::enable_if_t<
        std::is_base_of<B, T>::value>* = nullptr
    >
    handle<U, BlockSize> member(U B::*m) && {
        EXTPP_ASSERT(*this, "invalid pointer");
        return std::move(*this).neighbor(std::addressof(get()->*m));
    }

    /// Returns the address of this object on disk.
    extpp::address<T, BlockSize> address() const {
        if (!valid())
            return {};
        auto raw = raw_address<BlockSize>::from_block(m_block.index());
        raw += reinterpret_cast<const byte*>(m_data) - m_block.data();
        return raw_address_cast<T>(raw);
    }

    /// Returns a reference to the block engine.
    /// \pre `*this`.
    engine<BlockSize>& get_engine() const {
        EXTPP_ASSERT(*this, "invalid pointer");
        return m_block.get_engine();
    }

    /// The handle to the block that contains this value.
    const block_handle<BlockSize>& block() const & { return m_block; }
    block_handle<BlockSize>&& block() && { return std::move(m_block); }

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
    operator handle<U, BlockSize>() const & { return handle<U, BlockSize>(m_block, static_cast<U*>(m_data)); }

    template<typename U, std::enable_if_t<std::is_base_of<U, T>::value>* = nullptr>
    operator handle<U, BlockSize>() && {
        return handle<U, BlockSize>(std::move(m_block), static_cast<U*>(std::exchange(m_data, nullptr)));
    }

    /// Implicitly convertible to const.
    operator handle<const T, BlockSize>() const & { return {m_block, m_data}; }
    operator handle<const T, BlockSize>() && { return {std::move(m_block), std::exchange(m_data, nullptr)}; }

private:
    block_handle<BlockSize> m_block;
    T* m_data = nullptr;
};

template<typename T, typename U, u32 BlockSize>
bool operator==(const handle<T, BlockSize>& lhs, const handle<U, BlockSize>& rhs) {
    return lhs.get() == rhs.get();
}

template<typename T, typename U, u32 BlockSize>
bool operator!=(const handle<T, BlockSize>& lhs, const handle<U, BlockSize>& rhs) {
    return lhs.get() != rhs.get();
}

/// Cast the block handle to a specific type via reinterpret_cast.
template<typename T, u32 BlockSize>
handle<T, BlockSize> cast(block_handle<BlockSize> block) {
    static_assert(sizeof(T) <= BlockSize, "Type does not fit into a block.");

    T* ptr = static_cast<T*>(static_cast<void*>(block.data()));
    return handle<T, BlockSize>(std::move(block), ptr);
}

template<typename T, typename U, u32 BlockSize>
handle<T, BlockSize> cast(handle<U, BlockSize> h) {
    T* ptr = static_cast<T*>(h.get());
    return handle<T, BlockSize>(std::move(h).block(), ptr);
}

/// Constructs a new object of type T in the given block.
/// Invokes the constructor of T and passes the provided arguments.
/// The block will be marked as dirty.
template<typename T, u32 BlockSize, typename... Args>
handle<T, BlockSize> construct(block_handle<BlockSize> block, Args&&... args) {
    static_assert(sizeof(T) <= BlockSize, "Type does not fit into a block.");

    T* ptr = new(block.data()) T(std::forward<Args>(args)...);
    block.dirty();
    return handle<T, BlockSize>(std::move(block), ptr);
}

template<typename T, u32 BlockSize, typename... Args>
handle<T, BlockSize> construct(engine<BlockSize>& e, raw_address<BlockSize> addr, Args&&... args) {
    // TODO: Allow non block-aligned addresses?
    EXTPP_ASSERT(addr && addr.get_offset_in_block() == 0, "Address does not point to a valid block.");
    return construct<T>(e.zeroed(addr.get_block_index()), std::forward<Args>(args)...);
}

template<typename T, u32 BlockSize>
handle<T, BlockSize> access(engine<BlockSize>& e, address<T, BlockSize> addr) {
    EXTPP_ASSERT(addr, "Accessing an invalid address.");
    EXTPP_ASSERT(addr.raw().get_offset_in_block() + sizeof(T) <= BlockSize, "Object spans multiple blocks.");

    block_handle<BlockSize> block = e.read(addr.raw().get_block_index());
    T* ptr = static_cast<T*>(static_cast<void*>(block.data() + addr.raw().get_offset_in_block()));
    return {std::move(block), ptr};
}

/// Perform a linear write, starting from the given disk address.
/// Will write exactly `size` bytes from `data` to disk to the
/// address range [address, address + size).
template<u32 BlockSize>
void write(engine<BlockSize>& e, raw_address<BlockSize> address, const void* data, size_t size) {
    if (size == 0)
        return;

    const byte* buffer = reinterpret_cast<const byte*>(data);
    block_index index = address.get_block_index();

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(); offset != 0) {
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
void read(engine<BlockSize>& e, raw_address<BlockSize> address, void* data, size_t size) {
    if (size == 0)
        return;

    byte* buffer = reinterpret_cast<byte*>(data);
    block_index index = address.get_block_index();

    // Partial read at the start.
    if (u32 offset = address.get_offset_in_block(); offset != 0) {
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
void zero(engine<BlockSize>& e, raw_address<BlockSize> address, u64 size) {
    if (size == 0)
        return;

    block_index index = address.get_block_index();

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(); offset != 0) {
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
void copy_forward(engine<BlockSize>& e, raw_address<BlockSize> dest, raw_address<BlockSize> src, u64 size) {
    const bool can_overwrite = distance(src, dest) >= BlockSize;

    block_handle<BlockSize> src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || dest.get_offset_in_block() == 0) {
            if (can_overwrite && dest.get_offset_in_block() == 0 && size > BlockSize) {
                dest_handle = e.zeroed(dest.get_block_index());
            } else {
                dest_handle = e.read(dest.get_block_index());
                dest_handle.dirty();
            }
        }

        if (!src_handle || src.get_offset_in_block() == 0) {
            src_handle = e.read(src.get_block_index());
        }

        u32 chunk = std::min(BlockSize - src.get_offset_in_block(),
                             BlockSize - dest.get_offset_in_block());
        if (chunk > size)
            chunk = size;

        EXTPP_ASSERT(dest_handle.index() == dest.get_block_index(), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == src.get_block_index(), "Correct source block.");
        std::memmove(dest_handle.data() + dest.get_offset_in_block(),
                     src_handle.data() + src.get_offset_in_block(), chunk);
        src += chunk;
        dest += chunk;
        size -= chunk;
    }
}

template<u32 BlockSize>
void copy_backward(engine<BlockSize>& e, raw_address<BlockSize> dest, raw_address<BlockSize> src, u64 size) {
    const bool can_overwrite = distance(src, dest) >= BlockSize;

    src += size;
    dest += size;
    block_handle<BlockSize> src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || dest.get_offset_in_block() == 0) {
            if (can_overwrite && dest.get_offset_in_block() == 0 && size > BlockSize) {
                dest_handle = e.zeroed((dest-1).get_block_index());
            } else {
                dest_handle = e.read((dest-1).get_block_index());
                dest_handle.dirty();
            }
        }

        if (!src_handle || src.get_offset_in_block() == 0) {
            src_handle = e.read((src-1).get_block_index());
        }

        u32 chunk = std::min(src.get_offset_in_block() ? src.get_offset_in_block() : BlockSize,
                             dest.get_offset_in_block() ? dest.get_offset_in_block() : BlockSize);
        if (chunk > size)
            chunk = size;

        src -= chunk;
        dest -= chunk;
        size -= chunk;

        EXTPP_ASSERT(dest_handle.index() == dest.get_block_index(), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == src.get_block_index(), "Correct source block.");
        std::memmove(dest_handle.data() + dest.get_offset_in_block(),
                     src_handle.data() + src.get_offset_in_block(), chunk);
    }
}

}  // namespace detail

/// Copies `size` bytes from `src` to `dest`. The two ranges can overlap.
/// \pre `src` and `dest` are valid addresses.
template<u32 BlockSize>
void copy(engine<BlockSize>& e, raw_address<BlockSize> dest, raw_address<BlockSize> src, u64 size) {
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
