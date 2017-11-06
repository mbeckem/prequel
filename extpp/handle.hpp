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

    void reset(block_handle<BlockSize> handle, T* data) noexcept {
        *this = handle(std::move(handle), data);
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
    handle<U, BlockSize> neighbor(U* ptr) const {
        EXTPP_ASSERT(*this, "invalid pointer");
        return handle<U, BlockSize>(m_block, ptr);
    }

    /// Returns the address of this object on disk.
    extpp::address<T, BlockSize> address() const {
        if (!valid())
            return {};
        raw_address<BlockSize> raw(m_block.address().block_index(),
                                   reinterpret_cast<const byte*>(m_data) - m_block.data());
        return address_cast<T>(raw);
    }

    /// Returns a reference to the block engine.
    /// \pre `*this`.
    extpp::engine<BlockSize>& engine() const {
        EXTPP_ASSERT(*this, "invalid pointer");
        return m_block.engine();
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
    EXTPP_ASSERT(addr && addr.block_offset() == 0, "Address does not point to a valid block.");
    return construct<T>(e.read_zero(addr.block_index()), std::forward<Args>(args)...);
}

template<typename T, u32 BlockSize>
handle<T, BlockSize> access(engine<BlockSize>& e, address<T, BlockSize> addr) {
    EXTPP_ASSERT(addr, "Accessing an invalid address.");
    EXTPP_ASSERT(addr.raw().block_offset() + sizeof(T) <= BlockSize, "Object spans multiple blocks.");

    block_handle<BlockSize> block = e.read(addr.raw().block_index());
    T* ptr = static_cast<T*>(static_cast<void*>(block.data() + addr.raw().block_offset()));
    return {std::move(block), ptr};
}

} // namespace extpp

#endif // EXTPP_HANDLE_HPP