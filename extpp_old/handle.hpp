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

template<typename T, typename... Args>
handle<T> construct(engine& e, raw_address addr, Args&&... args) {
    // TODO: Allow non block-aligned addresses?
    EXTPP_ASSERT(addr && addr.get_offset_in_block(e.block_size()) == 0,
                 "Address does not point to a valid block.");
    return construct<T>(e.zeroed(addr.get_block_index(e.block_size())), std::forward<Args>(args)...);
}

template<typename T>
handle<T> access(engine& e, address<T> addr) {
    EXTPP_ASSERT(addr, "Accessing an invalid address.");
    EXTPP_ASSERT(addr.raw().get_offset_in_block(e.block_size()) + sizeof(T) <= e.block_size(),
                 "Object spans multiple blocks.");

    block_handle block = e.read(addr.raw().get_block_index(e.block_size()));
    T* ptr = static_cast<T*>(static_cast<void*>(block.data() + addr.raw().get_offset_in_block(e.block_size())));
    return {std::move(block), ptr};
}

} // namespace extpp

#endif // EXTPP_HANDLE_HPP
