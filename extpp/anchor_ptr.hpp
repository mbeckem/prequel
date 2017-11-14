#ifndef EXTPP_ANCHOR_PTR_HPP
#define EXTPP_ANCHOR_PTR_HPP

#include <extpp/assert.hpp>
#include <extpp/handle.hpp>
#include <extpp/type_traits.hpp>

#include <memory>

namespace extpp {
namespace detail {

struct anchor_ptr_base {
    struct interface {
        virtual ~interface() = default;
        virtual void dirty() = 0;
        virtual interface* clone() = 0;

        interface() = default;
        interface(const interface&) = delete;
        interface& operator=(const interface&) = delete;
    };

    template<typename Pointer>
    struct impl : anchor_ptr_base::interface {
        Pointer p;

        impl(const Pointer& p): p(p) {}
        impl(Pointer&& p): p(std::move(p)) {}

        impl(const impl& other): interface(), p(other.p) {}
        impl& operator=(const impl&) = delete;

        void dirty() override { p.dirty(); }
        impl* clone() override  { return new impl(*this); }
    };

public:
    anchor_ptr_base() = default;

    anchor_ptr_base(void* addr, std::unique_ptr<interface>&& handle)
        : m_addr(addr), m_handle(std::move(handle)) {}

    anchor_ptr_base(const anchor_ptr_base& other)
        : m_addr(other.m_addr)
        , m_handle(m_addr ? other.m_handle->clone() : nullptr)
    {}

    anchor_ptr_base(anchor_ptr_base&& other) noexcept
        : m_addr(std::exchange(other.m_addr, nullptr))
        , m_handle(std::move(other.m_handle))
    {}

    anchor_ptr_base& operator=(const anchor_ptr_base& other) {
        m_addr = other.m_addr;
        m_handle.reset(m_addr ? other.m_handle->clone() : nullptr);
        return *this;
    }

    anchor_ptr_base& operator=(anchor_ptr_base&& other) noexcept {
        if (this != &other) {
            m_addr = std::exchange(other.m_addr, nullptr);
            m_handle = std::move(other.m_handle);
        }
        return *this;
    }

    anchor_ptr_base alias(void* ptr) const& {
        EXTPP_ASSERT(m_addr, "Invalid pointer.");
        EXTPP_ASSERT(ptr, "Invalid alias pointer.");
        return anchor_ptr_base(ptr, std::unique_ptr<interface>(m_handle->clone()));
    }

    anchor_ptr_base alias(void *ptr) && {
        EXTPP_ASSERT(m_addr, "Invalid pointer.");
        EXTPP_ASSERT(ptr, "Invalid alias pointer.");
        m_addr = nullptr;
        return anchor_ptr_base(ptr, std::move(m_handle));
    }

    void* get() const { return m_addr; }

    void dirty() const {
        EXTPP_ASSERT(m_addr, "Invalid pointer.");
        m_handle->dirty();
    }

private:
    void* m_addr = nullptr;
    std::unique_ptr<interface> m_handle; ///< Inefficient, but should be OK for now. SBO would help.
};

template<typename T>
struct dirty_raw_pointer {
    T* ptr;
    bool* flag;

    T* operator->() const { return ptr; }
    T& operator*() const { return *ptr; }
    void dirty() {
        if (flag)
            *flag = true;
    }
};


} // namespace detail

/// A type-erased pointer to some data structure's anchor.
/// Supports dereferencing and dirty-flagging.
///
/// The purpose of this class is to abstract over the real location of an anchor (i.e.
/// in a file on disk accessed using a handle<...> or in primary memory.
///
/// The current implementation is not very efficient (a small heap allocation is required for
/// every pointer and it relies entirely on virtual dispatch).
/// SBO would greatly improve the performance of this class.
template<typename T>
class anchor_ptr : detail::anchor_ptr_base {
    using anchor_ptr_base = anchor_ptr::anchor_ptr_base;
    using typename anchor_ptr::anchor_ptr_base::impl;
    using typename anchor_ptr::anchor_ptr_base::interface;

public:
    using element_type = T;

public:
    anchor_ptr() = default;

    anchor_ptr(const anchor_ptr&) = default;
    anchor_ptr(anchor_ptr&&) noexcept = default;

    anchor_ptr& operator=(const anchor_ptr&) = default;
    anchor_ptr& operator=(anchor_ptr&&) noexcept = default;

    template<typename Pointer, DisableSelf<anchor_ptr_base, Pointer>* = nullptr>
    anchor_ptr(Pointer&& p)
        : anchor_ptr_base(init_base(std::forward<Pointer>(p)))
    {}

    template<typename Pointer, DisableSelf<anchor_ptr_base, Pointer>* = nullptr>
    anchor_ptr& operator=(Pointer&& p) {
        anchor_ptr_base::operator=(init_base(std::forward<Pointer>(p)));
        return *this;
    }

    /// Construct a new anchor pointer with the same ownership semantics as this one.
    /// For example, a handle<...> based pointer only allows addresses within
    /// the same memory block.
    ///
    /// This is commonly used to obtain a (managed) pointer to a sub-object:
    ///
    ///     anchor_ptr<T> a = ...;
    ///
    ///     // b stays valid for as long as a would.
    ///     anchor_ptr<U> b = a.neighbor(&a->member);
    ///
    ///     // Marks the same block of memory as dirty as a.dirty().
    ///     b.dirty();
    ///
    template<typename U>
    anchor_ptr<U> neighbor(U* addr) const& {
        return anchor_ptr<U>(anchor_ptr_base::alias(addr));
    }

    template<typename U>
    anchor_ptr<U> neighbor(U* addr) && {
        return anchor_ptr<U>(static_cast<anchor_ptr_base&&>(*this).alias(addr));
    }

    T* operator->() const {
        EXTPP_ASSERT(get(), "Nullpointer dereference.");
        return get();
    }

    T& operator*() const {
        EXTPP_ASSERT(get(), "Nullpointer dereference.");
        return *get();
    }

    T* get() const { return static_cast<T*>(anchor_ptr_base::get()); }

    bool valid() const { return get() != nullptr; }
    explicit operator bool() const { return valid(); }

    void dirty() { anchor_ptr_base::dirty(); }

private:
    template<typename U>
    friend class anchor_ptr;

    anchor_ptr(anchor_ptr_base&& base)
        : anchor_ptr_base(std::move(base)) {}

    template<typename Pointer>
    static anchor_ptr_base init_base(Pointer&& p) {
        using impl_t = impl<std::decay_t<Pointer>>;

        T* addr = p ? std::addressof(*p) : nullptr;
        std::unique_ptr<interface> handle(addr ? new impl_t(std::forward<Pointer>(p)) : nullptr);
        return anchor_ptr_base(addr, std::move(handle));
    }
};

template<typename T>
anchor_ptr<T> raw_anchor_ptr(T* object) {
    return anchor_ptr<T>(detail::dirty_raw_pointer<T>{object, nullptr});
}

template<typename T>
anchor_ptr<T> raw_anchor_ptr(T* object, bool& dirty_flag) {
    return anchor_ptr<T>(detail::dirty_raw_pointer<T>{object, &dirty_flag});
}

} // namespace extpp

#endif // EXTPP_ANCHOR_PTR_HPP
