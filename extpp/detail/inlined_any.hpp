#ifndef EXTPP_DETAIL_INLINED_ANY_HPP
#define EXTPP_DETAIL_INLINED_ANY_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/type_traits.hpp>

#include <new>
#include <stdexcept>
#include <type_traits>
#include <typeinfo>

namespace extpp {
namespace detail {

struct inlined_any_base {};

struct inlined_any_vtable {
    const std::type_info& type;
    void (*destroy)(void* ptr) noexcept;
    void (*copy_construct)(const void* from, void* to);
    void (*move_construct)(void* from, void* to);
};

template<typename T>
struct inlined_any_vtable_impl {
    static void destroy(void* ptr) noexcept {
        static_cast<T*>(ptr)->~T();
    }

    static void copy_construct(const void* from, void* to) {
        new (to) T(*static_cast<const T*>(from));
    }

    static void move_construct(void* from, void* to) {
        new (to) T(std::move(*static_cast<T*>(from)));
    }
};

template<typename T>
inlined_any_vtable inlined_vtable_for = {
    typeid(T),
    inlined_any_vtable_impl<T>::destroy,
    inlined_any_vtable_impl<T>::copy_construct,
    inlined_any_vtable_impl<T>::move_construct
};

template<size_t Size>
class inlined_any : inlined_any_base {
public:
    inlined_any() = default;

    /// Constructs an `inlined_any` that stored the given value.
    /// \post `has_value()`.
    template<typename T, DisableSelf<detail::inlined_any_base, T>* = nullptr>
    inlined_any(T&& value) {
        construct<std::decay_t<T>>(std::forward<T>(value));
    }

    inlined_any(const inlined_any& other) {
        if (other.has_value())
            copy_construct(other.m_vtable, &other.m_storage);
    }

    inlined_any(inlined_any&& other) {
        if (other.has_value())
            move_construct(other.m_vtable, &other.m_storage);
    }

    ~inlined_any() {
        reset();
    }

    /// Replaces the contained object with the provided value.
    template<typename T, DisableSelf<detail::inlined_any_base, T>* = nullptr>
    inlined_any& operator=(T&& value) {
        reset();
        construct<std::decay_t<T>>(std::forward<T>(value));
        return *this;
    }

    inlined_any& operator=(const inlined_any& other) {
        if (this != &other) {
            reset();
            if (other.has_value())
                copy_construct(other.m_vtable, &other.m_storage);
        }
        return *this;
    }

    inlined_any& operator=(inlined_any&& other) {
        if (this != &other) {
            reset();
            if (other.has_value())
                move_construct(other.m_vtable, &other.m_storage);
        }
        return *this;
    }

    /// Constructs a new object of type `std::decay_t<T>` and destroys any previous object.
    /// Returns a reference to the new object.
    template<typename T, DisableSelf<detail::inlined_any_base, T>* = nullptr, typename... Args>
    std::decay_t<T>& emplace(Args&&... args) {
        reset();
        return *construct<std::decay_t<T>>(std::forward<Args>(args)...);
    }

    /// Resets this instance into the empty state.
    /// \post `!has_value()`.
    void reset() {
        if (has_value())
            destroy();
    }

    /// Returns true if this instance contains an object.
    bool has_value() const {
        return m_vtable;
    }

    /// Returns the typeinfo of the contained object, or `typeid(void)`
    /// if there is no object.
    const std::type_info& type() const {
        return m_vtable ? m_vtable->type : typeid(void);
    }

    template<typename T>
    std::decay_t<T>& get() & { return inner_ref<std::decay_t<T>>(*this); }

    template<typename T>
    const std::decay_t<T>& get() const& { return inner_ref<std::decay_t<T>>(*this); }

    template<typename T>
    std::decay_t<T>&& get() && { return std::move(inner_ref<std::decay_t<T>>(*this)); }

private:
    template<typename T, typename... Args>
    auto* construct(Args&&... args) {
        static_assert(sizeof(T) <= Size,
                      "The object does not fit into the buffer. "
                      "You should increase the inlined size or use a smaller type instead.");
        static_assert(alignof(T) <= alignof(m_storage),
                      "The object's alignment is too large.");

        EXTPP_ASSERT(!m_vtable, "instance must be empty.");
        T* result = new (&m_storage) T(std::forward<Args>(args)...);
        m_vtable = &detail::inlined_vtable_for<T>;
        return result;
    }

    void copy_construct(detail::inlined_any_vtable* vtable, const void* from) {
        EXTPP_ASSERT(!m_vtable, "instance must be empty.");
        EXTPP_ASSERT(vtable, "new vtable is null");

        vtable->copy_construct(from, &m_storage);
        m_vtable = vtable;
    }

    void move_construct(detail::inlined_any_vtable* vtable, void* from) {
        EXTPP_ASSERT(!m_vtable, "instance must be empty.");
        EXTPP_ASSERT(vtable, "new vtable is null");

        vtable->move_construct(from, &m_storage);
        m_vtable = vtable;
    }

    void destroy() noexcept {
        m_vtable->destroy(&m_storage);
        m_vtable = nullptr;
    }

    template<typename T, typename Any>
    auto* inner(Any& self) {
        return (self.has_value() && self.m_vtable->type == typeid(T))
                ? const_pointer_cast<T>(const_pointer_cast<void>(&self.m_storage))
                : nullptr;
    }

    template<typename T, typename Any>
    auto& inner_ref(Any& self) {
        auto ptr = inner<T>(self);
        if (!ptr) {
            // TODO Own exception?
            throw std::logic_error("the any object does not store an object of this type.");
        }
        return *ptr;
    }

private:
    detail::inlined_any_vtable* m_vtable = nullptr;
    std::aligned_storage_t<Size> m_storage;
};

} // namespace detail
} // namespace name

#endif // EXTPP_DETAIL_INLINED_ANY_HPP
