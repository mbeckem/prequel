#ifndef EXTPP_ANCHOR_HANDLE_HPP
#define EXTPP_ANCHOR_HANDLE_HPP

#include <extpp/defs.hpp>
#include <extpp/assert.hpp>
#include <extpp/type_traits.hpp>

#include <memory>
#include <utility>

namespace extpp {

class anchor_storage_base {
public:
    void set_changed(bool changed) { if (m_changed != changed) m_changed = changed; }
    bool changed() const { return m_changed; }

private:
    bool m_changed = false;
};

template<typename Anchor>
class anchor_storage : public anchor_storage_base {
public:
    anchor_storage(const Anchor& anchor)
        : m_anchor(anchor)
    {}

    anchor_storage(const anchor_storage&) = delete;
    anchor_storage& operator=(const anchor_storage&) = delete;

    Anchor* get_anchor() { return std::addressof(m_anchor); }

private:
    Anchor m_anchor;
};

template<typename Anchor>
class anchor_handle {
public:
    using anchor_type = Anchor;

public:
    anchor_handle() = default;
    explicit anchor_handle(const Anchor& anchor);

    anchor_handle(const anchor_handle& other);
    anchor_handle(anchor_handle&& other) noexcept;

    anchor_handle& operator=(const anchor_handle& other);
    anchor_handle& operator=(anchor_handle&& other) noexcept;

public:
    /// Returns the anchor's value.
    Anchor get() const;

    /// Sets the anchor's value.
    void set(const Anchor& value) const;

    /// Returns anchor's member value specified by the given member data pointer.
    template<auto MemberPtr>
    member_type_t<decltype(MemberPtr)> get() const;

    /// Sets the anchor's member value specified by the given member data pointer.
    template<auto MemberPtr>
    void set(const member_type_t<decltype(MemberPtr)>& value) const;

    /// Returns the member of the anchor wrapped into an anchor handle.
    template<auto MemberPtr>
    anchor_handle<member_type_t<decltype(MemberPtr)>> member() const;

    bool valid() const { return m_anchor != nullptr; }
    explicit operator bool() const { return valid(); }

    /// Returns true if the anchor value has changed since the last call to `reset_changed()`.
    bool changed() const {
        check_valid();
        return m_base->changed();
    }

    /// Resets the changed flag of the anchor value.
    void reset_changed() const {
        check_valid();
        m_base->set_changed(false);
    }

private:
    template<typename Other>
    friend class anchor_handle;

    anchor_handle(std::shared_ptr<anchor_storage_base>&& base, Anchor* anchor);

    void check_valid() const;

private:
    std::shared_ptr<anchor_storage_base> m_base; // For memory management + changed flag
    Anchor* m_anchor = nullptr;
};

template<typename Anchor>
anchor_handle<Anchor> make_anchor_handle(const Anchor& anchor) {
    return anchor_handle<Anchor>(anchor);
}

template<typename Anchor>
anchor_handle<Anchor>::anchor_handle(const Anchor& anchor) {
    auto base = std::make_shared<anchor_storage<Anchor>>(anchor);
    m_anchor = base->get_anchor();
    m_base = std::move(base); // Erases concrete type.
}

template<typename Anchor>
anchor_handle<Anchor>::anchor_handle(const anchor_handle& other)
    : m_base(other.m_base)
    , m_anchor(other.m_anchor)
{}

template<typename Anchor>
anchor_handle<Anchor>::anchor_handle(anchor_handle&& other) noexcept
    : m_base(std::move(other.m_base))
    , m_anchor(std::exchange(other.m_anchor, nullptr))
{}

template<typename Anchor>
anchor_handle<Anchor>& anchor_handle<Anchor>::operator=(const anchor_handle& other) {
    m_base = other.m_base;
    m_anchor = other.m_anchor;
    return *this;
}

template<typename Anchor>
anchor_handle<Anchor>& anchor_handle<Anchor>::operator=(anchor_handle&& other) noexcept {
    if (this != &other) {
        m_base = std::move(other.m_base);
        m_anchor = std::exchange(other.m_anchor, nullptr);
    }
    return *this;
}

template<typename Anchor>
Anchor anchor_handle<Anchor>::get() const {
    check_valid();
    return *m_anchor;
}

template<typename Anchor>
void anchor_handle<Anchor>::set(const Anchor& value) const {
    check_valid();
    *m_anchor = value;
    m_base->set_changed(true);
}

template<typename Anchor>
template<auto MemberPtr>
member_type_t<decltype(MemberPtr)> anchor_handle<Anchor>::get() const {
    static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                  "The member pointer must belong to this type.");
    check_valid();
    return m_anchor->*MemberPtr;
}

template<typename Anchor>
template<auto MemberPtr>
void anchor_handle<Anchor>::set(const member_type_t<decltype(MemberPtr)>& value) const {
   static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                 "The member pointer must belong to this type.");
    check_valid();
    m_anchor->*MemberPtr = value;
    m_base->set_changed(true);
}

template<typename Anchor>
template<auto MemberPtr>
anchor_handle<member_type_t<decltype(MemberPtr)>> anchor_handle<Anchor>::member() const {
    static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                  "The member pointer must belong to this type.");
    check_valid();

    member_type_t<decltype(MemberPtr)>* member = std::addressof(m_anchor->*MemberPtr);
    auto member_base = m_base;
    return {std::move(member_base), member};
}

template<typename Anchor>
anchor_handle<Anchor>::anchor_handle(std::shared_ptr<anchor_storage_base>&& base, Anchor* anchor)
    : m_base(std::move(base))
    , m_anchor(anchor)
{}

template<typename Anchor>
void anchor_handle<Anchor>::check_valid() const {
    EXTPP_ASSERT(valid(), "Invalid handle."); // TODO: Exception?
}

} // namespace extpp

#endif // EXTPP_ANCHOR_HANDLE_HPP
