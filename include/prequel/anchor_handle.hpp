#ifndef PREQUEL_ANCHOR_HANDLE_HPP
#define PREQUEL_ANCHOR_HANDLE_HPP

#include <prequel/assert.hpp>
#include <prequel/defs.hpp>
#include <prequel/type_traits.hpp>

#include <memory>
#include <utility>

namespace prequel {

/// Dirty flag for anchors.
class anchor_flag {
public:
    anchor_flag() = default;

    // Other parts of the application take the address of this flag.
    // It should not move in memory.
    anchor_flag(const anchor_flag&) = delete;
    anchor_flag& operator=(const anchor_flag&) = delete;

    explicit operator bool() const { return m_changed; }
    bool changed() const { return m_changed; }

    void operator()() { set(true); }
    void set(bool changed = true) { m_changed = changed; }
    void reset() { set(false); }

private:
    bool m_changed = false;
};

template<typename Anchor>
class anchor_handle {
public:
    using anchor_type = Anchor;

public:
    /// Constructs an invalid anchor handle.
    anchor_handle() = default;

    /// Constructs an anchor handle that does not point to a dirty flag.
    anchor_handle(Anchor& anchor)
        : anchor_handle(anchor, nullptr) {}

    /// Constructs an anchor handle that references a dirty flag.
    /// The flag will be set to "changed" when the anchor was modified through
    /// the anchor or one of its aliasing children.
    anchor_handle(Anchor& anchor, anchor_flag& changed)
        : anchor_handle(anchor, &changed) {}

    anchor_handle(Anchor& anchor, anchor_flag* changed);

public:
    /// Returns the anchor's value.
    Anchor get() const {
        check_valid();
        return *m_anchor;
    }

    /// Sets the anchor's value.
    void set(const Anchor& value) const {
        check_valid();
        *m_anchor = value;
        set_changed();
    }

    /// Returns the anchor's member value specified by the given member data pointer.
    template<auto MemberPtr>
    member_type_t<decltype(MemberPtr)> get() const {
        static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                      "The member pointer must belong to this type.");
        check_valid();
        return m_anchor->*MemberPtr;
    }

    /// Sets the anchor's member value specified by the given member data pointer.
    template<auto MemberPtr>
    void set(const member_type_t<decltype(MemberPtr)>& value) const {
        static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                      "The member pointer must belong to this type.");
        check_valid();
        m_anchor->*MemberPtr = value;
        set_changed();
    }

    /// Returns the member of the anchor wrapped into an anchor handle.
    template<auto MemberPtr>
    anchor_handle<member_type_t<decltype(MemberPtr)>> member() const {
        static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, Anchor>,
                      "The member pointer must belong to this type.");
        check_valid();
        member_type_t<decltype(MemberPtr)>* m = std::addressof(m_anchor->*MemberPtr);
        return anchor_handle<member_type_t<decltype(MemberPtr)>>(*m, m_flag);
    }

    /// Returns a handle to some child object of the current anchor object.
    /// The caller *must* ensure that child really is a child (direct or indirect)
    /// of the current object.
    template<typename Child>
    anchor_handle<Child> child(Child& ch) const {
        check_valid();
        return anchor_handle<Child>(ch, m_flag);
    }

    bool valid() const { return m_anchor != nullptr; }
    explicit operator bool() const { return valid(); }

private:
    template<typename A>
    friend class anchor_handle;

    void check_valid() const {
        PREQUEL_ASSERT(valid(), "Invalid handle."); // TODO: Exception?
    }

    void set_changed() const {
        if (m_flag)
            m_flag->set(true);
    }

private:
    Anchor* m_anchor = nullptr;
    anchor_flag* m_flag = nullptr;
};

template<typename Anchor>
anchor_handle(Anchor& anchor)->anchor_handle<Anchor>;

// FIXME remove factory functions.
template<typename Anchor>
anchor_handle<Anchor> make_anchor_handle(Anchor& anchor) {
    return anchor_handle<Anchor>(anchor);
}

template<typename Anchor>
anchor_handle<Anchor> make_anchor_handle(Anchor& anchor, anchor_flag& changed) {
    return anchor_handle<Anchor>(anchor, changed);
}

template<typename Anchor>
anchor_handle<Anchor> make_anchor_handle(Anchor& anchor, anchor_flag* changed) {
    return anchor_handle<Anchor>(anchor, changed);
}

template<typename Anchor>
anchor_handle<Anchor>::anchor_handle(Anchor& anchor, anchor_flag* changed)
    : m_anchor(std::addressof(anchor))
    , m_flag(changed) {}

} // namespace prequel

#endif // PREQUEL_ANCHOR_HANDLE_HPP
