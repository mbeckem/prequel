#ifndef EXTPP_HANDLE_HPP
#define EXTPP_HANDLE_HPP

#include <extpp/address.hpp>
#include <extpp/engine.hpp>
#include <extpp/serialization.hpp>
#include <extpp/type_traits.hpp>

namespace extpp {

/// Common base class of all handle<T> instances.
class handle_base {
public:
    /// Constructs an invalid handle.
    handle_base() = default;

    /// Constructs a handle to an object within the given block,
    /// located at `offset`.
    handle_base(block_handle block, u32 offset) noexcept
        : m_block(std::move(block))
        , m_offset(offset)
    {
        EXTPP_ASSERT(m_block || m_offset == 0,
                     "Offset must be zero for invalid blocks.");
        EXTPP_ASSERT(m_offset <= m_block.block_size(),
                     "Offset out of bounds.");
    }

    handle_base(const handle_base&) = default;
    handle_base& operator=(const handle_base&) = default;

    handle_base(handle_base&& other) noexcept
        : m_block(std::move(other.m_block))
        , m_offset(std::exchange(other.m_offset, 0))
    {}

    handle_base& operator=(handle_base&& other) noexcept {
        if (this != &other) {
            m_block = std::move(other.m_block);
            m_offset = std::exchange(other.m_offset, 0);
        }
        return *this;
    }

    void reset(block_handle h, u32 offset) noexcept {
        m_block = std::move(h);
        m_offset = offset;
    }

    void reset() noexcept {
        m_block = block_handle();
        m_offset = 0;
    }

    /// The handle to the block that contains this value.
    const block_handle& block() const & { return m_block; }
    block_handle&& block() && { return std::move(m_block); }

    /// The offset of this handle's value in its block.
    u32 offset() const { return m_offset; }

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    bool valid() const { return m_block.valid(); }

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    explicit operator bool() const { return valid(); }

    friend bool operator==(const handle_base& lhs, const handle_base& rhs) {
        return lhs.m_block.index() == rhs.m_block.index() && lhs.m_offset == rhs.m_offset;
    }

    friend bool operator!=(const handle_base& lhs, const handle_base& rhs) {
        return lhs.m_block.index() != rhs.m_block.index() || lhs.m_offset != rhs.m_offset;
    }

protected:
    block_handle m_block;
    u32 m_offset = 0;
};

/// A handle refers to a serialized object on disk that has been loaded
/// into main memory. The handle can read and write the whole object
/// or parts of it (i.e. single structure members).
///
/// The block that contains the object is pinned in main memory
/// for as long as a handle points to it.
template<typename T>
class handle : public handle_base {
public:
    using element_type = T;

public:
    handle() = default;

    handle(block_handle block, u32 offset)
        : handle_base(std::move(block), offset)
    {
        EXTPP_ASSERT((!m_block || serialized_size<T>() <= m_block.block_size() - m_offset),
                     "Offset out of bounds.");
    }

    template<auto MemberPtr>
    handle<member_type_t<decltype(MemberPtr)>> member() const {
        static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, T>,
                      "The member pointer must belong to this type.");
        EXTPP_ASSERT(valid(), "Invalid handle.");
        return {block(), m_offset + static_cast<u32>(serialized_offset<MemberPtr>())};
    }

    /// Returns the address of this object on disk.
    extpp::address<T> address() const {
        if (!valid())
            return {};

        raw_address raw = m_block.address();
        raw += m_offset;
        return raw_address_cast<T>(raw);
    }

    /// Returns a reference to the block engine.
    /// \pre `*this`.
    engine& get_engine() const {
        EXTPP_ASSERT(*this, "invalid handle.");
        return m_block.get_engine();
    }

    /// Construct a new T by passing all provided arguments to it,
    /// then overwrite the existing content of the handle with the serialized
    /// representation of the new instance.
    template<typename... Args>
    void construct(Args&& ...args) {
        EXTPP_ASSERT(valid(), "invalid handle.");
        set(T(std::forward<Args>(args)...));
    }

    /// Returns the current value of the serialized object.
    T get() const {
        EXTPP_ASSERT(valid(), "invalid handle.");
        return m_block.get<T>(m_offset);
    }

    /// Returns the current value of the serialized object.
    void get(T& value) const {
        EXTPP_ASSERT(valid(), "invalid handle.");
        m_block.get(m_offset, value);
    }

    /// Updates the current value of the serialized object.
    void set(const T& value) const {
        EXTPP_ASSERT(valid(), "invalid handle.");
        serialize(value, m_block.writable_data() + m_offset);
    }

    template<auto MemberPtr>
    member_type_t<decltype(MemberPtr)> get() const {
        static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, T>,
                      "The member pointer must belong to this type.");
        EXTPP_ASSERT(valid(), "Invalid handle.");
        u32 offset = m_offset + serialized_offset<MemberPtr>();
        return m_block.get<member_type_t<decltype(MemberPtr)>>(offset);
    }

   template<auto MemberPtr>
   void set(const member_type_t<decltype(MemberPtr)>& value) const {
       static_assert(std::is_same_v<object_type_t<decltype(MemberPtr)>, T>,
                     "The member pointer must belong to this type.");
       EXTPP_ASSERT(valid(), "Invalid handle.");
       u32 offset = m_offset + serialized_offset<MemberPtr>();
       m_block.set<member_type_t<decltype(MemberPtr)>>(offset, value);
   }
};

/// Cast the block handle to a specific type. This cast is unchecked,
/// you must be sure that accessing the storage in this way is well defined.
template<typename T>
handle<T> cast(block_handle block, u32 offset = 0) {
    return handle<T>(std::move(block), offset);
}

} // namespace extpp

#endif // EXTPP_HANDLE_HPP
