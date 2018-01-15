#ifndef EXTPP_HANDLE_HPP
#define EXTPP_HANDLE_HPP

#include <extpp/address.hpp>
#include <extpp/block_handle.hpp>
#include <extpp/engine.hpp>
#include <extpp/serialization.hpp>
#include <extpp/type_traits.hpp>

namespace extpp {

/// A handle refers to a serialized object on disk that has been loaded
/// into main memory. The handle can read and write the whole object
/// or parts of it (i.e. single structure members).
///
/// The block that contains the object is pinned in main memory
/// for as long as a handle points to it.
template<typename T>
class handle {
public:
    using element_type = T;

public:
    /// Constructs an invalid handle.
    handle() = default;

    /// Constructs a handle to an object within the given block,
    /// located at `offset`.
    handle(block_handle block, u32 offset) noexcept
        : m_block(std::move(block))
        , m_offset(offset)
    {
        EXTPP_ASSERT(m_block || offset == 0,
                     "Offset must be zero for invalid blocks.");
        EXTPP_ASSERT(!m_block || (m_offset <= m_block.block_size() &&
                                  serialized_size<T>() <= m_block.block_size() - m_offset),
                     "Offset out of bounds.");
    }

    handle(const handle&) = default;
    handle& operator=(const handle&) = default;

    handle(handle&& other) noexcept
        : m_block(std::move(other.m_block))
        , m_offset(std::exchange(other.m_offset, 0))
    {}

    handle& operator=(handle&& other) noexcept {
        if (this != &other) {
            m_block = std::move(other.m_block);
            m_offset = std::exchange(other.m_offset, 0);
        }
        return *this;
    }

    void reset(block_handle h, u32 offset) noexcept {
        *this = handle(std::move(h), offset);
    }

    void reset() noexcept {
        *this = handle();
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

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    bool valid() const { return m_block.valid(); }

    /// Returns true if this instance points to a valid value.
    /// Default-constructed pointers and moved-from pointers are invalid.
    explicit operator bool() const { return valid(); }

private:
    block_handle m_block;
    u32 m_offset = 0;
};

template<typename T, typename U>
bool operator==(const handle<T>& lhs, const handle<U>& rhs) {
    return lhs.address() == rhs.address();
}

template<typename T, typename U>
bool operator!=(const handle<T>& lhs, const handle<U>& rhs) {
    return lhs.address() != rhs.address();
}

/// Cast the block handle to a specific type via reinterpret_cast.
template<typename T>
handle<T> cast(block_handle block, u32 offset = 0) {
    return handle<T>(std::move(block), offset);
}

///// Constructs a new object of type T in the given block.
///// Invokes the constructor of T and passes the provided arguments.
///// The block will be marked as dirty.
//// TODO: Offset
//template<typename T, typename... Args>
//handle<T> construct(block_handle block, u32 offset = 0, Args&&... args) {
//    EXTPP_ASSERT(sizeof(T) <= block.block_size(), "Type does not fit into a block.");

//    T* ptr = new(block.data()) T(std::forward<Args>(args)...);
//    block.dirty();
//    return handle<T>(std::move(block), ptr);
//}

//template<typename T>
//handle<T> access(engine& e, address<T> addr) {
//    EXTPP_ASSERT(addr, "Accessing an invalid address.");

//    auto block = e.read(addr.raw().get_block_index(e.block_size()));
//    u32 offset = addr.raw().get_offset_in_block(e.block_size());
//    return handle<T>(std::move(block), offset);
//}

} // namespace extpp

#endif // EXTPP_HANDLE_HPP
