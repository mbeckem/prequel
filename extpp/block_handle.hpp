#ifndef EXTPP_BLOCK_HANDLE_HPP
#define EXTPP_BLOCK_HANDLE_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/serialization.hpp>

#include <utility>

namespace extpp {

namespace detail {

// Note: This part can be made more efficient by
// using vtable + void pointers. For mmap, the void pointer
// can be the mapped pointer itself.
class block_handle_impl {
public:
    block_handle_impl() = default;

    // TODO: Move into this class. Remains constant anyway.
    virtual u64 index() const noexcept = 0;

    virtual const byte* data() const noexcept = 0;

    virtual byte* writable_data() = 0;

    virtual u32 block_size() const noexcept = 0;

    // Virtual copy & destroy so that refcount increment and decrement
    // is a valid implementation of this interface.
    virtual block_handle_impl* copy() = 0;
    virtual void destroy() = 0;

    block_handle_impl(const block_handle_impl&) = delete;
    block_handle_impl& operator=(const block_handle_impl&) = delete;

protected:
    // Call destroy() from the outside.
    ~block_handle_impl() = default;
};

} // namespace detail

/// A block handle is a (possibly invalid) reference to a block
/// loaded into memory by the block engine.
/// The handle gives access to the block's raw data and it's dirty flag.
class block_handle {
public:
    /// Constructs an invalid handle.
    block_handle() = default;

    /// Constructor used by the block engine.
    /// The handle object takes ownership of the pointer.
    /// \pre `base != nullptr`.
    block_handle(detail::block_handle_impl* base)
        : m_impl(base)
    {
        EXTPP_ASSERT(base, "Null pointer.");
    }

    block_handle(const block_handle& other)
        : m_impl(other.m_impl ? other.m_impl->copy() : nullptr)
    {}

    block_handle(block_handle&& other) noexcept
        : m_impl(std::exchange(other.m_impl, nullptr))
    {}

    ~block_handle() {
        if (m_impl)
            m_impl->destroy();
    }

    block_handle& operator=(const block_handle& other) {
        block_handle hnd(other);
        *this = std::move(hnd);
        return *this;
    }

    block_handle& operator=(block_handle&& other) noexcept {
        if (this != &other) {
            if (m_impl)
                m_impl->destroy();
            m_impl = std::exchange(other.m_impl, nullptr);
        }
        return *this;
    }

    /// @{
    /// Returns true if this handle is valid, i.e. if it references a block.
    bool valid() const noexcept { return m_impl; }
    explicit operator bool() const noexcept { return valid(); }
    /// @}

    /// Returns the index of this block in the underlying storage engine.
    block_index index() const noexcept {
        return valid() ? block_index(m_impl->index()) : block_index();
    }

    /// Returns the block's size.
    /// \pre `valid()`.
    u32 block_size() const noexcept {
        check_valid();
        return m_impl->block_size();
    }

    /// Returns the address of this block on disk.
    /// The address points to the first byte of the block.
    raw_address address() const noexcept {
        check_valid();
        return raw_address::block_address(index(), block_size());
    }

    /// Returns a pointer to the block's data.
    /// The data array is exactly block_size() bytes long.
    ///
    /// \warning The block's data array may be moved after a call to
    /// writable_data(), which will invalidate the previous data pointers.
    /// *Do not store this value*.
    ///
    /// \pre `valid()`.
    const byte* data() const noexcept {
        check_valid();
        return m_impl->data();
    }

    /// Returns a pointer to the block's data.
    /// The data array is exactly block_size() bytes long.
    ///
    /// \warning Invalidates earlier `data()`-pointers, because
    /// the storage of the block might be moved.
    ///
    /// \pre `valid()`.
    byte* writable_data() const {
        check_valid();
        return m_impl->writable_data();
    }

    template<typename T>
    void get(u32 offset, T& value) const {
        EXTPP_ASSERT(check_range(offset, serialized_size<T>()),
                     "Reading out of bounds.");

        deserialize(value, data() + offset);
    }

    template<typename T>
    T get(u32 offset) const {
        T value;
        get(offset, value);
        return value;
    }

    template<typename T>
    void set(u32 offset, const T& value) const {
        EXTPP_ASSERT(check_range(offset, serialized_size<T>()),
                     "Writing out of bounds.");
        serialize(value, writable_data() + offset);
    }

    void write(u32 offset, const void* data, u32 size) const {
        EXTPP_ASSERT(check_range(offset, size), "Writing out of bounds.");
        std::memmove(writable_data() + offset, data, size);
    }

    void read(u32 offset, void* data, u32 size) const {
        EXTPP_ASSERT(check_range(offset, size), "Reading out of bounds.");
        std::memmove(data, this->data() + offset, size);
    }

private:
    void check_valid() const {
        EXTPP_ASSERT(valid(), "Invalid instance.");
    }

    bool check_range(u32 offset, u32 size) const {
        check_valid();
        return offset <= block_size() && size <= block_size() - offset;
    }

private:
    detail::block_handle_impl* m_impl = nullptr;
};

} // namespace extpp

#endif // EXTPP_BLOCK_HANDLE_HPP
