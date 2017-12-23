#ifndef EXTPP_BLOCK_HANDLE_HPP
#define EXTPP_BLOCK_HANDLE_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>

#include <utility>

namespace extpp {
namespace detail {

class block_handle_impl {
public:
    block_handle_impl() = default;

    // TODO: Move into this class. Remains constant anyway.
    virtual u64 index() const noexcept = 0;

    virtual byte* data() const noexcept = 0;

    virtual u32 block_size() const noexcept = 0;

    virtual void dirty() = 0;

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
class new_block_handle {
public:
    /// Constructs an invalid handle.
    new_block_handle() = default;

    /// Constructor used by the block engine.
    /// The handle object takes ownership of the pointer.
    /// \pre `base != nullptr`.
    new_block_handle(detail::block_handle_impl* base)
        : m_impl(base) {
        EXTPP_ASSERT(base, "Null pointer.");
    }

    new_block_handle(const new_block_handle& other)
        : m_impl(other.m_impl ? other.m_impl->copy() : nullptr)
    {}

    new_block_handle(new_block_handle&& other) noexcept
        : m_impl(std::exchange(other.m_impl, nullptr))
    {}

    ~new_block_handle() {
        if (m_impl)
            m_impl->destroy();
    }


    new_block_handle& operator=(const new_block_handle& other) {
        new_block_handle hnd(other);
        *this = std::move(hnd);
        return *this;
    }

    new_block_handle& operator=(new_block_handle&& other) noexcept {
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
    /// \pre `valid()`.
    block_index index() const noexcept {
        check_valid();
        return block_index(m_impl->index());
    }

    /// Returns a pointer to the block's data.
    /// The data array is exactly block_size() bytes long.
    /// \pre `valid()`.
    byte* data() const noexcept {
        check_valid();
        return m_impl->data();
    }

    /// Returns the block's size.
    /// \pre `valid()`.
    u32 block_size() const noexcept {
        check_valid();
        return m_impl->block_size();
    }

    /// Mark this block as dirty. Blocks must be marked as dirty
    /// in order for the changes to be written back to disk.
    /// \pre `valid()`.
    void dirty() const {
        check_valid();
        return m_impl->dirty();
    }

private:
    void check_valid() const {
        EXTPP_ASSERT(valid(), "Invalid instance.");
    }

private:
    detail::block_handle_impl* m_impl = nullptr;
};

} // namespace extpp

#endif // EXTPP_BLOCK_HANDLE_HPP
