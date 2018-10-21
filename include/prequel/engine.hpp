#ifndef PREQUEL_ENGINE_HPP
#define PREQUEL_ENGINE_HPP

#include <prequel/address.hpp>
#include <prequel/assert.hpp>
#include <prequel/defs.hpp>
#include <prequel/math.hpp>
#include <prequel/serialization.hpp>

#include <utility>

namespace prequel {

namespace detail {

// Note: This part can be made more efficient by
// using vtable + void pointers. For mmap, the void pointer
// can be the mapped pointer itself.
class block_handle_impl {
public:
    block_handle_impl(const block_handle_impl&) = delete;
    block_handle_impl& operator=(const block_handle_impl&) = delete;

    virtual void handle_ref() = 0;

    virtual void handle_unref() = 0;

    virtual u64 index() const noexcept = 0;

    virtual const byte* data() const noexcept = 0;

    // Must either throw an exception or update this instance in such a way
    // that the data pointer can be written to.
    // Making a block writable might involve moving the block in memory to
    // protect existing readers from side effects.
    virtual byte* writable_data() = 0;

protected:
    block_handle_impl() = default;

    // Call handle_unref() from the outside. Nonvirtual!
    ~block_handle_impl() = default;
};

} // namespace detail

/// A block handle is a reference to a block that has been oaded into memory
/// by an engine. The handle gives access to the block's raw data.
/// A block will not be evicted from main memory while it is being referenced
/// by at least one block handle.
///
/// \note Block handles can be invalid if they were default constructed or have been moved.
class block_handle {
public:
    /// Constructs an invalid handle.
    block_handle() = default;

    /// Constructor used by the block engine.
    /// The handle object takes ownership of the pointer.
    /// \pre `base != nullptr`.
    block_handle(engine* eng, prequel::detail::block_handle_impl* base)
        : m_engine(eng)
        , m_impl(base)
    {
        PREQUEL_ASSERT(eng, "Null engine.");
        PREQUEL_ASSERT(base, "Null implementation.");
        base->handle_ref();
    }

    block_handle(const block_handle& other)
        : m_engine(other.m_engine)
        , m_impl(other.m_impl)
    {
        if (m_impl)
            m_impl->handle_ref();
    }

    block_handle(block_handle&& other) noexcept
        : m_engine(std::exchange(other.m_engine, nullptr))
        , m_impl(std::exchange(other.m_impl, nullptr))
    {}

    ~block_handle() {
        if (m_impl)
            m_impl->handle_unref();
    }

    block_handle& operator=(const block_handle& other) {
        block_handle hnd(other);
        *this = std::move(hnd);
        return *this;
    }

    block_handle& operator=(block_handle&& other) noexcept {
        if (this != &other) {
            if (m_impl)
                m_impl->handle_unref();
            m_engine = std::exchange(other.m_engine, nullptr);
            m_impl = std::exchange(other.m_impl, nullptr);
        }
        return *this;
    }

    /// @{
    /// Returns true if this handle is valid, i.e. if it references a block.
    bool valid() const noexcept { return m_impl; }
    explicit operator bool() const noexcept { return valid(); }
    /// @}

    /// Returns a reference to the block's engine.
    /// \pre `valid()`.
    engine& get_engine() const noexcept {
        check_valid();
        return *m_engine;
    }

    /// Returns the index of this block in the underlying storage engine.
    block_index index() const noexcept {
        return valid() ? block_index(m_impl->index()) : block_index();
    }

    /// Returns the block's size.
    /// \pre `valid()`.
    u32 block_size() const noexcept;

    /// Returns the address of this block on disk.
    /// The address points to the first byte of the block.
    raw_address address() const noexcept;

    /// Returns the address to the given offset in this block.
    /// \pre `offset_in_block < block_size()`.
    raw_address address(u32 offset_in_block) const noexcept;

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

    /// Deserialize a value of type T at the given offset.
    template<typename T>
    void get(u32 offset, T& value) const {
        PREQUEL_ASSERT(check_range(offset, serialized_size<T>()),
                     "Reading out of bounds.");

        deserialize(value, data() + offset);
    }

    /// Deserialize a value of type T at the given offset.
    template<typename T>
    T get(u32 offset) const {
        T value;
        get(offset, value);
        return value;
    }

    /// Serialize a value and put it at the given offset.
    template<typename T>
    void set(u32 offset, const T& value) const {
        PREQUEL_ASSERT(check_range(offset, serialized_size<T>()),
                     "Writing out of bounds.");
        serialize(value, writable_data() + offset);
    }

    /// Write a raw byte array of the given size to the provided offset.
    void write(u32 offset, const void* data, u32 size) const {
        PREQUEL_ASSERT(check_range(offset, size), "Writing out of bounds.");
        std::memmove(writable_data() + offset, data, size);
    }

    /// Read `size` bytes at the given offset into the `data` array.
    void read(u32 offset, void* data, u32 size) const {
        PREQUEL_ASSERT(check_range(offset, size), "Reading out of bounds.");
        std::memmove(data, this->data() + offset, size);
    }

private:
    void check_valid() const {
        PREQUEL_ASSERT(valid(), "Invalid instance.");
    }

    bool check_range(u32 offset, u32 size) const {
        check_valid();
        return offset <= block_size() && size <= block_size() - offset;
    }

private:
    engine* m_engine = nullptr;
    detail::block_handle_impl* m_impl = nullptr;
};

/**
 * Provides block-oriented access to the contents of a file.
 * This class defines the interface for multiple engine implementations.
 */
class engine {
protected:
    explicit engine(u32 block_size)
        : m_block_size(block_size)
    {
        if (!is_pow2(block_size)) {
            PREQUEL_THROW(bad_argument(fmt::format("Block size is not a power of two: {}.", block_size)));
        }
        m_block_size_log = log2(block_size);
        m_offset_mask = m_block_size - 1;
    }

public:
    virtual ~engine() = default;

    /// Returns the size (in bytes) of every block returned by this engine.
    u32 block_size() const noexcept { return m_block_size; }

    /// Returns the size of the underlying storage, in blocks.
    /// All block indices in `[0, size())` are valid for
    /// I/O operations.
    u64 size() const { return do_size(); }

    /// Grows the underyling storage by `n` blocks.
    void grow(u64 n) {
        if (n > 0) {
            return do_grow(n);
        }
    }

    /// Returns a handle to the given block if it already loaded into main memory.
    /// Otherwise returns an invalid handle.
    block_handle access(block_index index) {
        PREQUEL_CHECK(index, "Invalid index.");
        return do_access(index);
    }

    /// Reads the block at the given index and returns a handle to it.
    /// Throws if an I/O error occurs.
    block_handle read(block_index index) {
        PREQUEL_CHECK(index, "Invalid index.");
        return do_read(index);
    }

    /// Similar to `read()`, but the block is zeroed instead.
    /// This can save a read operation if the block is not already in memory.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// Throws if an I/O error occurs.
    block_handle overwrite_zero(block_index index) {
        PREQUEL_CHECK(index, "Invalid index.");
        return do_overwrite_zero(index);
    }

    /// Like `zeroed()`, but instead sets the content of the block to `data`.
    ///
    /// Throws if an I/O error occurs.
    ///
    /// \warning `data` must be a pointer to (at least) `block_size()` bytes.
    block_handle overwrite(block_index index, const byte* data, size_t data_size) {
        PREQUEL_CHECK(index, "Invalid index.");
        PREQUEL_CHECK(data_size >= block_size(), "Not enough data.");
        return do_overwrite(index, data);
    }

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    void flush() {
        do_flush();
    }

    /// Returns the address to the first byte of the given block.
    /// Returns the invalid address if the block index is invalid.
    raw_address to_address(block_index index) const noexcept {
        return to_address(index, 0);
    }

    /// Returns the address to the given byte offset in the given block.
    /// Returns the invalid address if the block index is invalid.
    ///
    /// \pre `offset_in_block < block_size()`.
    raw_address to_address(block_index index, u32 offset_in_block) const noexcept {
        PREQUEL_ASSERT(offset_in_block < block_size(), "Offset out of bounds.");
        if (!index) {
            return raw_address();
        }
        return raw_address((index.value() << m_block_size_log) | offset_in_block);
    }

    /// Takes the raw_address of a byte on disk and returns the block index of the containing block.
    /// If `addr` is invalid, an invalid block index will be returned.
    block_index to_block(raw_address addr) const noexcept {
        if (!addr) {
            return block_index();
        }
        u64 index = addr.value() >> m_block_size_log;
        return block_index(index);
    }

    /// Takes the raw address of a byte on disk and returns the offset of the byte in it's containing block.
    /// Returns 0 if the the address is invalid.
    u32 to_offset(raw_address addr) const noexcept {
        if (!addr) {
            return 0;
        }
        return u32(addr.value()) & m_offset_mask;
    }

    /// Converts the block count to a number of bytes.
    u64 to_byte_size(u64 block_count) const noexcept {
        return block_count << m_block_size_log;
    }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

protected:
    virtual u64 do_size() const = 0;
    virtual void do_grow(u64 n) = 0;
    virtual block_handle do_access(block_index index) = 0;
    virtual block_handle do_read(block_index index) = 0;
    virtual block_handle do_overwrite_zero(block_index index) = 0;
    virtual block_handle do_overwrite(block_index index, const byte* data) = 0;
    virtual void do_flush() = 0;

    // log_2 (block_size)
    u32 block_size_log() const { return m_block_size_log; }

    // bitwise AND with byte offset -> block offset (cuts of high bits).
    u32 offset_mask() const { return m_offset_mask; }

private:
    u32 m_block_size;
    u32 m_block_size_log;
    u32 m_offset_mask;
};

inline u32 block_handle::block_size() const noexcept {
    return get_engine().block_size();
}

inline raw_address block_handle::address() const noexcept {
    if (!valid())
        return {};

    return get_engine().to_address(index());
}

inline raw_address block_handle::address(u32 offset_in_block) const noexcept {
    if (!valid())
        return {};

    PREQUEL_ASSERT(offset_in_block < block_size(), "Invalid offset in block.");
    return get_engine().to_address(index(), offset_in_block);
}

} // namespace prequel

#endif // PREQUEL_ENGINE_HPP
