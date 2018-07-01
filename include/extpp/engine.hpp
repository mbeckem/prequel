#ifndef EXTPP_ENGINE_HPP
#define EXTPP_ENGINE_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/math.hpp>
#include <extpp/serialization.hpp>

#include <utility>

namespace extpp {

namespace detail {

// Note: This part can be made more efficient by
// using vtable + void pointers. For mmap, the void pointer
// can be the mapped pointer itself.
class block_handle_impl {
public:
    // Virtual copy & destroy so that refcount increment and decrement
    // is a valid implementation of this interface.
    virtual block_handle_impl* copy() = 0;
    virtual void destroy() = 0;

    block_handle_impl(const block_handle_impl&) = delete;
    block_handle_impl& operator=(const block_handle_impl&) = delete;

    u64 index() const noexcept { return m_index; }
    u32 block_size() const noexcept { return m_block_size; }

    const byte* data() const noexcept {
        EXTPP_ASSERT(m_data, "Data pointer was not initialized.");
        return m_data;
    }

    byte* writable_data() {
        if (!m_dirty) {
            make_dirty();
        }

        EXTPP_ASSERT(m_dirty, "The instance was not made writable.");
        EXTPP_ASSERT(m_data, "Data pointer was not initialized.");
        return m_data;
    }

protected:
    block_handle_impl() = default;

    // Call destroy() from the outside. Nonvirtual!
    ~block_handle_impl() = default;

    // Must either throw an exception or update this instance in such a way
    // that the data pointer can be written to.
    // Making a block writable might involve moving the block in memory to
    // protect existing readers from side effects.
    virtual void make_dirty() = 0;

public:
    /// The index of this block in the underlying file. Must be updated by the implementor.
    u64 m_index = 0;

    /// Size of m_data.
    u32 m_block_size = 0;

    /// True if the data can be written to.
    bool m_dirty = false;

    /// The location where this block's content resides in memory right now.
    /// Can change as a result of make_writable().
    byte* m_data = nullptr;
};

} // namespace detail

/// A block handle is a (possibly invalid) reference to a block
/// loaded into memory by the block engine.
/// The handle gives access to the block's raw data and it's dirty flag.
/// While a block is being referenced by at least one block handle,
/// it will not be evicted from main memory.
class block_handle {
public:
    /// Constructs an invalid handle.
    block_handle() = default;

    /// Constructor used by the block engine.
    /// The handle object takes ownership of the pointer.
    /// \pre `base != nullptr`.
    block_handle(engine* eng, extpp::detail::block_handle_impl* base)
        : m_engine(eng)
        , m_impl(base)
    {
        EXTPP_ASSERT(eng, "Null engine.");
        EXTPP_ASSERT(base, "Null implementation.");
    }

    block_handle(const block_handle& other)
        : m_engine(other.m_engine)
        , m_impl(other.m_impl ? other.m_impl->copy() : nullptr)
    {}

    block_handle(block_handle&& other) noexcept
        : m_engine(std::exchange(other.m_engine, nullptr))
        , m_impl(std::exchange(other.m_impl, nullptr))
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
        EXTPP_ASSERT(check_range(offset, serialized_size<T>()),
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
        EXTPP_ASSERT(check_range(offset, serialized_size<T>()),
                     "Writing out of bounds.");
        serialize(value, writable_data() + offset);
    }

    /// Write a raw byte array of the given size to the provided offset.
    void write(u32 offset, const void* data, u32 size) const {
        EXTPP_ASSERT(check_range(offset, size), "Writing out of bounds.");
        std::memmove(writable_data() + offset, data, size);
    }

    /// Read `size` bytes at the given offset into the `data` array.
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
    engine* m_engine = nullptr;
    detail::block_handle_impl* m_impl = nullptr;
};

class engine {
public:
    explicit engine(u32 block_size)
        : m_block_size(block_size)
    {
        if (!is_pow2(block_size)) {
            EXTPP_THROW(invalid_argument(fmt::format("Block size is not a power of two: {}.", block_size)));
        }
        m_block_size_log = log2(block_size);
        m_offset_mask = m_block_size - 1;
    }

    virtual ~engine() = default;

    /// Returns the size (in bytes) of every block returned by this engine.
    u32 block_size() const noexcept { return m_block_size; }

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
        EXTPP_ASSERT(offset_in_block < block_size(), "Offset out of bounds.");
        if (!index) {
            return raw_address();
        }
        return raw_address((index.value() << m_block_size_log) | offset_in_block);
    }

    /// Takes the raw_address of a byte on disk and returns the block index of the containing block.
    /// If `addr` is invalid, an invalid block index will be returned.
    block_index to_index(raw_address addr) const noexcept {
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

    /// Returns the size of the underlying storage, in blocks.
    /// All block indices in `[0, size())` are valid for
    /// I/O operations.
    u64 size() const { return do_size(); }

    /// Grows the underyling storage by `n` blocks.
    void grow(u64 n) { return do_grow(n); }

    /// Returns a handle to the given block if it already loaded into main memory.
    /// Otherwise returns an invalid handle.
    block_handle access(block_index index) {
        EXTPP_CHECK(index, "Invalid index.");
        return do_access(index);
    }

    /// Reads the block at the given index and returns a handle to it.
    /// Throws if an I/O error occurs.
    block_handle read(block_index index) {
        EXTPP_CHECK(index, "Invalid index.");
        return do_read(index);
    }

    /// Similar to `read()`, but the block is zeroed instead.
    /// This can save a read operation if the block is not already in memory.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// Throws if an I/O error occurs.
    block_handle zeroed(block_index index) {
        EXTPP_CHECK(index, "Invalid index.");
        return do_zeroed(index);
    }

    /// Like `zeroed()`, but instead sets the content of the block to `data`.
    ///
    /// Throws if an I/O error occurs.
    ///
    /// \warning `data` must be a pointer to (at least) `block_size()` bytes.
    block_handle overwritten(block_index index, const byte* data, size_t data_size) {
        EXTPP_CHECK(index, "Invalid index.");
        EXTPP_CHECK(data_size >= block_size(), "Not enough data.");
        return do_overwritten(index, data);
    }

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    void flush() {
        do_flush();
    }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

protected:
    virtual u64 do_size() const = 0;
    virtual void do_grow(u64 n) = 0;
    virtual block_handle do_access(block_index index) = 0;
    virtual block_handle do_read(block_index index) = 0;
    virtual block_handle do_zeroed(block_index index) = 0;
    virtual block_handle do_overwritten(block_index index, const byte* data) = 0;
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

    EXTPP_ASSERT(offset_in_block < block_size(), "Invalid offset in block.");
    return get_engine().to_address(index(), offset_in_block);
}

} // namespace extpp

#endif // EXTPP_ENGINE_HPP
