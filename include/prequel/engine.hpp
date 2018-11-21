#ifndef PREQUEL_ENGINE_HPP
#define PREQUEL_ENGINE_HPP

#include <prequel/address.hpp>
#include <prequel/assert.hpp>
#include <prequel/defs.hpp>
#include <prequel/math.hpp>
#include <prequel/serialization.hpp>

#include <utility>

namespace prequel {

class block_handle;
class engine;

namespace detail {

class block_handle_manager;

/*
 * References a block from some engine that has been pinned into memory.
 * The engine manages these objects (through refcounting) and unpins
 * the blocks once all references to them have been dropped.
 *
 * Note that the acutal implementation of this is within a subclass in the cpp file,
 * this (unusual) design allows us to expose the frequently accessed data members
 * in the header file and keep the heavy boost headers used for the intrusive
 * datastructures hidden in the implementation file.
 */
class block_handle_base {
protected:
    ~block_handle_base() = default;

    block_handle_base() = default;

public:
    block_handle_base(const block_handle_base&) = delete;
    block_handle_base& operator=(const block_handle_base&) = delete;

    void inc_ref() noexcept { ++m_refcount; }

    void dec_ref() noexcept; // "delete this" is possible

    engine* get_engine() const noexcept { return m_engine; }

    block_index index() const noexcept { return m_index; }

    const byte* data() const noexcept { return m_data; }
    byte* writable_data();
    void flush();

protected:
    // Number of references. Block is unpinned and handle is deleted when this reaches 0.
    u32 m_refcount = 0;

    // The engine that owns this block.
    engine* m_engine = nullptr;

    // Index of the block within the engine.
    block_index m_index;

    // Block data is kept alive by the underlying engine until it is unpinned.
    byte* m_data = nullptr;

    // Opaque value from the underlying engine.
    uintptr_t m_cookie = 0;
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
    explicit block_handle(detail::block_handle_base* base)
        : m_impl(base) {
        PREQUEL_ASSERT(base, "Null implementation.");
        m_impl->inc_ref();
    }

    block_handle(const block_handle& other)
        : m_impl(other.m_impl) {
        if (m_impl)
            m_impl->inc_ref();
    }

    block_handle(block_handle&& other) noexcept
        : m_impl(std::exchange(other.m_impl, nullptr)) {}

    ~block_handle() {
        if (m_impl)
            m_impl->dec_ref();
    }

    block_handle& operator=(const block_handle& other) {
        if (other.m_impl)
            other.m_impl->inc_ref();
        if (m_impl)
            m_impl->dec_ref();
        m_impl = other.m_impl;
        return *this;
    }

    block_handle& operator=(block_handle&& other) noexcept {
        if (this != &other) {
            if (m_impl)
                m_impl->dec_ref();
            m_impl = std::exchange(other.m_impl, nullptr);
        }
        return *this;
    }

    /// @{
    /// Returns true if this handle is valid, i.e. if it references a block.
    bool valid() const noexcept { return m_impl != nullptr; }
    explicit operator bool() const noexcept { return valid(); }
    /// @}

    /// Returns a reference to the block's engine.
    /// \pre `valid()`.
    engine& get_engine() const noexcept {
        check_valid();
        return *m_impl->get_engine();
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

    /// Flush the contents of this block to secondary storage if they were changed.
    void flush() const {
        check_valid();
        m_impl->flush();
    }

    /// Deserialize a value of type T at the given offset.
    template<typename T>
    T get(u32 offset) const {
        PREQUEL_ASSERT(check_range(offset, serialized_size<T>()), "Reading out of bounds.");
        return deserialize<T>(data() + offset);
    }

    /// Serialize a value and put it at the given offset.
    template<typename T>
    void set(u32 offset, const T& value) const {
        PREQUEL_ASSERT(check_range(offset, serialized_size<T>()), "Writing out of bounds.");
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
    void check_valid() const { PREQUEL_ASSERT(valid(), "Invalid instance."); }

    bool check_range(u32 offset, u32 size) const {
        check_valid();
        return offset <= block_size() && size <= block_size() - offset;
    }

private:
    detail::block_handle_base* m_impl = nullptr;
};

/**
 * Provides block-oriented access to the contents of a file.
 * This class defines the interface for multiple engine implementations.
 */
class engine {
protected:
    explicit engine(u32 block_size);

public:
    virtual ~engine();

    /// Returns the size (in bytes) of every block returned by this engine.
    u32 block_size() const noexcept { return m_block_size; }

    /// Returns the size of the underlying storage, in blocks.
    /// All block indices in `[0, size())` are valid for
    /// I/O operations.
    u64 size() const;

    /// Grows the underyling storage by `n` blocks.
    void grow(u64 n);

    /// Writes all dirty blocks back to disk.
    /// Throws if an I/O error occurs.
    void flush();

    /// Reads the block at the given index and returns a handle to it.
    /// Throws if an I/O error occurs.
    block_handle read(block_index index);

    /// Similar to `read()`, but the block is zeroed instead.
    /// This can save a read operation if the block is not already in memory.
    ///
    /// \note If the block was already in memory, its contents will be overwritten
    /// with zeroes as well.
    ///
    /// Throws if an I/O error occurs.
    block_handle overwrite_zero(block_index index);

    /// Like `zeroed()`, but instead sets the content of the block to `data`.
    ///
    /// Throws if an I/O error occurs.
    ///
    /// \warning `data` must be a pointer to (at least) `block_size()` bytes.
    block_handle overwrite(block_index index, const byte* data, size_t data_size);

    /// Returns the address to the first byte of the given block.
    /// Returns the invalid address if the block index is invalid.
    raw_address to_address(block_index index) const noexcept { return to_address(index, 0); }

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
    u64 to_byte_size(u64 block_count) const noexcept { return block_count << m_block_size_log; }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

private:
    friend detail::block_handle_base;

    // Initialize with block data from storage.
    struct initialize_block_t {};

    // Initialize with zero, without reading from storage.
    struct initialize_zero_t {
        void apply(byte* block, u32 block_size) { std::memset(block, 0, block_size); }
    };

    // Initialize with the given data array, without reading from storage.
    struct initialize_data_t {
        const byte* data;

        void apply(byte* block, u32 block_size) { std::memmove(block, data, block_size); }
    };

    template<typename Initializer>
    block_handle internal_populate_handle(block_index index, Initializer&& init);
    void internal_flush_handle(detail::block_handle_base* handle);
    void internal_dirty_handle(detail::block_handle_base* handle);
    void internal_release_handle(detail::block_handle_base* handle) noexcept;

    detail::block_handle_manager& handle_manager() const;

protected:
    virtual u64 do_size() const = 0;
    virtual void do_grow(u64 n) = 0;
    virtual void do_flush() = 0;

protected:
    struct pin_result {
        // Block data storage. Remains valid until the block is unpinned.
        byte* data = nullptr;

        // Opaque value. Will be passed back to the engine when the block is modified.
        uintptr_t cookie = 0;
    };

    // Pins the block with the given index into memory.
    // The block data must stay valid until do_unpin() has been called with the same index,
    // at which point the engine subclass may release the storage.
    //
    // If `initialize` is false, then the engine subclass does not have to initalize the block data (e.g.
    // by reading from disk). The data will initialized by the caller instead.
    virtual pin_result do_pin(block_index index, bool initialize) = 0;

    // Called exactly once for every pin call.
    virtual void do_unpin(block_index index, uintptr_t cookie) noexcept = 0;

    // Marks the block storage as dirty. Only called for pinned blocks.
    virtual void do_dirty(block_index index, uintptr_t cookie) = 0;

    // Flush the block with the given index. Only called for pinned blocks.
    virtual void do_flush(block_index index, uintptr_t cookie) = 0;

protected:
    // log_2 (block_size)
    u32 block_size_log() const { return m_block_size_log; }

    // bitwise AND with byte offset -> block offset (cuts of high bits).
    u32 offset_mask() const { return m_offset_mask; }

private:
    u32 m_block_size;
    u32 m_block_size_log;
    u32 m_offset_mask;

    std::unique_ptr<detail::block_handle_manager> m_handle_manager;
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

namespace detail {

inline void block_handle_base::dec_ref() noexcept {
    PREQUEL_ASSERT(m_refcount > 0, "Refcount is zero already.");
    if (--m_refcount == 0) {
        m_engine->internal_release_handle(this);
    }
}

inline byte* block_handle_base::writable_data() {
    m_engine->internal_dirty_handle(this);
    return m_data;
}

inline void block_handle_base::flush() {
    m_engine->internal_flush_handle(this);
}

} // namespace detail

} // namespace prequel

#endif // PREQUEL_ENGINE_HPP
