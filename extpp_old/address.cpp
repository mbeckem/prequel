#include <extpp/address.hpp>

#include <extpp/engine.hpp>

namespace extpp {

/// Perform a linear write, starting from the given disk address.
/// Will write exactly `size` bytes from `data` to disk to the
/// address range [address, address + size).
void write(engine& e, raw_address address, const void* data, size_t size) {
    if (size == 0)
        return;

    const u32 block_size = e.block_size();
    const byte* buffer = reinterpret_cast<const byte*>(data);
    block_index index = address.get_block_index(block_size);

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(block_size); offset != 0) {
        auto block = e.read(index);
        size_t n = std::min(size, size_t(block_size - offset));
        std::memmove(block.data() + offset, buffer, n);
        block.dirty();

        buffer += n;
        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= block_size) {
        e.overwritten(index, buffer);

        buffer += block_size;
        size -= block_size;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memmove(block.data(), buffer, size);
        block.dirty();
    }
}

/// Perform a linear read, starting from the given disk address.
/// Will read exactly `size` bytes from the address range [address, address + size)
/// on disk into `data`.
void read(engine& e, raw_address address, void* data, size_t size) {
    if (size == 0)
        return;

    const u32 block_size = e.block_size();
    byte* buffer = reinterpret_cast<byte*>(data);
    block_index index = address.get_block_index(block_size);

    // Partial read at the start.
    if (u32 offset = address.get_offset_in_block(block_size); offset != 0) {
        auto block = e.read(index);
        size_t n = std::min(size, size_t(block_size - offset));
        std::memmove(buffer, block.data() + offset, n);

        buffer += n;
        size -= n;
        index += 1;
    }
    // Full block reads.
    while (size >= block_size) {
        auto block = e.read(index);
        std::memmove(buffer, block.data(), block_size);

        buffer += block_size;
        size -= block_size;
        index += 1;
    }
    // Partial read at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memmove(buffer, block.data(), size);
    }
}

/// Zeroes `size` bytes, starting from the given address.
void zero(engine& e, raw_address address, u64 size) {
    if (size == 0)
        return;

    const u32 block_size = e.block_size();
    block_index index = address.get_block_index(block_size);

    // Partial write at the start.
    if (u32 offset = address.get_offset_in_block(block_size); offset != 0) {
        auto block = e.read(index);
        u64 n = std::min(size, u64(block_size - offset));
        std::memset(block.data() + offset, 0, n);
        block.dirty();

        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= block_size) {
        e.zeroed(index);

        size -= block_size;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memset(block.data(), 0, size);
        block.dirty();
    }
}

static void copy_forward(engine& e, raw_address dest, raw_address src, u64 size) {
    const u32 block_size = e.block_size();
    auto index = [&](auto addr) { return addr.get_block_index(block_size); };
    auto offset = [&](auto addr) { return addr.get_offset_in_block(block_size); };

    const bool can_overwrite = distance(src, dest) >= block_size;

    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size > block_size) {
                dest_handle = e.zeroed(index(dest));
            } else {
                dest_handle = e.read(index(dest));
                dest_handle.dirty();
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src));
        }

        u32 chunk = std::min(block_size - offset(src),
                             block_size - offset(dest));
        if (chunk > size)
            chunk = size;

        EXTPP_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.data() + offset(dest),
                     src_handle.data() + offset(src), chunk);
        src += chunk;
        dest += chunk;
        size -= chunk;
    }
}

static void copy_backward(engine& e, raw_address dest, raw_address src, u64 size) {
    const u32 block_size = e.block_size();
    auto index = [&](auto addr) { return addr.get_block_index(block_size); };
    auto offset = [&](auto addr) { return addr.get_offset_in_block(block_size); };

    const bool can_overwrite = distance(src, dest) >= block_size;

    src += size;
    dest += size;
    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size > block_size) {
                dest_handle = e.zeroed(index(dest-1));
            } else {
                dest_handle = e.read(index(dest-1));
                dest_handle.dirty();
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src-1));
        }

        u32 chunk = std::min(offset(src) ? offset(src) : block_size,
                             offset(dest) ? offset(dest) : block_size);
        if (chunk > size)
            chunk = size;

        src -= chunk;
        dest -= chunk;
        size -= chunk;

        EXTPP_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        EXTPP_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.data() + offset(dest),
                     src_handle.data() + offset(src), chunk);
    }
}

/// Copies `size` bytes from `src` to `dest`. The two ranges can overlap.
/// \pre `src` and `dest` are valid addresses.
void copy(engine& e, raw_address dest, raw_address src, u64 size) {
    EXTPP_ASSERT(dest, "Invalid destination address.");
    EXTPP_ASSERT(src, "Invalid source address.");

    if (dest == src || size == 0)
        return;
    if (src > dest || (src + size <= dest)) {
        return copy_forward(e, dest, src, size);
    }
    return copy_backward(e, dest, src, size);
}

} // namespace extpp
