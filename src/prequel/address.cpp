#include <prequel/address.hpp>

#include <prequel/engine.hpp>

namespace prequel {

std::ostream& operator<<(std::ostream& o, const raw_address& addr) {
    if (!addr) {
        o << "INVALID";
    } else {
        o << addr.value();
    }
    return o;
}

/// Perform a linear write, starting from the given disk address.
/// Will write exactly `size` bytes from `data` to disk to the
/// address range [address, address + size).
void write(engine& e, raw_address address, const void* data, size_t size) {
    if (size == 0)
        return;

    const u32 block_size = e.block_size();
    const byte* buffer = reinterpret_cast<const byte*>(data);
    block_index index = e.to_block(address);

    // Partial write at the start.
    if (u32 offset = e.to_offset(address); offset != 0) {
        auto block = e.read(index);
        size_t n = std::min(size, size_t(block_size - offset));
        std::memmove(block.writable_data() + offset, buffer, n);

        buffer += n;
        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= block_size) {
        e.overwrite(index, buffer, block_size);

        buffer += block_size;
        size -= block_size;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memmove(block.writable_data(), buffer, size);
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
    block_index index = e.to_block(address);

    // Partial read at the start.
    if (u32 offset = e.to_offset(address); offset != 0) {
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
    block_index index = e.to_block(address);

    // Partial write at the start.
    if (u32 offset = e.to_offset(address); offset != 0) {
        auto block = e.read(index);
        u64 n = std::min(size, u64(block_size - offset));
        std::memset(block.writable_data() + offset, 0, n);

        size -= n;
        index += 1;
    }
    // Write as many full blocks as possible.
    while (size >= block_size) {
        e.overwrite_zero(index);

        size -= block_size;
        index += 1;
    }
    // Partial write at the end.
    if (size > 0) {
        auto block = e.read(index);
        std::memset(block.writable_data(), 0, size);
    }
}

static void copy_forward(engine& e, raw_address src, raw_address dest, u64 size) {
    const u32 block_size = e.block_size();
    auto index = [&](auto addr) { return e.to_block(addr); };
    auto offset = [&](auto addr) { return e.to_offset(addr); };

    const bool can_overwrite = distance(src, dest) >= block_size;

    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size >= block_size) {
                dest_handle = e.overwrite_zero(index(dest));
            } else {
                dest_handle = e.read(index(dest));
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src));
        }

        u32 chunk = std::min(block_size - offset(src), block_size - offset(dest));
        if (chunk > size)
            chunk = size;

        PREQUEL_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        PREQUEL_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.writable_data() + offset(dest), src_handle.data() + offset(src),
                     chunk);
        src += chunk;
        dest += chunk;
        size -= chunk;
    }
}

static void copy_backward(engine& e, raw_address src, raw_address dest, u64 size) {
    const u32 block_size = e.block_size();
    auto index = [&](auto addr) { return e.to_block(addr); };
    auto offset = [&](auto addr) { return e.to_offset(addr); };

    const bool can_overwrite = distance(src, dest) >= block_size;

    src += size;
    dest += size;
    block_handle src_handle, dest_handle;
    while (size > 0) {
        if (!dest_handle || offset(dest) == 0) {
            if (can_overwrite && offset(dest) == 0 && size >= block_size) {
                dest_handle = e.overwrite_zero(index(dest - 1));
            } else {
                dest_handle = e.read(index(dest - 1));
            }
        }

        if (!src_handle || offset(src) == 0) {
            src_handle = e.read(index(src - 1));
        }

        u32 chunk = std::min(offset(src) ? offset(src) : block_size,
                             offset(dest) ? offset(dest) : block_size);
        if (chunk > size)
            chunk = size;

        src -= chunk;
        dest -= chunk;
        size -= chunk;

        PREQUEL_ASSERT(dest_handle.index() == index(dest), "Correct destination block.");
        PREQUEL_ASSERT(src_handle.index() == index(src), "Correct source block.");
        std::memmove(dest_handle.writable_data() + offset(dest), src_handle.data() + offset(src),
                     chunk);
    }
}

/// Copies `size` bytes from `src` to `dest`. The two ranges can overlap.
/// \pre `src` and `dest` are valid addresses.
void copy(engine& e, raw_address src, raw_address dest, u64 size) {
    PREQUEL_ASSERT(dest, "Invalid destination address.");
    PREQUEL_ASSERT(src, "Invalid source address.");

    if (dest == src || size == 0)
        return;
    if (src > dest || (src + size <= dest)) {
        return copy_forward(e, src, dest, size);
    }
    return copy_backward(e, src, dest, size);
}

} // namespace prequel
