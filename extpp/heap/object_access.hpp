#ifndef EXTPP_HEAP_OBJECT_ACCESS_HPP
#define EXTPP_HEAP_OBJECT_ACCESS_HPP

#include <extpp/address.hpp>
#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/handle.hpp>
#include <extpp/heap/base.hpp>
#include <extpp/heap/type_set.hpp>

namespace extpp::heap_detail {

/// Controls object layout on disk.
template<u32 BlockSize>
class object_access {
    /*
     * Object layout
     * =============
     *
     * The data part of every object is preceded by a header.
     * The kind of the header depends on the object's type and
     * the size of its body.
     *
     * The header for objects of static size is only a single u32,
     * its type index. Objects of dynamic size use an additional u32
     * to encode the body's legnth (in bytes). Very large objects
     * (those with more than 2^32 -1 bytes) have that field set to zero
     * and use an additional u64 to encode the object's real size.
     *
     */

public:
    static constexpr u32 max_compact_dynamic_size = u32(-1);

    struct object_header {
        const type_info* type = nullptr;    ///< Runtime type info.
        u64 header_size = 0;                ///< Number of bytes occupied by the header.
        u64 body_size = 0;                  ///< The size of the object's body.

        u64 total_size() const { return header_size + body_size; }
    };

public:
    object_access(extpp::engine<BlockSize>& eng)
        : m_engine(&eng)
    {}

    /// Constructs the object header for an object of the given type and size.
    /// The runtime representation of the header includes its own size on disk.
    object_header make_header(const type_info& type, u64 body_size) const {
        u32 required = sizeof(type_index);

        if (!type.dynamic_size) {
            EXTPP_CHECK(type.size == body_size,
                        "The size must match the size declared in the object's type.");
        } else {
            EXTPP_CHECK(body_size >= type.size,
                        "The size of a dynamic object must not be smaller than the size "
                        "declared in the object's type.");

            required += sizeof(u32);
            if (body_size > max_compact_dynamic_size)
                required += sizeof(u64);
        }

        object_header header;
        header.type = &type;
        header.header_size = required;
        header.body_size = body_size;
        return header;
    }

    /// Writes the header for an object of type `type` with size `body_size`
    /// into the location pointed at by `addr`. There must be enough space available
    /// at that location to hold both the header and the object's body.
    ///
    /// \param addr         The location at which the header must be written.
    /// \param available    The total number of available bytes at the given location.
    /// \param header       The header that shall be written to disk.
    void write_header(raw_address<BlockSize> addr, u64 available, const object_header& header) {
        EXTPP_ASSERT(available >= header.header_size + header.body_size,
                     "There must be enough space available for the header and the data.");
        unused(available);

        u64 bytes_written = 0;
        auto write = [&](auto* ptr) {
            size_t n = sizeof(*ptr);
            extpp::write(this->engine(), addr, ptr, n);
            addr += n;
            bytes_written += n;
        };

        type_index index = header.type->index;
        write(&index);

        if (header.type->dynamic_size) {
            u32 compact_size = header.body_size <= max_compact_dynamic_size ? header.body_size : 0;
            write(&compact_size);

            if (compact_size == 0) {
                u64 huge_size = header.body_size;
                write(&huge_size);
            }
        }
        EXTPP_ASSERT(bytes_written == header.header_size, "Invalid number of written bytes.");
    }

    /// Reads the object header at the given address.
    object_header read_header(raw_address<BlockSize> addr, const type_set& types) const {
        u64 bytes_read = 0;
        auto read = [&](auto* ptr) {
            size_t n = sizeof(*ptr);
            extpp::read(this->engine(), addr, ptr, n);
            addr += n;
            bytes_read += n;
        };

        object_header header;

        type_index index;
        read(&index);
        header.type = &types.get(index);

        if (header.type->dynamic_size) {
            u32 compact_size = 0;
            read(&compact_size);
            if (compact_size != 0) {
                header.body_size = compact_size;
            } else {
                u64 huge_size = 0;
                read(&huge_size);
                header.body_size = huge_size;
            }
        } else {
            header.body_size = header.type->size;
        }

        header.header_size = bytes_read;
        return header;
    }

private:
    extpp::engine<BlockSize>& engine() const { return *m_engine; }

private:
    extpp::engine<BlockSize>* m_engine;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_OBJECT_ACCESS_HPP
