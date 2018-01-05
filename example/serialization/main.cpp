#include <extpp/serialization.hpp>

#include <memory>

using extpp::u8;
using extpp::u16;
using extpp::u32;
using extpp::u64;

struct sqlite_header_t {
    static std::array<u8, 16> sqlite_magic() {
        std::array<u8, 16> buffer{};
        std::memcpy(buffer.data(), "SQLite format 3", strlen("SQLite format 3"));
        return buffer;
    }

    std::array<u8, 16> magic = sqlite_magic();

    u16 page_size = 0;
    u8 write_version = 0;
    u8 read_version = 0;
    u8 reserved_at_end = 0;

    u8 max_embedded_payload_fraction = 64;
    u8 min_embedded_payload_fraction = 32;
    u8 leaf_payload_fraction = 32;

    u32 file_change_counter = 0;
    u32 file_size = 0;

    u32 first_freelist_page = 0;
    u32 freelist_pages = 0;

    u32 schema_cookie = 0;
    u32 schema_format = 0;

    u32 default_page_cache_size = 0;
    u32 largest_btree_root_page = 0;
    u32 text_encoding = 0;
    u32 user_version = 0;
    u32 incremental_vacuum = 0;
    u32 application_id = 0;
    u8 reserved[20]{};

    u32 version_valid_for = 0;
    u32 sqlite_version_number = 0;

    static constexpr auto get_binary_format() {
        return extpp::make_binary_format(
            &sqlite_header_t::magic,
            &sqlite_header_t::page_size,
            &sqlite_header_t::write_version,
            &sqlite_header_t::read_version,
            &sqlite_header_t::reserved_at_end,

            &sqlite_header_t::max_embedded_payload_fraction,
            &sqlite_header_t::min_embedded_payload_fraction,
            &sqlite_header_t::leaf_payload_fraction,

            &sqlite_header_t::file_change_counter,
            &sqlite_header_t::file_size,

            &sqlite_header_t::first_freelist_page,
            &sqlite_header_t::freelist_pages,

            &sqlite_header_t::schema_cookie,
            &sqlite_header_t::schema_format,

            &sqlite_header_t::default_page_cache_size,
            &sqlite_header_t::largest_btree_root_page,
            &sqlite_header_t::text_encoding,
            &sqlite_header_t::user_version,
            &sqlite_header_t::incremental_vacuum,
            &sqlite_header_t::application_id,
            &sqlite_header_t::reserved,

            &sqlite_header_t::version_valid_for,
            &sqlite_header_t::sqlite_version_number
        );
    }
};

// To inspect the generated assembly.
// g++ generates a large sequence of moves + byteswaps.
extern "C" void serialize(const sqlite_header_t* hdr, unsigned char* buffer, size_t buffer_size) {
    EXTPP_ASSERT(sizeof(buffer) >= extpp::serialized_size(*hdr),
                 "Buffer not large enough.");
    extpp::unused(buffer_size);
    extpp::serialize(*hdr, buffer);
}


int main() {
    return 0;
}
