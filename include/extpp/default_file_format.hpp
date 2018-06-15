#ifndef EXTPP_DEFAULT_FILE_FORMAT_HPP
#define EXTPP_DEFAULT_FILE_FORMAT_HPP

#include <extpp/defs.hpp>
#include <extpp/io.hpp>
#include <extpp/default_allocator.hpp>
#include <extpp/exception.hpp>
#include <extpp/file_engine.hpp>
#include <extpp/handle.hpp>

#include <fmt/format.h>

#include <iostream>
#include <memory>

namespace extpp {

// TODO: Version flags.
template<typename UserData>
class default_file_format {
private:
    struct main_anchor {
        UserData user;
        default_allocator::anchor alloc;

        static constexpr auto get_binary_format() {
            return make_binary_format(&main_anchor::user, &main_anchor::alloc);
        }
    };

public:
    static constexpr u32 default_cache_size = 512;

public:
    explicit default_file_format(file& f, u32 block_size);
    explicit default_file_format(file& f, u32 block_size, u32 cache_size);

    ~default_file_format() {
        flush();
    }

    default_file_format(const default_file_format&) = delete;
    default_file_format(default_file_format&&) = delete;

    file_engine& get_engine() const { return *m_engine; }
    default_allocator& get_allocator() const { return *m_allocator; }
    handle<UserData> get_user_data() const { return m_handle.template member<&main_anchor::user>(); }

    void flush() {
        // FIXME think more about anchor handles they're awkward.
        if (m_allocator_anchor.changed()) {
            m_handle.template set<&main_anchor::alloc>(m_allocator_anchor.get());
            m_allocator_anchor.reset_changed();
        }

        m_engine->flush(); // TODO: Sync?
    }

private:
    file* m_file = nullptr;
    std::unique_ptr<file_engine> m_engine;
    handle<main_anchor> m_handle;
    anchor_handle<default_allocator::anchor> m_allocator_anchor;
    std::unique_ptr<default_allocator> m_allocator;
};

template<typename Anchor>
inline default_file_format<Anchor>::default_file_format(file& f, u32 block_size)
    : default_file_format(f, block_size, default_cache_size)
{}

template<typename Anchor>
inline default_file_format<Anchor>::default_file_format(file& f, u32 block_size, u32 cache_size)
    : m_file(&f)
{
    if (m_file->file_size() % block_size != 0) {
        EXTPP_THROW(invalid_argument("File size is not a multiple of the block size."));
    }
    m_engine = std::make_unique<file_engine>(*m_file, block_size, cache_size);

    if (m_engine->size() == 0) {
        m_engine->grow(1);
        m_handle.reset(m_engine->zeroed(block_index(0)), 0);
        m_handle.construct();
        m_engine->flush();
    } else {
        m_handle.reset(m_engine->read(block_index(0)), 0);
        // TODO: Verify first block (checksum etc ?)
    }

    m_allocator_anchor = make_anchor_handle(m_handle.template get<&main_anchor::alloc>());
    m_allocator = std::make_unique<default_allocator>(m_allocator_anchor, *m_engine);
}

} // namespace extpp

#endif // EXTPP_DEFAULT_FILE_FORMAT_HPP
