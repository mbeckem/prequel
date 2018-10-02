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
    struct anchor {
        UserData user;
        u32 block_size = 0;
        default_allocator::anchor alloc;

        static constexpr auto get_binary_format() {
            return make_binary_format(&anchor::user, &anchor::block_size, &anchor::alloc);
        }
    };

public:
    static constexpr u32 default_cache_size = 512;

public:
    explicit default_file_format(file& f, u32 block_size);
    explicit default_file_format(file& f, u32 block_size, u32 cache_size);

    ~default_file_format() {
        try {
            flush();
        } catch (...) {}
    }

    default_file_format(const default_file_format&) = delete;
    default_file_format(default_file_format&&) = delete;

    file_engine& get_engine() const { return *m_engine; }
    default_allocator& get_allocator() const { return *m_allocator; }
    anchor_handle<UserData> get_user_data() const { return m_anchor_handle.template member<&anchor::user>(); }

    void flush() {
        if (m_anchor_changed) {
            m_handle.set(m_anchor_data);
            m_anchor_changed.reset();
        }
        m_engine->flush();
    }

private:
    file* m_file = nullptr;
    std::unique_ptr<file_engine> m_engine;
    handle<anchor> m_handle;                        ///< Keeps the main block in memory.
    anchor m_anchor_data;                           ///< In-Memory representation of the anchor.
    anchor_flag m_anchor_changed;                   ///< Changed flag (for flush).
    anchor_handle<anchor> m_anchor_handle;          ///< Anchor handle to pass around, points to m_anchor_data.
    std::unique_ptr<default_allocator> m_allocator; ///< Main allocator used by the file.
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
        EXTPP_THROW(bad_argument("File size is not a multiple of the block size."));
    }
    m_engine = std::make_unique<file_engine>(*m_file, block_size, cache_size);

    if (m_engine->size() == 0) {
        m_engine->grow(1);
        m_handle.reset(m_engine->zeroed(block_index(0)), 0);
        m_handle.construct();
        m_handle.template set<&anchor::block_size>(m_engine->block_size());
        m_engine->flush();
    } else {
        m_handle.reset(m_engine->read(block_index(0)), 0);
    }

    m_anchor_data = m_handle.get();

    // Verify meta information.
    // TODO: More (e.g. version, checksum).
    if (m_anchor_data.block_size != m_engine->block_size()) {
        std::string message = fmt::format(
                    "File was opened with invalid block size ({}), "
                    "its original block size is {}.",
                    m_engine->block_size(), m_anchor_data.block_size);
        EXTPP_THROW(corruption_error(std::move(message)));
    }

    m_anchor_handle = make_anchor_handle(m_anchor_data, m_anchor_changed);
    m_allocator = std::make_unique<default_allocator>(m_anchor_handle.template member<&anchor::alloc>(), *m_engine);
    m_allocator->can_grow(true);
}

} // namespace extpp

#endif // EXTPP_DEFAULT_FILE_FORMAT_HPP
