#ifndef PREQUEL_DEFAULT_FILE_FORMAT_HPP
#define PREQUEL_DEFAULT_FILE_FORMAT_HPP

#include <prequel/defs.hpp>
#include <prequel/vfs.hpp>
#include <prequel/default_allocator.hpp>
#include <prequel/exception.hpp>
#include <prequel/file_engine.hpp>
#include <prequel/handle.hpp>
#include <prequel/mmap_engine.hpp>

#include <fmt/format.h>

#include <iostream>
#include <memory>

namespace prequel {

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

    struct anchor_data {
        anchor data;
        anchor_flag changed;
    };

public:
    /// Default number of blocks cached when using the normal file engine.
    static constexpr u32 default_cache_size = 512;

    static default_file_format mmap(file& f, u32 block_size);

public:
    explicit default_file_format(file& f, u32 block_size);
    explicit default_file_format(file& f, u32 block_size, u32 cache_size);
    default_file_format(default_file_format&&) noexcept;
    ~default_file_format();

    default_file_format& operator=(default_file_format&&) noexcept = delete;

    engine& get_engine() const { return *m_engine; }
    default_allocator& get_allocator() const { return *m_allocator; }
    anchor_handle<UserData> get_user_data() const { return m_anchor_handle.template member<&anchor::user>(); }

    void flush() {
        if (m_anchor->changed) {
            m_handle.set(m_anchor->data);
            m_anchor->changed.reset();
        }
        m_engine->flush();
    }

private:
    default_file_format() = default;

    void init();

private:
    file* m_file = nullptr;
    std::unique_ptr<engine> m_engine;
    handle<anchor> m_handle;                        ///< Keeps the main block in memory.
    std::unique_ptr<anchor_data> m_anchor;          ///< In-Memory representation (+ changed flag) of the anchor.
    anchor_handle<anchor> m_anchor_handle;          ///< Anchor handle to pass around, points to m_anchor->data.
    std::unique_ptr<default_allocator> m_allocator; ///< Main allocator used by the file.
};

template<typename Anchor>
inline default_file_format<Anchor> default_file_format<Anchor>::mmap(file& f, u32 block_size)
{
    if (f.file_size() % block_size != 0) {
        PREQUEL_THROW(bad_argument("File size is not a multiple of the block size."));
    }

    default_file_format df;
    df.m_file = &f;
    df.m_engine = std::make_unique<mmap_engine>(f, block_size);
    df.init();
    return df;
}

template<typename Anchor>
inline default_file_format<Anchor>::default_file_format(file& f, u32 block_size)
    : default_file_format(f, block_size, default_cache_size)
{}

template<typename Anchor>
inline default_file_format<Anchor>::default_file_format(file& f, u32 block_size, u32 cache_size)
{
    if (f.file_size() % block_size != 0) {
        PREQUEL_THROW(bad_argument("File size is not a multiple of the block size."));
    }
    m_file = &f;
    m_engine = std::make_unique<file_engine>(*m_file, block_size, cache_size);
    init();
}

template<typename Anchor>
inline default_file_format<Anchor>::default_file_format(default_file_format&& other) noexcept
    : m_file(other.m_file)
    , m_engine(std::move(other.m_engine))
    , m_handle(std::move(other.m_handle))
    , m_anchor(std::move(other.m_anchor))
    , m_anchor_handle(std::move(other.m_anchor_handle))
    , m_allocator(std::move(other.m_allocator))
{}

template<typename Anchor>
inline default_file_format<Anchor>::~default_file_format() {
    if (!m_engine)
        return; // Moved

    try {
       flush();
    } catch (...) {}
}

template<typename Anchor>
inline void default_file_format<Anchor>::init() {
    if (m_engine->size() == 0) {
        m_engine->grow(1);
        m_handle.reset(m_engine->overwrite_zero(block_index(0)), 0);
        m_handle.construct();
        m_handle.template set<&anchor::block_size>(m_engine->block_size());
        m_engine->flush();
    } else {
        m_handle.reset(m_engine->read(block_index(0)), 0);
    }

    m_anchor = std::make_unique<anchor_data>();
    m_anchor->data = m_handle.get();

    // Verify meta information.
    // TODO: More (e.g. version, checksum).
    if (m_anchor->data.block_size != m_engine->block_size()) {
        std::string message = fmt::format(
                    "File was opened with invalid block size ({}), "
                    "its original block size is {}.",
                    m_engine->block_size(), m_anchor->data.block_size);
        PREQUEL_THROW(corruption_error(std::move(message)));
    }

    m_anchor_handle = make_anchor_handle(m_anchor->data, m_anchor->changed);
    m_allocator = std::make_unique<default_allocator>(m_anchor_handle.template member<&anchor::alloc>(), *m_engine);
    m_allocator->can_grow(true);
}

} // namespace prequel

#endif // PREQUEL_DEFAULT_FILE_FORMAT_HPP
