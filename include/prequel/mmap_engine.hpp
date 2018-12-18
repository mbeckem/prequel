#ifndef PREQUEL_MMAP_ENGINE_HPP
#define PREQUEL_MMAP_ENGINE_HPP

#include <prequel/defs.hpp>
#include <prequel/engine.hpp>
#include <prequel/exception.hpp>
#include <prequel/vfs.hpp>

#include <memory>

namespace prequel {

namespace detail {

class mmap_engine_impl;

} // namespace detail

class mmap_engine final : public engine {
public:
    /**
     * Constructs a new mmap engine.
     *
     * @param fd
     *      The file used for input and output (via mmap). The reference
     *      must remain valid for the lifetime of the engine instance.
     *
     * @param block_size
     *      The size of a single block, in bytes.
     *      Must be a power of two.
     */
    explicit mmap_engine(file& fd, u32 block_size);
    ~mmap_engine();

    /**
     * Returns the underlying file handle. The file should not be manipulated
     * directly unless you know exactly what you're doing.
     */
    file& fd() const;

private:
    u64 do_size() const override;
    void do_grow(u64 n) override;
    void do_flush() override;

    pin_result do_pin(block_index index, bool initialize) override;
    void do_unpin(block_index index, uintptr_t cookie) noexcept override;
    void do_flush(block_index index, uintptr_t cookie) override;
    void do_dirty(block_index index, uintptr_t cookie) override;

private:
    detail::mmap_engine_impl& impl() const;

private:
    std::unique_ptr<detail::mmap_engine_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_MMAP_ENGINE_HPP
