#ifndef EXTPP_MMAP_ENGINE_HPP
#define EXTPP_MMAP_ENGINE_HPP

#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/exception.hpp>
#include <extpp/vfs.hpp>

#include <memory>

namespace extpp {

namespace detail {

class mmap_engine_impl;

} // namespace detail

class mmap_engine : public engine {
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
    block_handle do_access(block_index index) override;
    block_handle do_read(block_index index) override;
    block_handle do_overwrite_zero(block_index index) override;
    block_handle do_overwrite(block_index index, const byte* data) override;
    void do_flush() override;

private:
    detail::mmap_engine_impl& impl() const;

private:
    std::unique_ptr<detail::mmap_engine_impl> m_impl;
};

} // namespace extpp

#endif // EXTPP_MMAP_ENGINE_HPP
