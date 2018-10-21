#ifndef PREQUEL_MEMORY_ENGINE_HPP
#define PREQUEL_MEMORY_ENGINE_HPP

#include <prequel/defs.hpp>
#include <prequel/engine.hpp>

namespace prequel {

namespace detail {

class memory_engine_impl;

} // namespace detail

/**
 * A very simplistic in-memory engine implementation.
 * Data is not persisted in any way; everything will be lost
 * once the engine instance is being destroyed.
 *
 * The main use of this class is for unit testing.
 */
class memory_engine : public engine {
public:
    /**
     * Constructs a new in-memory engine with the specified block size.
     */
    explicit memory_engine(u32 block_size);

    ~memory_engine();

private:
    u64 do_size() const override;
    void do_grow(u64 n) override;
    block_handle do_access(block_index index) override;
    block_handle do_read(block_index index) override;
    block_handle do_overwrite_zero(block_index index) override;
    block_handle do_overwrite(block_index index, const byte* data) override;
    void do_flush() override;

private:
    detail::memory_engine_impl& impl() const;

private:
    std::unique_ptr<detail::memory_engine_impl> m_impl;
};

} // namespace prequel

#endif // PREQUEL_MEMORY_ENGINE_HPP
