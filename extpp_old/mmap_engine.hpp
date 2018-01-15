#ifndef EXTPP_MMAP_ENGINE_HPP
#define EXTPP_MMAP_ENGINE_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/io.hpp>

#include <memory>
#include <vector>

namespace extpp {
namespace detail {

class mmap_backend;
class mmap_engine;
class mmap_handle;

class mmap_backend {
public:
    mmap_backend(file& f, u32 block_size);
    ~mmap_backend();

    mmap_backend(const mmap_backend&) = delete;
    mmap_backend& operator=(const mmap_backend&) = delete;

    byte* access(u64 block) const;
    void update();

    void sync();

private:
    file* m_file;
    u32 m_block_size;
    u64 m_mapped_size = 0;
    std::vector<void*> m_maps;
};

class mmap_engine {
public:
    mmap_engine(file& fd, u32 block_size);

    u64 size() const {
        return m_file->file_size() / m_block_size;
    }

    void grow(u64 n);

    u32 block_size() const { return m_block_size; }

    std::unique_ptr<mmap_handle> read(u64 index);

    std::unique_ptr<mmap_handle> zeroed(u64 index);

    std::unique_ptr<mmap_handle> overwritten(u64 index, const byte* data);

    void flush() {
        m_backend.sync();
    }

    mmap_engine(const mmap_engine&) = delete;
    mmap_engine& operator=(const mmap_engine&) = delete;

private:
    friend class mmap_handle;

    std::unique_ptr<mmap_handle> allocate_handle();

    void free_handle(std::unique_ptr<mmap_handle> handle);

private:
    /// Underlying file storage.
    file* m_file;

    /// Block size of this engine.
    u32 m_block_size;

    /// Keeps the file mapped in memory.
    mmap_backend m_backend;

    /// Pool of unused handle instances.
    std::vector<std::unique_ptr<mmap_handle>> m_pool;

    /// Maximum number of instances in the pool.
    size_t m_max_pool_size = 1024;
};

class mmap_handle : public block_handle_impl {
private:
    u64 m_index = -1;
    byte* m_data = nullptr;
    mmap_engine* m_engine = nullptr;

private:
    friend class mmap_engine;

    mmap_handle(mmap_engine* engine)
        : m_engine(engine)
    {}

    void reset() {
        m_index = -1;
        m_data = nullptr;
    }

    void reset(u64 index, byte* data) {
        m_index = index;
        m_data = data;
    }

public:
    u64 index() const noexcept override {
        check_valid();
        return m_index;
    }

    byte* data() const noexcept override {
        check_valid();
        return m_data;
    }

    u32 block_size() const noexcept override {
        check_valid();
        return m_engine->block_size();
    }

    void dirty() override {
        check_valid();
    }

    virtual mmap_handle* copy() override {
        check_valid();

        std::unique_ptr<mmap_handle> ptr = m_engine->allocate_handle();
        ptr->reset(m_index, m_data);
        return ptr.release();
    }

    virtual void destroy() override {
        check_valid();

        std::unique_ptr<mmap_handle> self(this);
        m_engine->free_handle(std::move(self));
    }

private:
    void check_valid() const noexcept {
        EXTPP_ASSERT(m_data, "Invalid instance leaked.");
    }
};

} // namespace detail

class mmap_engine : public engine {
public:
    explicit mmap_engine(file& fd, u32 block_size)
        : engine(block_size)
        , m_impl(fd, block_size)
    {}

private:
    u64 do_size() const {
        return m_impl.size();
    }

    void do_grow(u64 n) {
        m_impl.grow(n);
    }

    block_handle do_read(block_index index) {
        return m_impl.read(index.value()).release();
    }

    block_handle do_zeroed(block_index index) {
        return m_impl.zeroed(index.value()).release();
    }

    block_handle do_overwritten(block_index index, const byte* data) {
        return m_impl.overwritten(index.value(), data).release();
    }

    void do_flush() {
        m_impl.flush();
    }

private:
    detail::mmap_engine m_impl;
};

} // namespace extpp

#endif // EXTPP_MMAP_ENGINE_HPP
