#ifndef PREQUEL_ENGINE_FILE_ENGINE_HPP
#define PREQUEL_ENGINE_FILE_ENGINE_HPP

#include "base.hpp"
#include "engine_base.hpp"

#include <prequel/vfs.hpp>

namespace prequel::detail::engine_impl {

class file_engine final : public engine_base {
public:
    inline explicit file_engine(file& fd, u32 block_size, size_t cache_blocks);
    inline ~file_engine();

    inline file& fd() const { return *m_file; }

    inline u64 size() const;
    inline void grow(u64 n);

protected:
    inline void do_read(u64 index, byte* buffer) override;
    inline void do_write(u64 index, const byte* buffer) override;

private:
    /// Underlying I/O-object.
    file* m_file = nullptr;
};

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_FILE_ENGINE_HPP
