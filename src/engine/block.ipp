#ifndef PREQUEL_ENGINE_BLOCK_IPP
#define PREQUEL_ENGINE_BLOCK_IPP

#include "block.hpp"
#include "engine_base.hpp"

namespace prequel::detail::engine_impl {

block::block(engine_base* engine)
    : m_engine(engine) {
    PREQUEL_ASSERT(engine, "Invalid engine pointer.");
    m_data = static_cast<byte*>(std::malloc(engine->block_size()));
    if (!m_data)
        throw std::bad_alloc();
}

block::~block() {
    std::free(m_data);
}

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BLOCK_IPP
