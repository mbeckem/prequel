#ifndef EXTPP_HEAP_BASE_HPP
#define EXTPP_HEAP_BASE_HPP

#include <extpp/defs.hpp>
#include <extpp/math.hpp>

namespace extpp {

namespace heap_detail {

static constexpr u64 cell_size = 16;
static constexpr u64 cell_size_log = log2(cell_size);

}

}

#endif // EXTPP_HEAP_BASE_HPP
