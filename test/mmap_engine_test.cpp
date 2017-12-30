#include <catch.hpp>

#include <extpp/mmap_engine.hpp>

using namespace extpp;

static constexpr u32 block_size = 512;
using engine_t = mmap_engine;
