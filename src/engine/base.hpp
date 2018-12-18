#ifndef PREQUEL_ENGINE_BASE_HPP
#define PREQUEL_ENGINE_BASE_HPP

#include <prequel/assert.hpp>
#include <prequel/defs.hpp>

#include <fmt/format.h>

#include <boost/intrusive/list_hook.hpp>
#include <boost/intrusive/set_hook.hpp>
#include <boost/intrusive/unordered_set_hook.hpp>

namespace prequel::detail::engine_impl {

class block;
class block_cache;
class block_dirty_set;
class block_map;
class block_pool;

class engine_base;
class file_engine;
class transaction_engine;

using block_cache_hook = boost::intrusive::list_member_hook<>;

using block_dirty_set_hook = boost::intrusive::set_member_hook<>;

using block_map_hook = boost::intrusive::unordered_set_member_hook<>;

using block_pool_hook = boost::intrusive::list_member_hook<>;

#ifdef PREQUEL_TRACE_IO
#    define PREQUEL_PRINT_READ(index) (fmt::print("Reading block {}\n", (index)))
#    define PREQUEL_PRINT_WRITE(index) (fmt::print("Writing block {}\n", (index)))
#else
#    define PREQUEL_PRINT_READ(index)
#    define PREQUEL_PRINT_WRITE(index)
#endif

} // namespace prequel::detail::engine_impl

#endif // PREQUEL_ENGINE_BASE_HPP
