#include <extpp/exception.hpp>

namespace extpp {

bad_access::bad_access(const char* what)
    : exception(what)
{}

} // namespace extpp
