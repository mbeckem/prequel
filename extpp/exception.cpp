#include <extpp/exception.hpp>

namespace extpp {

bad_cursor::bad_cursor()
    : exception("bad cursor") {}

bad_cursor::bad_cursor(const char* what)
    : exception(what)
{}

} // namespace extpp
