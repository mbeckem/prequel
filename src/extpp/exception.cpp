#include <extpp/exception.hpp>

namespace extpp {

bad_element::bad_element()
    : exception("bad element") {}

bad_cursor::bad_cursor()
    : exception("bad cursor") {}

bad_cursor::bad_cursor(const char* what)
    : exception(what)
{}

} // namespace extpp
