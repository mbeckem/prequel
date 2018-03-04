#ifndef EXTPP_EXCEPTION_HPP
#define EXTPP_EXCEPTION_HPP

#include <exception>
#include <string>
#include <system_error>

#include <fmt/format.h>

/// Expends to the current source location (file, line, function).
#define EXTPP_SOURCE_LOCATION \
    (::extpp::source_location(__FILE__, __LINE__, __func__))

/// Augments an \ref extpp::exception with the current source location.
#define EXTPP_AUGMENT_EXCEPTION(e) \
    (::extpp::detail::with_location((e), EXTPP_SOURCE_LOCATION))

/// Throw the given \ref extpp::exception with added source location information.
#define EXTPP_THROW(e) throw (EXTPP_AUGMENT_EXCEPTION(e))

/// Throw a new \ref extpp::exception `e` with added source location information
/// and the currently active exception (if any) as its cause.
#define EXTPP_THROW_NESTED(e) (::std::throw_with_nested(EXTPP_AUGMENT_EXCEPTION(e)))

namespace extpp {

class source_location {
public:
    source_location() = default;

    source_location(const char* file, int line, const char* function)
        : m_file(file), m_line(line), m_function(function)
    {}

    const char* file() const { return m_file; }
    int line() const { return m_line; }
    const char* function() { return m_function; }

private:
    const char* m_file = "";
    int m_line = 0;
    const char* m_function = "";
};

class exception;

namespace detail {

template<typename Exception>
Exception with_location(Exception&& e, const source_location& where) {
    static_assert(std::is_base_of<exception, std::decay_t<Exception>>::value,
                  "Exception must be derived from extpp::exception.");
    e.set_where(where);
    return std::forward<Exception>(e);
}

} // namespace detail

class exception : public std::runtime_error {
public:
    using runtime_error::runtime_error;

    const source_location& where() const { return m_where; }

private:
    template<typename T>
    friend T detail::with_location(T&&, const source_location&);

    void set_where(const source_location& loc) { m_where = loc; }

private:
    source_location m_where;
};

// TODO: use std class instead?
class invalid_argument : public exception {
public:
    using exception::exception;
};

class unsupported : public exception {
public:
    using exception::exception;
};

class io_error : public exception {
public:
    using exception::exception;
};

class bad_cursor : public exception {
public:
    bad_cursor();

    bad_cursor(const char* what);
};

} // namespace extpp

#endif // EXTPP_EXCEPTION_HPP
