#ifndef EXTPP_EXCEPTION_HPP
#define EXTPP_EXCEPTION_HPP

#include <exception>
#include <string>
#include <system_error>

#include <fmt/format.h>

/// @defgroup exception_support Exception support macros
/// @{

/**
 * Expands to the current source location (file, line, function).
 */
#define EXTPP_SOURCE_LOCATION \
    (::extpp::source_location(__FILE__, __LINE__, __func__))

/**
 * Augments an @ref extpp::exception with the current source location.
 */
#define EXTPP_AUGMENT_EXCEPTION(e) \
    (::extpp::detail::with_location((e), EXTPP_SOURCE_LOCATION))

/**
 * Throw the given @ref extpp::exception with added source location information.
 */
#define EXTPP_THROW(e) throw (EXTPP_AUGMENT_EXCEPTION(e))

/**
 * Throw a new @ref extpp::exception `e` with added source location information
 * and the currently active exception (if any) as its cause.
 */
#define EXTPP_THROW_NESTED(e) (::std::throw_with_nested(EXTPP_AUGMENT_EXCEPTION(e)))

/// @}

namespace extpp {

/**
 * Represents the source code location at which an exception was thrown.
 */
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

/**
 * Base class for all exceptions thrown by this library.
 */
class exception : public std::runtime_error {
public:
    using runtime_error::runtime_error;

    /**
     * Returns the source code location that threw this exception.
     *
     * \note Requires that the exception was thrown using
     * @ref EXTPP_THROW or @ref EXTPP_THROW_NESTED, otherwise `where()`
     * will return an empty source location.
     */
    const source_location& where() const { return m_where; }

private:
    template<typename T>
    friend T detail::with_location(T&&, const source_location&);

    void set_where(const source_location& loc) { m_where = loc; }

private:
    source_location m_where;
};

/**
 * Thrown when an allocation using extpp::allocator failed.
 */
class bad_alloc : public exception {
    using exception::exception;
};

/**
 * Thrown when the content of a datastructure is known to be corrupted.
 */
class corruption_error : public exception {
public:
    using exception::exception;
};

/**
 * Thrown when a specific operation is not implemented by an object.
 * For example, mmap() may not supported on all platforms.
 */
class unsupported : public exception {
public:
    using exception::exception;
};

/**
 * Thrown when data could not be read or written to secondary storage.
 */
class io_error : public exception {
public:
    using exception::exception;
};

/**
 * Exceptions of this class or its subclasses are thrown when an object
 * is being misused, i.e. it is being passed the wrong arguments
 * or it is in the wrong state.
 */
class usage_error : public exception {
public:
    using exception::exception;
};

/**
 * Thrown when an object cannot perform an operation in its current state.
 */
class bad_operation : public usage_error {
public:
    using usage_error::usage_error;
};

/**
 * Thrown when an invalid cursor is being accessed.
 */
class bad_cursor : public bad_operation {
public:
    using bad_operation::bad_operation;
};

/**
 * Thrown when an invalid argument is being passed to some operation.
 */
class bad_argument : public usage_error {
public:
    using usage_error::usage_error;
};

} // namespace extpp

#endif // EXTPP_EXCEPTION_HPP
