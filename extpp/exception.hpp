#ifndef EXTPP_EXCEPTION_HPP
#define EXTPP_EXCEPTION_HPP

#include <exception>
#include <string>
#include <system_error>

namespace extpp {

class exception : public std::exception {};

class io_error : public exception {
public:
    io_error(const char* message)
        : io_error(std::string(message))
    {}

    io_error(std::string message)
        : m_message(std::move(message))
    {}

    const char* what() const noexcept override {
        return m_message.c_str();
    }

private:
    std::string m_message;
};


} // namespace extpp

#endif // EXTPP_EXCEPTION_HPP
