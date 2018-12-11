#ifndef PREQUEL_CONTAINER_IDENTITY_KEY_HPP
#define PREQUEL_CONTAINER_IDENTITY_KEY_HPP

#include <prequel/defs.hpp>

#include <functional>

namespace prequel {

// TODO move these to a file like <container/support.hpp>

struct identity_t {
    template<typename T>
    T operator()(const T& value) const {
        return value;
    }
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_IDENTITY_KEY_HPP
