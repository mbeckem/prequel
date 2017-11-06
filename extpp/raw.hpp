#ifndef EXTPP_RAW_HPP
#define EXTPP_RAW_HPP

#include <extpp/type_traits.hpp>

#include <memory>

namespace extpp {

/// A class stores an instance of T but defers its construction.
///
/// The purpose of raw<T> objects is to represent not-yet-constructed
/// objects in preallocated storage, i.e. for yet unused elements
/// in a resizable array.
///
/// Note that this class does not keep track of the object's state in order
/// to be the same size as T.
/// The user is resposible for only accessing the inner object if it has been
/// previously set to a valid value.
///
/// \tparam T
///     The value stored by this class. Must be trivial.
template<typename T>
union raw {
    static_assert(is_trivial<T>::value,
                  "The stored value must be trivial.");

    using value_type = T;

    /// Valaue remains uninitialized.
    constexpr raw() {}

    /// Value will be initialized using `v`.
    explicit raw(const T& v)
        : value(v)
    {}

    raw(const raw&) noexcept = default;
    raw& operator=(const raw&) noexcept = default;

    /// Assigns the given value to the inner object.
    raw& operator=(const T& value) noexcept {
        this->value = value;
        return *this;
    }

    T* ptr() { return std::addressof(value); }
    const T* ptr() const { return std::addressof(value); }

    T& ref() { return value; }
    const T& ref() const { return value; }

    /// Convertible to a reference to the inner object.
    /// The object must have been constructed earlier.
    operator T&() { return value; }
    operator const T&() const { return value; }

    /// Uninitialized by default.
    T value;
};

} // namespace extpp

#endif // EXTPP_RAW_HPP
