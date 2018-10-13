#ifndef PREQUEL_DETAIL_OPERATORS_HPP
#define PREQUEL_DETAIL_OPERATORS_HPP

namespace prequel {
namespace detail {

/// Implements all comparison operators for the derived type.
/// Only '<' and '==' have to be supplied by the user.
template<typename Derived>
class make_comparable {
public:
    friend bool operator>(const Derived& lhs, const Derived& rhs) {
        return rhs < lhs;
    }

    friend bool operator<=(const Derived& lhs, const Derived& rhs) {
        return !(lhs > rhs);
    }

    friend bool operator>=(const Derived& lhs, const Derived& rhs) {
        return !(lhs < rhs);
    }

    friend bool operator!=(const Derived& lhs, const Derived& rhs) {
        return !(lhs == rhs);
    }
};

template<typename Derived, typename Argument>
class make_addable {
public:
    Derived& operator++() {
        return static_cast<Derived*>(this)->operator+=(Argument(1));
    }

    Derived operator++(int) {
        Derived d = *static_cast<Derived*>(this);
        static_cast<Derived*>(this)->operator+=(Argument(1));
        return d;
    }

    friend Derived operator+(const Derived& lhs, const Argument& rhs) {
        Derived d(lhs);
        d += rhs;
        return d;
    }
};

template<typename Derived, typename Argument>
class make_subtractable {
public:
    Derived& operator--() {
        return static_cast<Derived*>(this)->operator-=(Argument(1));
    }

    Derived operator--(int) {
        Derived d = *static_cast<Derived*>(this);
        static_cast<Derived*>(this)->operator-=(Argument(1));
        return d;
    }

    friend Derived operator-(const Derived& lhs, const Argument& rhs) {
        Derived d(lhs);
        d -= rhs;
        return d;
    }
};

} // namespace detail
} // namespace prequel

#endif // PREQUEL_DETAIL_OPERATORS_HPP
