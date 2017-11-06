#ifndef EXTPP_DETAIL_OPERATORS_HPP
#define EXTPP_DETAIL_OPERATORS_HPP

namespace extpp {
namespace detail {

/// Implements all comparison operators for the derived type.
/// Only '<' and '==' have to be supplied by the user.
template<typename Derived>
class make_comparable {
private:
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
    friend Derived operator+(const Derived& lhs, const Argument& rhs) {
        Derived d(lhs);
        d += rhs;
        return d;
    }
};

template<typename Derived, typename Argument>
class make_subtractable {
    friend Derived operator-(const Derived& lhs, const Argument& rhs) {
        Derived d(lhs);
        d -= rhs;
        return d;
    }
};

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_OPERATORS_HPP
