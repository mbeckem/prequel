#ifndef EXTPP_IDENTITY_KEY_HPP
#define EXTPP_IDENTITY_KEY_HPP

namespace extpp {

struct identity_key {
    template<typename Key>
    Key operator()(const Key& k) const {
        return k;
    }
};

} // namespace extpp

#endif // EXTPP_IDENTITY_KEY_HPP
