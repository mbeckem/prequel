#ifndef PREQUEL_CONTAINER_INDEXING_HPP
#define PREQUEL_CONTAINER_INDEXING_HPP

#include <prequel/defs.hpp>
#include <prequel/type_traits.hpp>

namespace prequel {

/**
 * For types that are indexed by themselves, for example
 * integers when stored in a set.
 */
struct indexed_by_identity {
    template<typename T>
    T operator()(const T& value) const {
        return value;
    }
};

/**
 * For types that are indexed by a member of that type, for example
 * a object that stores its own id.
 *
 * The single template argument must a member pointer, e.g.
 *
 *      struct entry {
 *          i32 id;
 *          i32 value;
 *
 *          // .. binary format
 *      };
 *
 *      btree<entry, indexed_by_member<&entry::id>> tree = ...;
 *
 * declares a btree that indexes its values according to their id.
 */
template<auto MemberPtr>
struct indexed_by_member {
    using object_type = object_type_t<decltype(MemberPtr)>;
    using key_type = member_type_t<decltype(MemberPtr)>;

    key_type operator()(const object_type& obj) const {
        return obj.*MemberPtr;
    }
};

} // namespace prequel

#endif // PREQUEL_CONTAINER_INDEXING_HPP
