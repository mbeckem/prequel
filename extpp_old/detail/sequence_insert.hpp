#ifndef EXTPP_DETAIL_SEQUENCE_INSERT_HPP
#define EXTPP_DETAIL_SEQUENCE_INSERT_HPP

#include <extpp/assert.hpp>
#include <extpp/defs.hpp>

namespace extpp {
namespace detail {

/// Insert a value into a sequence and perform a split at the same time.
/// Values exist in `left`, and `right` is treated as empty.
/// After the insertion, exactly `mid` entries will remain in `left` and the remaining
/// entries will have been copied over into `right`.
///
/// \param left             The left sequence.
/// \param right            The right sequence.
/// \param count            The current size of the left sequence, without the new element.
/// \param mid              The target size of the left sequence, after the split.
/// \param insert_index     The target insertion index of `value` in the left sequence.
/// \param value            The value to insert.
///
/// \pre `0 <= insert_index <= count`.
/// \pre `mid > 0 && mid <= count`.
///
/// \post If `insert_index < mid`, then the new value will be stored in the left sequence at index `insert_index`.
/// Otherwise, the value will be located in the right sequence, at index `insert_index - mid`.
///
/// \note This function does not apply the new size to either sequence, it only moves elements.
template<typename Left, typename Right, typename T>
void sequence_insert(Left left, Right right, u32 count, u32 mid, u32 insert_index, const T& value) {
    EXTPP_ASSERT(mid > 0 && mid <= count, "index can't be used as mid");
    EXTPP_ASSERT(insert_index <= count, "index out of bounds");
    if (insert_index < mid) {
        u32 i = 0, j = mid - 1;
        for (; j < count; ++i, ++j)
            right[i] = left[j];

        i = mid - 1, j = mid - 2;
        for (; i > insert_index; --i, --j)
            left[i] = left[j];
        left[i] = value;
    } else {
        insert_index -= mid;

        u32 i = 0, j = mid;
        for (; i < insert_index; ++i, ++j)
            right[i] = left[j];
        right[i++] = value;
        for (; j < count; ++i, ++j)
            right[i] = left[j];
    }
}

} // namespace detail
} // namespace extpp

#endif // EXTPP_DETAIL_SEQUENCE_INSERT_HPP
