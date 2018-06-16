#include <extpp/id_generator.hpp>

#include <fmt/ostream.h>

#include <optional>
#include <ostream>

namespace extpp {

id_generator::id_generator(anchor_handle<anchor> _anchor, allocator& _alloc)
    : m_anchor(std::move(_anchor))
    , m_tree(m_anchor.member<&anchor::tree>(), _alloc)
{}

id_generator::value_type id_generator::max() const {
    return m_anchor.get<&anchor::max>();
}

id_generator::value_type id_generator::allocate() {
    if (m_tree.empty()) {
        value_type current_max = max();
        if (max() == std::numeric_limits<value_type>::max()) {
            EXTPP_THROW(exception("ID space exhausted."));
        }

        value_type id = current_max + 1;
        m_anchor.set<&anchor::max>(id);
        return id;
    }

    return pop_one();
}

void id_generator::free(value_type id) {
    EXTPP_CHECK(id > 0 && id <= max(), "Invalid id.");

    // Highest interval with id < end, i.e. right neighbor to ID.
    interval_cursor right = m_tree.lower_bound(id);

    // Left neighbor.
    interval_cursor left;
    if (right) {
        left = right;
        left.move_prev();
    } else {
        left = m_tree.create_cursor(m_tree.seek_max);
    }


    std::optional<interval> left_interval;
    if (left) {
        left_interval = left.get();
    }
    if (left_interval && left_interval->end >= id) {
        EXTPP_THROW(invalid_argument("ID has already been freed."));
    }

    std::optional<interval> right_interval;
    if (right) {
        right_interval = right.get();
    }
    if (right_interval && right_interval->begin <= id) {
        EXTPP_THROW(invalid_argument("ID has already been freed."));
    }

    EXTPP_ASSERT(!left_interval || !right_interval || left_interval->end < right_interval->begin,
                 "Intervals are ordered and do not overlap.");



    // Merge with neighbors if possible.
    interval range(id, id);
    if (left_interval && left_interval->end == id - 1) {
        range.begin = left_interval->begin;
        left.erase();
    }
    if (right_interval && right_interval->begin == id + 1) {
        range.end = right_interval->end;
        right.erase();
    }

    if (range.end == max()) {
        m_anchor.set<&anchor::max>(range.begin - 1);
    } else {
        bool inserted;
        std::tie(std::ignore, inserted) = m_tree.insert(range);
        unused(inserted);
        EXTPP_ASSERT(inserted, "Interval must have been inserted.");
    }
}

void id_generator::dump(std::ostream& out) const {
    fmt::print(out, "Max: {}\n", max());
    fmt::print(out, "\n");

    fmt::print(out, "Free intervals:\n");
    for (interval_cursor c = m_tree.create_cursor(m_tree.seek_min); c; c.move_next()) {
        interval i = c.get();
        fmt::print(out, "- [{}, {}]\n", i.begin, i.end);
    }
}

id_generator::value_type id_generator::pop_one() {
    EXTPP_ASSERT(!m_tree.empty(), "Tree must not be empty.");
    interval_cursor cursor = m_tree.create_cursor(m_tree.seek_min);

    interval range = cursor.get();
    value_type result = range.begin;

    if (range.begin != range.end) {
        range.begin += 1;
        cursor.set(range);
    } else {
        cursor.erase();
    }
    return result;
}

} // namespace extpp
