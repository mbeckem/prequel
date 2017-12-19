#ifndef EXTPP_HEAP_SEGREGATED_FREE_LIST_HPP
#define EXTPP_HEAP_SEGREGATED_FREE_LIST_HPP

#include <extpp/address.hpp>
#include <extpp/allocator.hpp>
#include <extpp/anchor_ptr.hpp>
#include <extpp/btree.hpp>
#include <extpp/defs.hpp>
#include <extpp/engine.hpp>
#include <extpp/identity_key.hpp>
#include <extpp/stream.hpp>
#include <extpp/heap/base.hpp>

#include <fmt/ostream.h>

#include <array>
#include <vector>

namespace extpp::heap_detail {

// TODO: Pick large contiguous regions for bump-ptr allocations.
template<u32 BlockSize>
class segregated_free_list {
    using cell_address_t = address<cell, BlockSize>;
    using raw_address_t = raw_address<BlockSize>;

    // The free list for index `i` contains cell ranges
    // of sizes `size_classes[i], ..., size_classes[i+1] - 1`.
    static constexpr std::array<u16, 16> size_classes{
        1,  2,   3,   4,
        6,  8,   12,  16,
        24, 32,  48,  64,
        96, 128, 192, 256
    };

    struct list_node {
        address<list_node, BlockSize> next;
        u64 size;

        list_node(address<list_node, BlockSize> next, u64 size)
            : next(next), size(size) {}
    };

    struct list_header {
        address<list_node, BlockSize> head, tail;
    };

    using stream_type = stream<list_header, BlockSize>;

    struct cell_range {
        cell_address_t addr;
        u64 size = 0;

        cell_range() = default;
        cell_range(cell_address_t addr, u64 size)
            : addr(addr)
            , size(size)
        {}

        // Sort entries by size, break ties using the address.
        struct best_fit_order {
            bool operator()(const cell_range& lhs, const cell_range& rhs) const {
                if (lhs.size != rhs.size)
                    return lhs.size < rhs.size;
                return lhs.addr < rhs.addr;
            }
        };

        bool operator==(const cell_range& other) const {
            return addr == other.addr && size == other.size;
        }
    };

    using free_tree = extpp::btree<cell_range, identity_key, typename cell_range::best_fit_order, BlockSize>;
    using free_tree_iterator = typename free_tree::iterator;

public:
    class anchor {
        typename stream_type::anchor lists;
        typename free_tree::anchor tree;

        friend class segregated_free_list;
    };

public:
    segregated_free_list(anchor_ptr<anchor> anc, allocator<BlockSize>& alloc)
        : m_lists_headers(anc.member(&anchor::lists), alloc)
        , m_large_ranges(anc.member(&anchor::tree), alloc)
    {
        m_lists_headers.growth(linear_growth(1));
        if (m_lists_headers.empty()) {
            m_lists_headers.resize(size_classes.size() - 1);
        }
        for (auto i = m_lists_headers.begin(), e = m_lists_headers.end(); i != e; ++i)
            m_lists.push_back(m_lists_headers.pointer_to(i));
    }

    void clear() {
        for (auto& i : m_lists) {
            *i = list_header();
            i.dirty();
        }
        m_large_ranges.clear();
    }

    void free(cell_address_t cell, u64 size) {
        EXTPP_ASSERT(cell, "Cell pointer must be valid.");
        EXTPP_ASSERT(size > 0, "Invalid region size.");

        const size_t sc = size_class_index(size);
        EXTPP_ASSERT(sc < size_classes.size(), "Invalid size class.");
        EXTPP_ASSERT(size_classes[sc] <= size, "Size class invariant.");
        EXTPP_ASSERT(sc == size_classes.size() - 1 || size_classes[sc+1] > size, "Size class invariant.");

        if (sc == size_classes.size() - 1) {
            insert_large_run({cell, size});
        } else {
            insert_into_list(sc, {cell, size});
        }
    }

    cell_address_t allocate(u64 size) {
        cell_range range = get_free(size);
        if (!range.addr)
            return {};

        EXTPP_ASSERT(range.size >= size, "Range does not satisfy the size request.");
        if (range.size > size) {
            free(range.addr + size, range.size - size);
        }
        return range.addr;
    }

    void debug_stats(std::ostream& out) {
        for (size_t i = 0; i < size_classes.size() - 1; ++i) {
            auto& ls = m_lists[i];

            fmt::print(out, "Size [{}, {}):\n", size_classes[i], size_classes[i+1]);

            for (address<list_node, BlockSize> addr = ls->head; addr;) {
                auto node = access(get_engine(), addr);
                fmt::print(out, "  - Cell {}, Size {}\n", addr.raw(), node->size);
                addr = node->next;
            }

            fmt::print(out, "\n");
        }

        fmt::print(out, "Size [{}, INF):\n", size_classes.back());
        for (const cell_range& r : m_large_ranges) {
            fmt::print(out, "  - Cell {}, Size {}\n", r.addr.raw(), r.size);
        }
    }

private:
    void insert_into_list(size_t index, cell_range range) {
        EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");
        static_assert(cell_size >= sizeof(list_node), "Cannot store a list node in a cell.");

        auto new_tail = access(get_engine(), raw_address_cast<list_node>(range.addr.raw()));
        new (new_tail.get()) list_node({}, range.size);
        new_tail.dirty();

        auto& ls = m_lists[index];
        if (!ls->head) {
            EXTPP_ASSERT(!ls->tail, "Tail must be invalid too.");
            ls->head = ls->tail = new_tail.address();
            ls.dirty();
        } else {
            auto old_tail = access(get_engine(), ls->tail);
            old_tail->next = new_tail.address();
            old_tail.dirty();
            ls->tail = new_tail.address();
            ls.dirty();
        }
    }

    cell_range remove_list_head(size_t index) {
        EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");

        auto& ls = m_lists[index];
        if (!ls->head)
            return {};

        auto head_node = access(get_engine(), ls->head);
        auto cell_addr = raw_address_cast<cell>(ls->head);

        ls->head = head_node->next;
        if (!ls->head)
            ls->tail = {};
        ls.dirty();

        return {cell_addr, head_node->size};
    }

    /// Scan through the entire list and try to find a large enough range.
    cell_range remove_first_fit(size_t index, u64 size) {
        EXTPP_ASSERT(index >= 0 && index < m_lists.size(), "Invalid list index.");
        EXTPP_ASSERT(size_classes[index] <= size && size_classes[index+1] > size, "Wrong size class.");

        auto& ls = m_lists[index];

        address<list_node, BlockSize> prev_addr;
        address<list_node, BlockSize> curr_addr = ls->head;
        handle<list_node, BlockSize> prev_node;
        while (curr_addr) {
            handle<list_node, BlockSize> curr_node = access(get_engine(), curr_addr);
            if (curr_node->size >= size) {
                // The node is large enough, unlink it.
                if (prev_addr) {
                    prev_node->next = curr_node->next;
                    prev_node.dirty();
                } else {
                    ls->head = curr_node->next;
                    ls.dirty();
                }

                if (curr_addr == ls->tail) {
                    ls->tail = prev_addr;
                    ls.dirty();
                }
                return {raw_address_cast<cell>(curr_addr), curr_node->size};
            }

            prev_addr = curr_addr;
            curr_addr = curr_node->next;
            prev_node = std::move(curr_node);
        }
        return {};
    }

    /// Find the index of the list that contains blocks of the given size.
    size_t size_class_index(u64 size) {
        // First size class with sz > size.
        auto p = std::upper_bound(size_classes.begin(), size_classes.end(), size);
        EXTPP_ASSERT(p != size_classes.begin(), "Impossible."); // First size class is 1.
        p--; // sz <= size.
        return p - size_classes.begin();
    }

    /// Find a free range of the given size or more (if possible) and remove it
    /// from its datastructure.
    cell_range get_free(u64 size) {
        if (size >= size_classes.back()) {
            if (auto pos = find_large_run(size); pos != m_large_ranges.end()) {
                cell_range result = *pos;
                m_large_ranges.erase(pos);
                return result;
            }
            return {};
        }

        // Test all segmented lists that are guaranteed
        // to satisfy the request (if they're not empty).
        const size_t si = size_class_index(size);
        const size_t sj = size_classes[si] == size ? si : si + 1;
        for (size_t i = sj; i < m_lists.size(); ++i) {
            if (cell_range range = remove_list_head(i); range.addr)
                return range;
        }

        // Any large object would be big enough.
        if (auto pos = m_large_ranges.begin(); pos != m_large_ranges.end()) {
            cell_range range = *pos;
            m_large_ranges.erase(pos);
            return range;
        }

        // Otherwise, do a first fit search in the list that *might*
        // be able to satisfy the request. We may have already visited the list above.
        if (si != sj) {
            if (cell_range range = remove_first_fit(si, size); range.addr)
                return range;
        }
        return {};
    }

    void insert_large_run(cell_range range) {
        EXTPP_ASSERT(range.size >= size_classes.back(),
                     "Range is not large enough.");

        free_tree_iterator pos;
        bool inserted;
        std::tie(pos, inserted) = m_large_ranges.insert(range);
        EXTPP_ASSERT(inserted, "Entry was not inserted.");
    }

    free_tree_iterator find_large_run(u64 size) {
        if (size <= size_classes.back())
            return m_large_ranges.begin();

        cell_range entry;
        entry.size = size;
        entry.addr = raw_address_cast<cell>(raw_address_t(0, 0));
        return m_large_ranges.lower_bound(entry);
    }

    engine<BlockSize>& get_engine() const { return m_lists_headers.get_engine(); }

private:
    /// Stores the linked list headers.
    stream_type m_lists_headers;

    /// Pins the headers in main memory.
    std::vector<handle<list_header, BlockSize>> m_lists;

    /// Ordered tree for very large regions.
    free_tree m_large_ranges;
};

} // namespace extpp::heap_detail

#endif // EXTPP_HEAP_SEGREGATED_FREE_LIST_HPP
