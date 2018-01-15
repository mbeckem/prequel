#ifndef EXTPP_BTREE_VERIFY_HPP
#define EXTPP_BTREE_VERIFY_HPP

#include <extpp/defs.hpp>

#include <memory>
#include <stdexcept>

namespace extpp::btree_detail {

template<typename State>
void verify(const State& state) {
    using leaf_type = typename State::leaf_type;
    using internal_type = typename State::internal_type;

    using node_address = typename State::node_address;
    using key_type = typename State::key_type;

    static constexpr u32 min_values = leaf_type::min_size();
    static constexpr u32 max_values = leaf_type::max_size();
    static constexpr u32 min_children = internal_type::min_size();
    static constexpr u32 max_children = internal_type::max_size();

    struct context {
        u32 level = 0;
        node_address parent;
        key_type* lower = nullptr;  /// All values or keys must be greater.
        key_type* upper = nullptr;  /// All values or keys must be lesser or equal.
    };

    struct checker {
        const State& state;
        const typename State::anchor* anchor;
        leaf_type last_leaf;
        u64 value_count = 0;
        u64 leaf_count = 0;
        u64 internal_count = 0;

        checker(const State& state)
            : state(state)
            , anchor(state.get_anchor().get())
        {}

        void run() {
            if (anchor->height != 0) {
                if (!anchor->root)
                    error("non-empty state does not have a root");

                context root_ctx;
                root_ctx.level = state.get_anchor()->height - 1;
                check(root_ctx, anchor->root);
            }

            if (value_count != anchor->size)
                error("value count does not match the state's size");
            if (leaf_count != anchor->leaves)
                error("wrong of leaves");
            if (internal_count != anchor->internals)
                error("wrong number of internal nodes");
            if (last_leaf.address() != anchor->rightmost)
                error("last leaf is not the rightmost one");
        }

        void check(const context& ctx, node_address node_address) {
            if (!node_address)
                error("invalid node index");

            if (ctx.level == 0) {
                check(ctx, state.access(state.cast_leaf(node_address)));
            } else {
                check(ctx, state.access(state.cast_internal(node_address)));
            }
        }

        void check(const context& ctx, const leaf_type& leaf) {
            ++leaf_count;

            if (last_leaf) {
                if (leaf.prev() != last_leaf.address())
                    error("current leaf does not point to its predecessor");
                if (last_leaf.next() != leaf.address())
                    error("last leaf does not point to its successor");
            } else {
                if (anchor->leftmost != leaf.address())
                    error("first leaf is not the leftmost leaf");
                if (leaf.prev())
                    error("the first leaf has a predecessor");
            }

            u32 size = leaf.size();
            if (size == 0)
                error("leaf is empty");
            if (size < min_values
                    && leaf.address() != anchor->root
                    && leaf.address() != anchor->leftmost
                    && leaf.address() != anchor->rightmost)
                error("leaf is underflowing");
            if (size > max_values)
                error("leaf is overflowing");

            for (u32 i = 0; i < size; ++i) {
                check_key(ctx, state.key(leaf.get(i)));
                if (i > 0 && !state.key_less(
                            state.key(leaf.get(i-1)),
                            state.key(leaf.get(i)))) {
                    error("leaf entries are not sorted");
                }
            }

            value_count += size;
            last_leaf = leaf;
        }

        void check(const context& ctx, const internal_type& internal) {
            ++internal_count;

            u32 size = internal.size();
            if (size < min_children && internal.address() != anchor->root)
                error("internal node is underflowing");
            if (size < 2 && internal.address() == anchor->root)
                error("root is too empty");
            if (size > max_children)
                error("internal node is overflowing");

            key_type last_key = internal.get_key(0);
            check_key(ctx, last_key);

            context child_ctx;
            child_ctx.parent = internal.address();
            child_ctx.level = ctx.level - 1;
            child_ctx.lower = ctx.lower;
            child_ctx.upper = std::addressof(last_key);
            check(child_ctx, internal.get_child(0));

            for (u32 i = 1; i < size - 1; ++i) {
                key_type current_key = internal.get_key(i);
                check_key(ctx, current_key);

                if (!state.key_less(last_key, current_key)) {
                    error("internal node entries are not sorted");
                }

                child_ctx.lower = std::addressof(last_key);
                child_ctx.upper = std::addressof(current_key);
                check(child_ctx, internal.get_child(i));
                last_key = current_key;
            }

            child_ctx.lower = std::addressof(last_key);
            child_ctx.upper = ctx.upper;
            check(child_ctx, internal.get_child(size - 1));
        }

        void check_key(const context& ctx, const key_type& key) {
            if (ctx.lower && !state.key_less(*ctx.lower, key))
                error("key is not greater than the lower bound");
            if (ctx.upper && state.key_less(*ctx.upper, key))
                error("key greater than the upper bound");
        }

        void error(const char* message) {
            // TODO Own exception type?
            throw std::logic_error(std::string("verify(): invariant violated (") + message + ").");
        }
    };

    checker(state).run();
}

} // namespace extpp::btree_detail

#endif // EXTPP_BTREE_VERIFY_HPP
