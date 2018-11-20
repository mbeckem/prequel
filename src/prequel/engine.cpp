#include <prequel/engine.hpp>

#include <prequel/detail/deferred.hpp>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive/set_hook.hpp>

namespace prequel {

namespace detail {

// This is the real implementation of block handles,
// it contains the additional boost intrusive anchors.
//
// The index of a block handle instance MUST NOT change because
// it is used for indexing by the block handle manager.
//
// TODO: Maybe switch to intrusive hash table instead?
class block_handle_internal final : public block_handle_base {
    friend engine;
    friend block_handle_manager;

    // Block handles are linked together using the block index field.
    boost::intrusive::set_member_hook<> index_hook;

    // Linked together in a free list.
    boost::intrusive::list_member_hook<> freelist_hook;

    void reset() {
        m_refcount = 0;
        m_engine = nullptr;
        m_index = block_index();
        m_data = nullptr;
        m_cookie = 0;
    }
};

class block_handle_manager {
public:
    block_handle_manager() = default;

    ~block_handle_manager() {
        m_freelist.clear_and_dispose([](block_handle_internal* handle) { delete handle; });
    }

    block_handle_manager(const block_handle_manager&) = delete;
    block_handle_manager& operator=(const block_handle_manager&) = delete;

    auto begin() { return m_handles.begin(); }
    auto end() { return m_handles.end(); }

    void insert(block_handle_internal& handle) noexcept {
        auto result = m_handles.insert(handle);
        PREQUEL_ASSERT(result.second, "Insertion must have succeeded.");
        unused(result);
    }

    void remove(block_handle_internal& handle) noexcept {
        PREQUEL_ASSERT(contains(handle), "Block handle is not linked.");
        m_handles.erase(m_handles.iterator_to(handle));
    }

    bool contains(block_handle_internal& handle) const noexcept {
        return handle.index_hook.is_linked();
    }

    block_handle_internal* find(block_index index) noexcept {
        auto iterator = m_handles.find(index);
        if (iterator == m_handles.end())
            return nullptr;
        return &*iterator;
    }

    size_t size() const noexcept { return m_handles.size(); }

    block_handle_internal* allocate() {
        if (!m_freelist.empty()) {
            block_handle_internal* handle = &m_freelist.front();
            m_freelist.pop_front();
            return handle;
        }
        return new block_handle_internal();
    }

    void free(block_handle_internal* handle) {
        static constexpr u32 max = 1024;

        if (!handle)
            return;

        if (m_freelist.size() >= max) {
            delete handle;
            return;
        }

        handle->reset();
        m_freelist.push_front(*handle);
    }

private:
    struct block_handle_index {
        using type = block_index;

        block_index operator()(const block_handle_base& handle_base) const noexcept {
            return handle_base.index();
        }
    };

    using set_t = boost::intrusive::set<
        block_handle_internal,
        boost::intrusive::member_hook<block_handle_internal, boost::intrusive::set_member_hook<>,
                                      &block_handle_internal::index_hook>,
        boost::intrusive::key_of_value<block_handle_index>>;

    using freelist_t = boost::intrusive::list<
        block_handle_internal,
        boost::intrusive::member_hook<block_handle_internal, boost::intrusive::list_member_hook<>,
                                      &block_handle_internal::freelist_hook>>;

private:
    set_t m_handles;
    freelist_t m_freelist;
};

} // namespace detail

engine::engine(u32 block_size)
    : m_block_size(block_size)
    , m_handle_manager(new detail::block_handle_manager()) {
    if (!is_pow2(block_size)) {
        PREQUEL_THROW(bad_argument(fmt::format("Block size is not a power of two: {}.", block_size)));
    }
    m_block_size_log = log2(block_size);
    m_offset_mask = m_block_size - 1;
}

engine::~engine() {
    if (handle_manager().size() > 0) {
        PREQUEL_ABORT("There are still block handles in use while the engine is being destroyed.");
    }
}

u64 engine::size() const {
    return do_size();
}

void engine::grow(u64 n) {
    if (n > 0) {
        do_grow(n);
    }
}

void engine::flush() {
    do_flush();
}

block_handle engine::read(block_index index) {
    if (!index.valid()) {
        PREQUEL_THROW(bad_argument("Invalid block index."));
    }
    return internal_populate_handle(index, initialize_block_t{});
}

block_handle engine::overwrite_zero(block_index index) {
    if (!index.valid()) {
        PREQUEL_THROW(bad_argument("Invalid block index."));
    }
    return internal_populate_handle(index, initialize_zero_t{});
}

block_handle engine::overwrite(block_index index, const byte* data, size_t size) {
    if (!index.valid()) {
        PREQUEL_THROW(bad_argument("Invalid block index."));
    }
    if (size < m_block_size) {
        PREQUEL_THROW(bad_argument(
            fmt::format("Buffer not large enough ({} byte given but blocks are {} byte large).",
                        size, m_block_size)));
    }
    return internal_populate_handle(index, initialize_data_t{data});
}

template<typename Initializer>
block_handle engine::internal_populate_handle(block_index index, Initializer&& init) {
    static constexpr bool overwrite =
        !std::is_same_v<remove_cvref_t<Initializer>, initialize_block_t>;
    PREQUEL_ASSERT(index.valid(), "Invalid index.");

    auto& manager = handle_manager();

    // We might have a handle to this block already.
    if (auto handle = manager.find(index)) {
        PREQUEL_ASSERT(handle->m_refcount > 0,
                       "The handle must be referenced "
                       "if it can be found through the manager.");

        if constexpr (overwrite) {
            init.apply(handle->m_data, m_block_size);
            do_dirty(index, handle->m_cookie);
        }

        return block_handle(handle);
    }

    // Prepare a handle for the new block.
    auto handle = manager.allocate();
    detail::deferred cleanup_handle = [&] { manager.free(handle); };

    // Read the block from disk. Don't initialize the contents
    // if we're about to overwrite them anyway.
    pin_result pinned = do_pin(index, !overwrite);
    detail::deferred cleanup_pin = [&] { do_unpin(index, pinned.cookie); };

    // Initialize the block contents.
    if constexpr (overwrite) {
        init.apply(pinned.data, m_block_size);
        do_dirty(index, pinned.cookie);
    }

    handle->m_engine = this;
    handle->m_index = index;
    handle->m_data = pinned.data;
    handle->m_cookie = pinned.cookie;
    manager.insert(*handle);

    cleanup_pin.disable();
    cleanup_handle.disable();
    return block_handle(handle);

    (void) init; // unused warning :/
}

void engine::internal_flush_handle(detail::block_handle_base* base) {
    PREQUEL_ASSERT(base, "Invalid handle instance.");
    PREQUEL_ASSERT(base->get_engine() == this, "Handle does not belong to this engine.");

    detail::block_handle_internal* handle = static_cast<detail::block_handle_internal*>(base);
    PREQUEL_ASSERT(handle_manager().contains(*handle), "Handle is not managed by this instance.");
    do_flush(handle->index(), handle->m_cookie);
}

void engine::internal_dirty_handle(detail::block_handle_base* base) {
    PREQUEL_ASSERT(base, "Invalid handle instance.");
    PREQUEL_ASSERT(base->get_engine() == this, "Handle does not belong to this engine.");

    detail::block_handle_internal* handle = static_cast<detail::block_handle_internal*>(base);
    PREQUEL_ASSERT(handle_manager().contains(*handle), "Handle is not managed by this instance.");
    do_dirty(handle->index(), handle->m_cookie);
}

void engine::internal_release_handle(detail::block_handle_base* base) noexcept {
    PREQUEL_ASSERT(base, "Invalid handle instance.");
    PREQUEL_ASSERT(base->get_engine() == this, "Handle does not belong to this engine.");

    detail::block_handle_internal* handle = static_cast<detail::block_handle_internal*>(base);
    do_unpin(handle->index(), handle->m_cookie);

    auto& manager = handle_manager();
    manager.remove(*handle);
    manager.free(handle);
}

detail::block_handle_manager& engine::handle_manager() const {
    PREQUEL_ASSERT(m_handle_manager, "Invalid block handle manager instance.");
    return *m_handle_manager;
}

} // namespace prequel
