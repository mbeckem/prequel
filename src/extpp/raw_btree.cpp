#include <extpp/raw_btree.hpp>

#include <extpp/exception.hpp>
#include <extpp/btree/cursor.hpp>
#include <extpp/btree/internal_node.hpp>
#include <extpp/btree/leaf_node.hpp>
#include <extpp/btree/loader.hpp>
#include <extpp/btree/tree.hpp>

#include <extpp/btree/cursor.ipp>
#include <extpp/btree/internal_node.ipp>
#include <extpp/btree/leaf_node.ipp>
#include <extpp/btree/loader.ipp>
#include <extpp/btree/tree.ipp>

namespace extpp {

raw_btree::raw_btree(anchor_handle<anchor> _anchor, const raw_btree_options& options, allocator& alloc)
    : m_impl(std::make_unique<detail::btree_impl::tree>(std::move(_anchor), options, alloc))
{}

raw_btree::~raw_btree() {}

raw_btree::raw_btree(raw_btree&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree& raw_btree::operator=(raw_btree&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

engine& raw_btree::get_engine() const { return impl().get_engine(); }
allocator& raw_btree::get_allocator() const { return impl().get_allocator(); }

u32 raw_btree::value_size() const { return impl().value_size(); }
u32 raw_btree::key_size() const { return impl().key_size(); }
u32 raw_btree::internal_node_capacity() const { return impl().internal_node_max_children(); }
u32 raw_btree::leaf_node_capacity() const { return impl().leaf_node_max_values(); }
bool raw_btree::empty() const { return impl().empty(); }
u64 raw_btree::size() const { return impl().size(); }
u32 raw_btree::height() const { return impl().height(); }
u64 raw_btree::internal_nodes() const { return impl().internal_nodes(); }
u64 raw_btree::leaf_nodes() const { return impl().leaf_nodes(); }
u64 raw_btree::nodes() const { return internal_nodes() + leaf_nodes(); }

double raw_btree::fill_factor() const {
    return empty() ? 0 : double(size()) / (leaf_nodes() * leaf_node_capacity());
}

u64 raw_btree::byte_size() const { return nodes() * get_engine().block_size(); }

double raw_btree::overhead() const {
    return empty() ? 0 : double(byte_size()) / (size() * value_size());
}

raw_btree_cursor raw_btree::create_cursor(raw_btree::cursor_seek_t seek) const {
    return raw_btree_cursor(impl().create_cursor(seek));
}

raw_btree_cursor raw_btree::find(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.find(key);
    return c;
}

raw_btree_cursor raw_btree::lower_bound(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.lower_bound(key);
    return c;
}

raw_btree_cursor raw_btree::upper_bound(const byte* key) const {
    auto c = create_cursor(raw_btree::seek_none);
    c.upper_bound(key);
    return c;
}

raw_btree::insert_result raw_btree::insert(const byte* value) {
    auto c = create_cursor(raw_btree::seek_none);
    bool inserted = c.insert(value);
    return insert_result(std::move(c), inserted);
}

raw_btree::insert_result raw_btree::insert_or_update(const byte* value) {
    auto c = create_cursor(raw_btree::seek_none);
    bool inserted = c.insert_or_update(value);
    return insert_result(std::move(c), inserted);
}

void raw_btree::reset() { impl().clear(); }
void raw_btree::clear() { impl().clear(); }

raw_btree_loader raw_btree::bulk_load() {
    return raw_btree_loader(impl().bulk_load());
}

void raw_btree::dump(std::ostream& os) const { return impl().dump(os); }

raw_btree::node_view::~node_view() {}

void raw_btree::visit(bool (*visit_fn)(const node_view& node, void* user_data), void* user_data) const {
    return impl().visit(visit_fn, user_data);
}

void raw_btree::validate() const { return impl().validate(); }

detail::btree_impl::tree& raw_btree::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_operation("Invalid tree instance."));
    return *m_impl;
}

// --------------------------------
//
//   Cursor public interface
//
// --------------------------------

raw_btree_cursor::raw_btree_cursor()
{}

raw_btree_cursor::raw_btree_cursor(std::unique_ptr<detail::btree_impl::cursor> impl)
    : m_impl(std::move(impl))
{}

raw_btree_cursor::raw_btree_cursor(const raw_btree_cursor& other)
{
    if (other.m_impl) {
        if (!m_impl || m_impl->tree() != other.m_impl->tree()) {
            m_impl = std::make_unique<detail::btree_impl::cursor>(other.m_impl->tree());
        }
        m_impl->copy(*other.m_impl);
    }
}

raw_btree_cursor::raw_btree_cursor(raw_btree_cursor&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree_cursor::~raw_btree_cursor() {}

raw_btree_cursor& raw_btree_cursor::operator=(const raw_btree_cursor& other) {
    if (this != &other) {
        if (!other.m_impl) {
            m_impl.reset();
        } else {
            if (!m_impl || m_impl->tree() != other.m_impl->tree()) {
                m_impl = std::make_unique<detail::btree_impl::cursor>(other.m_impl->tree());
            }
            m_impl->copy(*other.m_impl);
        }
    }
    return *this;
}

raw_btree_cursor& raw_btree_cursor::operator=(raw_btree_cursor&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

detail::btree_impl::cursor& raw_btree_cursor::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_cursor("Invalid cursor."));
    return *m_impl;
}

u32 raw_btree_cursor::value_size() const { return impl().value_size(); }
u32 raw_btree_cursor::key_size() const { return impl().key_size(); }

bool raw_btree_cursor::at_end() const { return !m_impl || impl().at_end(); }
bool raw_btree_cursor::erased() const { return m_impl && impl().erased(); }

void raw_btree_cursor::reset() { return impl().reset_to_invalid(); }
bool raw_btree_cursor::move_min() { return impl().move_min(); }
bool raw_btree_cursor::move_max() { return impl().move_max(); }
bool raw_btree_cursor::move_next() { return impl().move_next(); }
bool raw_btree_cursor::move_prev() { return impl().move_prev(); }

bool raw_btree_cursor::lower_bound(const byte* key) { return impl().lower_bound(key); }
bool raw_btree_cursor::upper_bound(const byte* key) { return impl().upper_bound(key); }
bool raw_btree_cursor::find(const byte* key) { return impl().find(key); }
bool raw_btree_cursor::insert(const byte* value) { return impl().insert(value, false); }
bool raw_btree_cursor::insert_or_update(const byte* value) { return impl().insert(value, true); }
void raw_btree_cursor::erase() { impl().erase(); }

void raw_btree_cursor::set(const byte* data) { return impl().set(data); }
const byte* raw_btree_cursor::get() const { return impl().get(); }

void raw_btree_cursor::validate() const { return impl().validate(); }

bool raw_btree_cursor::operator==(const raw_btree_cursor& other) const {
    if (!m_impl) {
        return !other.m_impl || other.impl().at_end();
    }
    if (!other.m_impl) {
        return impl().at_end();
    }
    return impl() == other.impl();
}


// --------------------------------
//
//   Loader public interface
//
// --------------------------------

raw_btree_loader::raw_btree_loader(std::unique_ptr<detail::btree_impl::loader> impl)
    : m_impl(std::move(impl))
{
    EXTPP_ASSERT(m_impl, "Invalid impl pointer.");
}

raw_btree_loader::~raw_btree_loader() {}

raw_btree_loader::raw_btree_loader(raw_btree_loader&& other) noexcept
    : m_impl(std::move(other.m_impl))
{}

raw_btree_loader& raw_btree_loader::operator=(raw_btree_loader&& other) noexcept {
    if (this != &other) {
        m_impl = std::move(other.m_impl);
    }
    return *this;
}

detail::btree_impl::loader& raw_btree_loader::impl() const {
    if (!m_impl)
        EXTPP_THROW(bad_operation("Invalid loader instance."));
    return *m_impl;
}

void raw_btree_loader::insert(const byte* value) {
    insert(value, 1);
}

void raw_btree_loader::insert(const byte* values, size_t count) {
    return impl().insert(values, count);
}

void raw_btree_loader::finish() {
    impl().finish();
}

void raw_btree_loader::discard() {
    impl().discard();
}

} // namespace extpp
