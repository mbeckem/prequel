#ifndef EXTPP_BTREE_SAFE_ITERATOR_HPP
#define EXTPP_BTREE_SAFE_ITERATOR_HPP

#include <extpp/address.hpp>

#include <boost/iterator/iterator_adaptor.hpp>

#include <map>

namespace extpp::btree_detail {

template<typename Container, typename BaseIterator, typename Derived>
class safe_iterator_base;

template<typename Container, typename Iterator>
class safe_iterator_map;

template<typename Container, typename BaseIterator, typename Derived>
class safe_iterator_base : public boost::iterator_adaptor<
        safe_iterator_base<Container, BaseIterator, Derived>,
        BaseIterator,
        boost::use_default,
        boost::bidirectional_traversal_tag
    >
{
    using adaptor = typename safe_iterator_base::iterator_adaptor;

public:
    using map_type = safe_iterator_map<Container, Derived>;

public:
    safe_iterator_base() {}

    safe_iterator_base(map_type& map, BaseIterator iter)
        : adaptor(std::move(iter))
        , m_map(&map)
    {
        register_self();
    }

    safe_iterator_base(const safe_iterator_base& other)
        : adaptor(other.base_reference())
        , m_map(other.m_map)
    {
        if (m_map)
            register_self();
    }

    safe_iterator_base(safe_iterator_base&& other) noexcept
        : adaptor(std::move(other.base_reference()))
        , m_map(std::exchange(other.m_map, nullptr))
    {
        if (m_map)
            replace_other(other);
    }

    ~safe_iterator_base() {
        if (m_map)
            unregister_self();
    }

    safe_iterator_base& operator=(const safe_iterator_base& other) {
        if (m_map)
            unregister_self();
        m_map = other.m_map;
        this->base_reference() = other.base_reference();
        if (m_map)
            register_self();
        return *this;
    }

    safe_iterator_base& operator=(safe_iterator_base&& other) noexcept {
        if (this == &other)
            return *this;

        if (m_map)
            unregister_self();
        m_map = std::exchange(other.m_map, nullptr);
        this->base_reference() = std::move(other.base_reference());
        if (m_map)
            replace_other(other);
        return *this;
    }

    /// True iff this iterator is still valid, i.e. it's element has not been deleted.
    bool valid() const { return m_map != nullptr; }

    explicit operator bool() const { return valid(); }

    /// Resets this iterator to its invalid state.
    void reset() {
        if (m_map) {
            unregister_self();
            m_map = nullptr;
            this->base_reference() = BaseIterator();
        }
    }

    /// Resets this iterator to a valid state.
    void reset(map_type& map, BaseIterator iter) {
        if (m_map)
            unregister_self();

        m_map = &map;
        this->base_reference() = std::move(iter);
        register_self();
    }

    const BaseIterator& iterator() const {
        check_valid();
        return this->base();
    }

    // TODO: Is an implicit conversion a good idea?
    operator const BaseIterator&() const {        
        return iterator();
    }

private:
    Derived& derived() { return *static_cast<Derived*>(this); }
    const Derived& derived() const { return *static_cast<const Derived*>(this); }

    void register_self() {
        m_map->register_iterator(this->base().address(), this->base().index(), &derived());
    }

    void replace_other(safe_iterator_base& other) {
        m_map->replace_iterator(this->base().address(), this->base().index(), &other.derived(), &derived());
    }

    void unregister_self() {
        m_map->unregister_iterator(this->base().address(), this->base().index(), &derived());
    }

private:
    friend class boost::iterator_core_access;

    void check_valid() const {
        EXTPP_CHECK(valid(),
                    "Accessing an invalid safe_iterator. Possible reasons are that the iterator "
                    "was default constructed or moved from, the element it was pointing to has been deleted or "
                    "the container went out of scope.");
    }

    void increment() {
        check_valid();
        unregister_self();
        this->base_reference()++;
        register_self();
    }

    void decrement() {
        check_valid();
        unregister_self();
        this->base_reference()--;
        register_self();
    }

    decltype(auto) dereference() const {
        check_valid();
        return *this->base();
    }

    bool equal(const safe_iterator_base& other) const {
        if (m_map != other.m_map)
            return false;
        if (!m_map)
            return true;
        return this->base() == other.base();
    }

    friend bool operator==(const safe_iterator_base& lhs, const safe_iterator_base& rhs) {
        if (lhs.valid() != rhs.valid())
            return false;
        if (lhs.valid())
            return lhs.base() == rhs.base();
        return true;
    }

    friend bool operator!=(const safe_iterator_base& lhs, const safe_iterator_base& rhs) {
        return !(lhs == rhs);
    }

    friend bool operator==(const safe_iterator_base& lhs, const BaseIterator& rhs) {
        if (!lhs.valid())
            return false;
        return lhs.base() == rhs;
    }

    friend bool operator==(const BaseIterator& lhs, const safe_iterator_base& rhs) {
        return rhs == lhs;
    }

    friend bool operator!=(const safe_iterator_base& lhs, const BaseIterator& rhs) {
        if (!lhs.valid())
            return true;
        return lhs.base() != rhs;
    }

    friend bool operator!=(const BaseIterator& lhs, const safe_iterator_base& rhs) {
        return rhs != lhs;
    }

private:
    template<typename C, typename I>
    friend class safe_iterator_map;

    // Called by the map when it's being destroyed.
    // Does not unregister this instance from the map
    // as that would be redundant.
    void make_invalid() {
        m_map = nullptr;
        this->base_reference() = BaseIterator();
    }

private:
    /// Valid iterators have non-null map pointers.
    map_type* m_map = nullptr;
};


template<typename Container, typename Iterator>
class safe_iterator_map {
    struct key_t {
        raw_address<Container::block_size> node;
        u32 index;

        bool operator<(const key_t& other) const {
            if (node != other.node)
                return node < other.node;
            return index < other.index;
        }

        bool operator==(const key_t& other) const {
            return node == other.node && index == other.index;
        }
    };

    static constexpr u32 block_size = Container::block_size;

    // TODO: This could be an intrusive map that uses the storage
    // within the iterators to link them together.
    // This would also make unregistering easier.
    using map_t = std::multimap<key_t, Iterator*>;

public:
    safe_iterator_map() = default;

    safe_iterator_map(safe_iterator_map&&) {
        // FIXME Change pointers.
    }

    safe_iterator_map& operator=(safe_iterator_map&&) {
        // FIXME Change pointers.
        return *this;
    }

    ~safe_iterator_map() {
        for (auto& p : m_map) {
            Iterator* i = p.second;
            i->m_map = nullptr;
        }
    }

    /// Finds all iterators to `block` values with indices in [first, last).
    template<typename OutputContainer>
    void find_iterators(raw_address<block_size> block, u32 first, u32 last, OutputContainer& out) {
        auto begin = m_map.lower_bound(key_t{block, first});
        auto end = m_map.lower_bound(key_t{block, last});
        for (; begin != end; ++begin)
            out.push_back(begin->second);
    }

private:
    template<typename C, typename I, typename D>
    friend class safe_iterator_base;

    void register_iterator(raw_address<block_size> block, u32 index, Iterator* iter) {
        m_map.emplace(key_t{block, index}, iter);
    }

    void replace_iterator(raw_address<block_size> block, u32 index, Iterator* olditer, Iterator* newiter) {
        find_iter(block, index, olditer)->second = newiter;
    }

    void unregister_iterator(raw_address<block_size> block, u32 index, Iterator* iter) {
        m_map.erase(find_iter(block, index, iter));
    }

private:
    auto find_iter(raw_address<block_size> addr, u32 index, Iterator* iter) {
        auto range = m_map.equal_range(key_t{addr, index});
        EXTPP_ASSERT(range.first != range.second,
                     "No registered iterators for that address.");

        // An intrusive impl could get rid of the linear scan here.
        for (auto i = range.first; i != range.second; ++i) {
            if (i->second == iter) {
                return i;
            }
        }

        EXTPP_UNREACHABLE("Iterator has not been registered.");
    }

private:
    map_t m_map;
};

} // namespace extpp::btree_detail

#endif // EXTPP_BTREE_SAFE_ITERATOR_HPP
