#include "filesystem.hpp"

#include <iostream>

namespace blockfs {

static std::optional<fixed_string> to_filename(std::string_view path) {
    if (path.size() <= 1 || path.front() != '/')
        return {};
    path = path.substr(1);
    if (path.size() > fixed_string::max_size)
        return {};
    return fixed_string(path);
}

static extpp::block_handle read_master_block(extpp::file_engine& engine) {
    if (engine.size() < 1) {
        throw filesystem_exception("File system is too small, cannot access master block");
    }

    return engine.read(extpp::block_index(0));
}

static master_block read_master_block_content(const extpp::block_handle& handle) {
    if (handle.block_size() < extpp::serialized_size<master_block>()) {
        throw filesystem_exception("Invalid block size (too small for master block)");
    }

    master_block content = handle.get<master_block>(0);
    if (content.magic != master_block::magic_value()) {
        throw filesystem_exception("Invalid magic value, file system was not formatted correctly");
    }
    return content;
}

filesystem::filesystem(extpp::file_engine& eng)
    : m_engine(eng)
    // Load and validate master block from disk.
    , m_master_handle(read_master_block(m_engine))
    , m_master(read_master_block_content(m_master_handle))
    // Access datastructures from disk.
    , m_alloc(make_anchor_handle(m_master.alloc, m_master_changed), m_engine)
    , m_root(make_anchor_handle(m_master.root, m_master_changed), m_alloc)
{
    std::cout << "Allocator state at startup:\n";
    m_alloc.dump(std::cout);
}

filesystem::~filesystem() {
    try {
        flush();
    } catch (...) {}

    std::cout << "Allocator state at exit:\n";
    m_alloc.dump(std::cout);
}

std::vector<file_metadata> filesystem::list_files() {
    std::vector<file_metadata> files;
    files.reserve(m_root.size());

    for (directory::cursor pos = m_root.create_cursor(m_root.seek_min); pos; pos.move_next()) {
        files.push_back(pos.get().metadata);
    }

    return files;
}

bool filesystem::exists(const char* path) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        return false;
    }

    return true;
}

file_metadata filesystem::get_metadata(const char* path) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    return pos.get().metadata;
}

void filesystem::update_modification_time(const char* path, u64 mtime) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    file_entry entry = pos.get();
    entry.metadata.mtime = mtime;
    entry.metadata.ctime = time(0);
    pos.set(entry);
}

bool filesystem::create(const char* path, u32 permissions) {
    std::optional<fixed_string> name = to_filename(path);
    if (!name) {
        throw invalid_file_name(path);
    }

    // Create a new file entry.
    file_entry new_entry;
    new_entry.metadata.name = name.value();
    new_entry.metadata.permissions = permissions;
    new_entry.metadata.ctime = new_entry.metadata.mtime = time(0);

    // Try to insert it into the directory.
    auto result = m_root.insert(new_entry);
    writeback_master();
    return result.inserted;
}

void filesystem::rename(const char* from, const char* to) {
    std::optional<fixed_string> from_name, to_name;
    from_name = to_filename(from);
    to_name = to_filename(to);

    if (!from_name) {
        throw file_not_found(from);
    }
    if (!to_name) {
        throw invalid_file_name(to);
    }

    if (from_name.value() == to_name.value()) {
        return; // Same name.
    }

    // Find the entry that belongs to the filename.
    directory::cursor pos = m_root.find(from_name.value());
    if (!pos) {
        throw file_not_found(from);
    }

    // Load the old entry into memory and change the name.
    file_entry new_entry = pos.get();
    new_entry.metadata.name = to_name.value();
    new_entry.metadata.ctime = time(0);

    // Insert the new entry (possibly overwrite an existing file with that name)
    // and then delete the old entry.
    auto new_pos = m_root.insert(new_entry);
    if (!new_pos.inserted) {
        // Not inserted because the file existed; new_pos.first points to that entry.
        // Free the content of the old file and then overwrite the entry.
        file_entry overwrite_entry = new_pos.position.get();
        destroy_file(overwrite_entry);
        new_pos.position.set(new_entry);
    }
    writeback_master();
}

void filesystem::remove(const char* path) {
    // Find the file entry.
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    // Free the content and erase the entry from the directory tree.
    file_entry entry = pos.get();
    destroy_file(entry);
    pos.erase();
    writeback_master();
}

void filesystem::resize(const char* path, u64 new_size) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    file_entry entry = pos.get();
    if (new_size == entry.metadata.size) {
        return;
    }

    // Access and resize file content.
    extpp::extent content(make_anchor_handle(entry.content), m_alloc);
    adapt_capacity(content, new_size);

    // Write the changed file entry back into the directory tree.
    pos.set(entry);
    writeback_master();
}

size_t filesystem::read(const char* path, u64 offset, byte* buffer, size_t size) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    file_entry entry = pos.get();
    if (offset >= entry.metadata.size)
        return 0; // End of file

    // Access content of the file and read n bytes.
    const size_t n = std::min(entry.metadata.size - offset, u64(size));
    extpp::anchor_flag file_changed;
    extpp::extent content(make_anchor_handle(entry.content, file_changed), m_alloc);
    extpp::read(m_engine, m_engine.to_address(content.data()) + offset, buffer, n);

    if (file_changed) {
        throw std::logic_error("File changed as a result of a read operation.");
    }

    return n;
}

size_t filesystem::write(const char* path, u64 offset, const byte* buffer, size_t size) {
    directory::cursor pos = find_file(path);
    if (!pos) {
        throw file_not_found(path);
    }

    // Load the file entry from disk. It must always be written back to disk
    // because we change the metadata (e.g. time, size).
    file_entry entry = pos.get();
    if (offset > entry.metadata.size) {
        throw invalid_file_offset(path);
    }

    // Access file content and write to disk.
    extpp::extent content(make_anchor_handle(entry.content), m_alloc);
    if (offset + size > entry.metadata.size) {
        // File needs to grow.
        adapt_capacity(content, offset + size);
        entry.metadata.size = offset + size;
    }
    extpp::write(m_engine, m_engine.to_address(content.data()) + offset, buffer, size);

    // Update the file entry.
    pos.set(entry);
    writeback_master();
    return size;
}

void filesystem::flush() {
    writeback_master();
    m_engine.flush();
}

directory::cursor filesystem::find_file(const char* path) {
    std::optional<fixed_string> name = to_filename(path);
    if (!name) {
        throw file_not_found(path);
    }

    // Find the file entry.
    return m_root.find(name.value());
}

// Only grow or shrink the file when needed.
void filesystem::adapt_capacity(extpp::extent& content, u64 required_bytes) {
    const u64 old_blocks = content.size();
    const u64 required_blocks = extpp::ceil_div(required_bytes, u64(block_size));

    if (required_blocks > old_blocks) {
        // Grow the extent in powers of two.
        const u64 new_blocks = extpp::round_towards_pow2(required_blocks);
        content.resize(new_blocks);

        // Zero newly allocated storage.
        zero(m_engine,
             m_engine.to_address(content.data() + old_blocks),
             (new_blocks - old_blocks) * block_size);

        return;
    }

    if (required_blocks <= old_blocks / 4) {
        // Less than 25% full, shrink.
        const u64 new_blocks = extpp::round_towards_pow2(required_blocks);
        content.resize(new_blocks);
        return;
    }
}

void filesystem::destroy_file(file_entry& entry) {
    // Free the file content.
    extpp::extent content(make_anchor_handle(entry.content), m_alloc);
    content.reset();
    entry.metadata.size = 0;
}

void filesystem::writeback_master() {
    if (m_master_changed) {
        m_master_handle.set(0, m_master);
        m_master_changed.reset();
    }
}

} // namespace blockfs
