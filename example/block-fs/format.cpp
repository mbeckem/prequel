#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include <prequel/file_engine.hpp>
#include <prequel/vfs.hpp>

#include "filesystem.hpp"

using namespace blockfs;

// Formatting a block device: Just create the master block at index 0.
void format_device(prequel::file& device) {
    prequel::file_engine engine(device, block_size, 1);

    const u64 size_in_bytes = device.file_size();
    const u64 size_in_blocks = size_in_bytes / block_size;
    if (size_in_blocks <= 2) {
        throw std::runtime_error("Device is too small.");
    }

    master_block master;
    master.magic = master_block::magic_value();
    master.partition_size = size_in_bytes;

    // Initialize the region allocator with the rest of the file.
    {
        prequel::default_allocator alloc(make_anchor_handle(master.alloc), engine);
        alloc.can_grow(false);
        alloc.add_region(prequel::block_index(1), size_in_blocks - 1);
    }

    // Write the master block.
    prequel::block_handle handle = engine.overwrite_zero(prequel::block_index(0));
    handle.set(0, master);
    engine.flush();
    device.sync();
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Expected a file system path as first argument." << std::endl;
        return 1;
    }

    try {
        std::unique_ptr<prequel::file> device =
            prequel::system_vfs().open(argv[1], prequel::vfs::read_write);
        format_device(*device);
        std::cout << "OK." << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
