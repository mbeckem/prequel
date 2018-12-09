set(HEADER_ROOT ${PROJECT_SOURCE_DIR}/include/prequel)

set(PUBLIC_HEADERS
    ${HEADER_ROOT}/detail/bitset.hpp
    ${HEADER_ROOT}/detail/fix.hpp
    ${HEADER_ROOT}/detail/free_list.hpp
    ${HEADER_ROOT}/detail/inlined_any.hpp
    ${HEADER_ROOT}/detail/iter_tools.hpp
    ${HEADER_ROOT}/detail/operators.hpp

    ${HEADER_ROOT}/address.hpp
    ${HEADER_ROOT}/allocator.hpp
    ${HEADER_ROOT}/anchor_handle.hpp
    ${HEADER_ROOT}/array.hpp
    ${HEADER_ROOT}/assert.hpp
    ${HEADER_ROOT}/binary_format.hpp
    ${HEADER_ROOT}/block_index.hpp
    ${HEADER_ROOT}/btree.hpp
    ${HEADER_ROOT}/default_allocator.hpp
    ${HEADER_ROOT}/deferred.hpp
    ${HEADER_ROOT}/defs.hpp
    ${HEADER_ROOT}/engine.hpp
    ${HEADER_ROOT}/exception.hpp
    ${HEADER_ROOT}/extent.hpp
    ${HEADER_ROOT}/file_engine.hpp
    ${HEADER_ROOT}/formatting.hpp
    ${HEADER_ROOT}/handle.hpp
    ${HEADER_ROOT}/hash.hpp
    ${HEADER_ROOT}/hash_table.hpp
    ${HEADER_ROOT}/heap.hpp
    ${HEADER_ROOT}/identity_key.hpp
    ${HEADER_ROOT}/id_generator.hpp
    ${HEADER_ROOT}/iteration.hpp
    ${HEADER_ROOT}/list.hpp
    ${HEADER_ROOT}/map.hpp
    ${HEADER_ROOT}/math.hpp
    ${HEADER_ROOT}/memory_engine.hpp
    ${HEADER_ROOT}/mmap_engine.hpp
    ${HEADER_ROOT}/node_allocator.hpp
    ${HEADER_ROOT}/raw_array.hpp
    ${HEADER_ROOT}/raw_btree.hpp
    ${HEADER_ROOT}/raw_hash_table.hpp
    ${HEADER_ROOT}/raw_list.hpp
    ${HEADER_ROOT}/raw_stack.hpp
    ${HEADER_ROOT}/serialization.hpp
    ${HEADER_ROOT}/simple_file_format.hpp
    ${HEADER_ROOT}/stack.hpp
    ${HEADER_ROOT}/type_traits.hpp
    ${HEADER_ROOT}/vfs.hpp
)

set(PRIVATE_HEADERS
    btree/base.hpp
    btree/cursor.hpp
    btree/cursor.ipp
    btree/internal_node.hpp
    btree/internal_node.ipp
    btree/leaf_node.hpp
    btree/leaf_node.ipp
    btree/loader.hpp
    btree/loader.ipp
    btree/tree.hpp
    btree/tree.ipp
)

set(SOURCES
    address.cpp
    assert.cpp
    block_index.cpp
    default_allocator.cpp
    engine.cpp
    exception.cpp
    extent.cpp
    file_engine.cpp
    formatting.cpp
    hash.cpp
    heap.cpp
    id_generator.cpp
    memory_engine.cpp
    mmap_engine.cpp
    node_allocator.cpp
    raw_array.cpp
    raw_btree.cpp
    raw_hash_table.cpp
    raw_list.cpp
    raw_stack.cpp
    simple_file_format.cpp
    vfs.cpp
    detail/free_list.cpp
)

if (WIN32)
    list(APPEND SOURCES
        vfs_win32.cpp
    )
elseif(UNIX)
    list(APPEND SOURCES
        vfs_unix.cpp
    )
endif()

add_library(prequel ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})
target_link_libraries(prequel PUBLIC fmt::fmt)
target_include_directories(prequel PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_include_directories(prequel PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_include_directories(prequel SYSTEM PUBLIC ${Boost_INCLUDE_DIRS})

# To log file engine block load/stores
# target_compile_definitions(prequel PRIVATE PREQUEL_TRACE_IO=1)

if (UNIX)
    # TODO: glibc specific
    target_compile_definitions(prequel PRIVATE -D_FILE_OFFSET_BITS=64)
endif()
