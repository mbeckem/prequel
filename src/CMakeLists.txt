set(HEADER_ROOT ${PROJECT_SOURCE_DIR}/include/prequel)

set(PUBLIC_HEADERS
    ${HEADER_ROOT}/address.hpp
    ${HEADER_ROOT}/anchor_handle.hpp
    ${HEADER_ROOT}/assert.hpp
    ${HEADER_ROOT}/binary_format.hpp
    ${HEADER_ROOT}/block_index.hpp
    ${HEADER_ROOT}/deferred.hpp
    ${HEADER_ROOT}/defs.hpp
    ${HEADER_ROOT}/engine.hpp
    ${HEADER_ROOT}/exception.hpp
    ${HEADER_ROOT}/file_engine.hpp
    ${HEADER_ROOT}/formatting.hpp
    ${HEADER_ROOT}/handle.hpp
    ${HEADER_ROOT}/hash.hpp
    ${HEADER_ROOT}/math.hpp
    ${HEADER_ROOT}/memory_engine.hpp
    ${HEADER_ROOT}/mmap_engine.hpp
    ${HEADER_ROOT}/serialization.hpp
    ${HEADER_ROOT}/simple_file_format.hpp
    ${HEADER_ROOT}/transaction_engine.hpp
    ${HEADER_ROOT}/type_traits.hpp
    ${HEADER_ROOT}/vfs.hpp

    ${HEADER_ROOT}/detail/bitset.hpp
    ${HEADER_ROOT}/detail/fix.hpp
    ${HEADER_ROOT}/detail/free_list.hpp
    ${HEADER_ROOT}/detail/inlined_any.hpp
    ${HEADER_ROOT}/detail/iter_tools.hpp
    ${HEADER_ROOT}/detail/operators.hpp

    ${HEADER_ROOT}/container/allocator.hpp
    ${HEADER_ROOT}/container/array.hpp
    ${HEADER_ROOT}/container/btree.hpp
    ${HEADER_ROOT}/container/default_allocator.hpp
    ${HEADER_ROOT}/container/extent.hpp
    ${HEADER_ROOT}/container/hash_table.hpp
    ${HEADER_ROOT}/container/heap.hpp
    ${HEADER_ROOT}/container/id_generator.hpp
    ${HEADER_ROOT}/container/indexing.hpp
    ${HEADER_ROOT}/container/iteration.hpp
    ${HEADER_ROOT}/container/list.hpp
    ${HEADER_ROOT}/container/map.hpp
    ${HEADER_ROOT}/container/node_allocator.hpp
    ${HEADER_ROOT}/container/stack.hpp
)

set(PRIVATE_HEADERS
    container/btree/base.hpp
    container/btree/cursor.hpp
    container/btree/cursor.ipp
    container/btree/internal_node.hpp
    container/btree/internal_node.ipp
    container/btree/leaf_node.hpp
    container/btree/leaf_node.ipp
    container/btree/loader.hpp
    container/btree/loader.ipp
    container/btree/tree.hpp
    container/btree/tree.ipp

    engine/base.hpp
    engine/block.hpp
    engine/block.ipp
    engine/block_cache.hpp
    engine/block_dirty_set.hpp
    engine/block_map.hpp
    engine/block_pool.hpp
    engine/engine_base.hpp
    engine/engine_base.ipp
    engine/file_engine.hpp
    engine/file_engine.ipp
    engine/journal.hpp
    engine/journal.ipp
    engine/transaction_engine.hpp
    engine/transaction_engine.ipp
)

set(SOURCES
    address.cpp
    assert.cpp
    block_index.cpp
    engine.cpp
    exception.cpp
    file_engine.cpp
    formatting.cpp
    hash.cpp
    memory_engine.cpp
    mmap_engine.cpp
    simple_file_format.cpp
    transaction_engine.cpp
    vfs.cpp

    detail/free_list.cpp    

    container/array.cpp
    container/btree.cpp
    container/default_allocator.cpp
    container/extent.cpp
    container/hash_table.cpp
    container/heap.cpp
    container/id_generator.cpp
    container/list.cpp
    container/node_allocator.cpp
    container/stack.cpp
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

# Workaround so that our warnings don't trigger in libraries included by us (header files...).
# It transforms all includes of the library to "-isystem" mode, so the compiler skips warnings.
# Currently unused but kept around for later.
function(target_link_libraries_system target visibility)
    set(libs ${ARGN})
    foreach(lib ${libs})
        get_target_property(lib_include_dirs ${lib} INTERFACE_INCLUDE_DIRECTORIES)
        target_include_directories(${target} SYSTEM ${visibility} ${lib_include_dirs})
        target_link_libraries(${target} ${visibility} ${lib})
    endforeach()
endfunction()

add_library(prequel ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})
target_include_directories(prequel
    PUBLIC
        "${PROJECT_SOURCE_DIR}/include"
    PRIVATE
        "${PROJECT_SOURCE_DIR}/src"
    SYSTEM PRIVATE
        "${Boost_INCLUDE_DIRS}"
)
target_link_libraries(prequel PUBLIC fmt::fmt)

# To log file engine block load/stores
# target_compile_definitions(prequel PRIVATE PREQUEL_TRACE_IO=1)

if (UNIX)
    # TODO: glibc specific
    target_compile_definitions(prequel PRIVATE -D_FILE_OFFSET_BITS=64)
endif()
