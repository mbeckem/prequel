set(HEADERS
    detail/empty.hpp
    detail/bitset.hpp
    detail/fix.hpp
    detail/free_list.hpp
    detail/inlined_any.hpp
    detail/iter_tools.hpp
    detail/memory.hpp
    detail/operators.hpp
    detail/rollback.hpp
    detail/sequence_insert.hpp
    detail/wal.hpp

    heap/base.hpp
    heap/gc.hpp
    heap/object_access.hpp
    heap/object_table.hpp
    heap/segregated_free_list.hpp
    heap/storage.hpp
    heap/type_set.hpp

    btree/base.hpp
    btree/iterator.hpp
    btree/node.hpp
    btree/safe_iterator.hpp
    btree/state.hpp
    btree/verify.hpp

    address.hpp
    allocator.hpp
    anchor_ptr.hpp
    assert.hpp
    block.hpp
    block_index.hpp
    btree.hpp
    default_allocator.hpp
    default_file_format.hpp
    defs.hpp
    engine.hpp
    exception.hpp
    extent.hpp
    handle.hpp
    heap.hpp
    identity_key.hpp
    id_generator.hpp
    io.hpp
    list.hpp
    log.hpp
    map.hpp
    math.hpp
    raw.hpp
    set.hpp
    stack.hpp
    stream.hpp
    type_traits.hpp
)

set(SOURCES
    assert.cpp
    engine.cpp
    exception.cpp
    io.cpp
)

if (WIN32)
    list(APPEND SOURCES
        io_win32.cpp
    )
elseif(UNIX)
    list(APPEND SOURCES
        io_unix.cpp
    )
endif()

add_library(extpp ${SOURCES} ${HEADERS})
target_link_libraries(extpp PUBLIC fmt::fmt)
target_include_directories(extpp PUBLIC "${PROJECT_SOURCE_DIR}")
target_include_directories(extpp PUBLIC SYSTEM ${Boost_INCLUDE_DIRS})

if (UNIX)
    # TODO: glibc specific
    target_compile_definitions(${PROJECT_NAME} PRIVATE -D_FILE_OFFSET_BITS=64)
endif()

#add_executable(${PROJECT_NAME} main.cpp)
#target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}-lib)

