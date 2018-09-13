set(HEADER_ROOT ${PROJECT_SOURCE_DIR}/include/extpp)

set(PUBLIC_HEADERS
    ${HEADER_ROOT}/detail/bitset.hpp
    ${HEADER_ROOT}/detail/deferred.hpp
    ${HEADER_ROOT}/detail/fix.hpp
    ${HEADER_ROOT}/detail/free_list.hpp
    ${HEADER_ROOT}/detail/inlined_any.hpp
    ${HEADER_ROOT}/detail/iter_tools.hpp
    ${HEADER_ROOT}/detail/operators.hpp

    ${HEADER_ROOT}/address.hpp
    ${HEADER_ROOT}/allocator.hpp
    ${HEADER_ROOT}/anchor_handle.hpp
    ${HEADER_ROOT}/assert.hpp
    ${HEADER_ROOT}/binary_format.hpp
    ${HEADER_ROOT}/block_index.hpp
    ${HEADER_ROOT}/btree.hpp
    ${HEADER_ROOT}/default_allocator.hpp
    ${HEADER_ROOT}/default_file_format.hpp
    ${HEADER_ROOT}/defs.hpp
    ${HEADER_ROOT}/engine.hpp
    ${HEADER_ROOT}/exception.hpp
    ${HEADER_ROOT}/extent.hpp
    ${HEADER_ROOT}/file_engine.hpp
    ${HEADER_ROOT}/formatting.hpp
    ${HEADER_ROOT}/handle.hpp
    ${HEADER_ROOT}/heap.hpp
    ${HEADER_ROOT}/id_generator.hpp
    ${HEADER_ROOT}/io.hpp
    ${HEADER_ROOT}/list.hpp
    ${HEADER_ROOT}/math.hpp
    ${HEADER_ROOT}/node_allocator.hpp
    ${HEADER_ROOT}/raw_btree.hpp
    ${HEADER_ROOT}/raw_list.hpp
    ${HEADER_ROOT}/raw_stream.hpp
    ${HEADER_ROOT}/serialization.hpp
    ${HEADER_ROOT}/stream.hpp
    ${HEADER_ROOT}/type_traits.hpp
)

set(PRIVATE_HEADERS
    raw_btree_internal_node.hpp
    raw_btree_leaf_node.hpp
)

set(SOURCES
     address.cpp
     assert.cpp
     block_index.cpp
     default_allocator.cpp
     exception.cpp
     extent.cpp
     file_engine.cpp
     formatting.cpp
     heap.cpp
     id_generator.cpp
     io.cpp
     node_allocator.cpp
     raw_btree.cpp
     raw_list.cpp
     raw_stream.cpp
     detail/free_list.cpp
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

add_library(extpp ${SOURCES} ${PUBLIC_HEADERS} ${PRIVATE_HEADERS})
target_link_libraries(extpp PUBLIC fmt::fmt)
target_include_directories(extpp PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_include_directories(extpp PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_include_directories(extpp SYSTEM PUBLIC ${Boost_INCLUDE_DIRS})

if (UNIX)
    # TODO: glibc specific
    target_compile_definitions(extpp PRIVATE -D_FILE_OFFSET_BITS=64)
endif()

#add_executable(${PROJECT_NAME} main.cpp)
#target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}-lib)

