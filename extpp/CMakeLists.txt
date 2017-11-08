set(HEADERS
    detail/fix.hpp
    detail/free_list.hpp
    detail/inlined_any.hpp
    detail/iter_tools.hpp
    detail/operators.hpp
    detail/rollback.hpp
    detail/sequence_insert.hpp

    address.hpp
    allocator.hpp
    assert.hpp
    block.hpp
    btree.hpp
    btree_map.hpp
    btree_set.hpp
    default_allocator.hpp
    defs.hpp
    engine.hpp
    exception.hpp
    extent.hpp
    handle.hpp
    identity_key.hpp
    io.hpp
    list.hpp
    math.hpp
    raw.hpp
    stack.hpp
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
    target_compile_definitions(${PROJECT_NAME} PRIVATE -D_FILE_OFFSET_BIT=64)
endif()

#add_executable(${PROJECT_NAME} main.cpp)
#target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}-lib)

