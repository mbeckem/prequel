set(HEADERS
    detail/bitset.hpp
    detail/fix.hpp
    detail/free_list.hpp
    detail/hex.hpp
    detail/inlined_any.hpp
    detail/iter_tools.hpp
    detail/operators.hpp
    detail/rollback.hpp

    address.hpp
    allocator.hpp
    block_handle.hpp
    engine.hpp
    handle.hpp
    node_allocator.hpp
    raw_list.hpp
    list.hpp
    extent.hpp
    raw_stream.hpp
    stream.hpp

    address.hpp
    allocator.hpp
    assert.hpp
    binary_format.hpp
    block_handle.hpp
    block_index.hpp
    defs.hpp
    engine.hpp
    exception.hpp
    file_engine.hpp
    handle.hpp
    io.hpp
    math.hpp
    serialization.hpp
    type_traits.hpp
)

set(SOURCES
    address.cpp
    assert.cpp
    file_engine.cpp
    exception.cpp
    node_allocator.cpp
    io.cpp

    extent.cpp
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

add_library(extpp ${SOURCES} ${HEADERS})
target_link_libraries(extpp PUBLIC fmt::fmt)
target_include_directories(extpp PUBLIC "${PROJECT_SOURCE_DIR}")
target_include_directories(extpp PUBLIC SYSTEM ${Boost_INCLUDE_DIRS})

if (UNIX)
    # TODO: glibc specific
    target_compile_definitions(extpp PRIVATE -D_FILE_OFFSET_BITS=64)
endif()

#add_executable(${PROJECT_NAME} main.cpp)
#target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_NAME}-lib)

