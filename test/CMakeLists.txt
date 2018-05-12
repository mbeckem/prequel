set(SOURCES
    main.cpp

    address_test.cpp
    bitset_test.cpp
    extent_test.cpp
    file_engine_test.cpp
    free_list_test.cpp
    handle_test.cpp
    inlined_any_test.cpp
    io_test.cpp
    iter_tools_test.cpp
    list_test.cpp
    math_test.cpp
    node_allocator_test.cpp
    serialization_test.cpp
    stream_test.cpp
    test_file.hpp
)

if (UNIX)
    # TODO
    #list(APPEND SOURCES
    #    mmap_engine_test.cpp
    #)
endif()

add_executable(unit-tests ${SOURCES})
target_link_libraries(unit-tests extpp)
