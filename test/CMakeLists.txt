set(SOURCES
    address_test.cpp
    anchor_ptr_test.cpp
    bitset_test.cpp
    block_engine_test.cpp
    block_test.cpp
    btree_test.cpp
    default_allocator_test.cpp
    extent_test.cpp
    freelist_test.cpp
    handle_test.cpp
    heap_test.cpp
    id_generator_test.cpp
    inlined_any_test.cpp
    io_test.cpp
    list_test.cpp
    main.cpp
    math_test.cpp
    memory.cpp
    stack_test.cpp
    stream_test.cpp
    test_file.hpp
)

if (UNIX)
    list(APPEND SOURCES
        mmap_engine_test.cpp
    )
endif()

add_executable(unit-tests ${SOURCES})
target_link_libraries(unit-tests extpp)
