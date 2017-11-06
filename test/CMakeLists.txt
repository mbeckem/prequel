set(SOURCES
    main.cpp
    address_test.cpp
    block_engine_test.cpp
    block_test.cpp
    btree_test.cpp
    default_allocator_test.cpp
    freelist_test.cpp
    inlined_any_test.cpp
    io_test.cpp
    list_test.cpp
    math_test.cpp
    stack_test.cpp

    test_file.hpp
)

add_executable(unit-tests ${SOURCES})
target_link_libraries(unit-tests extpp)
