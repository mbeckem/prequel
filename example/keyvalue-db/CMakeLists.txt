set(HEADERS
    database.hpp
    fixed_string.hpp
)

set(SOURCES
    main.cpp
    database.cpp
)

add_executable(keyvalue-db ${SOURCES} ${HEADERS})
target_link_libraries(keyvalue-db PRIVATE prequel)
