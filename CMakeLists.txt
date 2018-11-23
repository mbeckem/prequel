cmake_minimum_required(VERSION 2.8)

project(prequel)

set(GCC_WARNINGS
    -Wall
    -Wextra
    -pedantic

    # Disabled warnings
    -Wno-unknown-warning-option
    -Wno-noexcept-type
    -Wno-terminate
    -Wno-exceptions

    -Wshadow
    -Wnon-virtual-dtor
    -Wcast-align
    -Wunused
    -Wpedantic
    -Wsign-conversion
    -Wmisleading-indentation
    -Wduplicated-cond
    -Wduplicated-branches
    -Wlogical-op
    -Wnull-dereference
    -Wdouble-promotion
    -Wformat=2
#    -Wlifetime # Recent clang only
)

if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    string(REPLACE ";" " " GCC_WARNINGS_STR "${GCC_WARNINGS}")

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 ${GCC_WARNINGS_STR}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
endif()

include_directories(SYSTEM "deps")

set(Boost_USE_STATIC_LIBS       ON)
set(Boost_USE_MULTITHREADED     ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.60 REQUIRED)

find_package(fmt REQUIRED)

add_subdirectory(doc)
add_subdirectory(example)
add_subdirectory(src/prequel)
add_subdirectory(test)
