cmake_minimum_required(VERSION 3.0)

set(MASTER_PROJECT OFF)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(MASTER_PROJECT ON)
    message(STATUS "Prequel is master project, CMake version: ${CMAKE_VERSION}")
endif()

project(prequel)

# These options should be enabled during development!
option(PREQUEL_WARNINGS "Build with aggressive warnings." OFF)
option(PREQUEL_WERROR "Enable -Werror (halt compilation on warnings)." OFF)

option(PREQUEL_TESTS "Build unit test target." ${MASTER_PROJECT})
option(PREQUEL_EXAMPLES "Build example targets." ${MASTER_PROJECT})

if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    set(GCC_WARNINGS
        -pedantic-errors
        -Wall
        -Wextra
        -pedantic

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
    string(REPLACE ";" " " GCC_WARNINGS_STR "${GCC_WARNINGS}")

    # TODO: Not ideal to use CXX_FLAGS here, they should be applied on a per-target base.
    # However we have many targets right now and have to apply the options to each one.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    if (PREQUEL_WARNINGS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${GCC_WARNINGS_STR}")
    endif()
    if (PREQUEL_WERROR)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
    endif()

    # Disabled warnings
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unknown-warning-option -Wno-noexcept-type -Wno-terminate -Wno-exceptions")

    # Release options
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
endif()

include_directories(SYSTEM "deps")

set(Boost_USE_STATIC_LIBS       ON)
set(Boost_USE_MULTITHREADED     ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.60 REQUIRED)

find_package(fmt REQUIRED)

add_subdirectory(doc)
add_subdirectory(src)

if (PREQUEL_EXAMPLES)
    add_subdirectory(example)
endif()

if (PREQUEL_TESTS)
    add_subdirectory(test)
endif()
