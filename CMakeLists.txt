cmake_minimum_required(VERSION 2.8)

project(extpp)

if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wextra -pedantic -Wno-noexcept-type -Wno-terminate -Wno-exceptions -Wno-unknown-warning-option")
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
add_subdirectory(src/extpp)
add_subdirectory(test)
