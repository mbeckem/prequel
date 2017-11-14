cmake_minimum_required(VERSION 2.8)

project(extpp)

find_package(Boost REQUIRED)

set(CMAKE_CXX_STANDARD 14)
if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pedantic -Wno-noexcept-type -Wno-terminate")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror")
endif()

include_directories("deps" SYSTEM)

set(Boost_USE_STATIC_LIBS       ON)
set(Boost_USE_MULTITHREADED     ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.60 COMPONENTS REQUIRED)

find_package(fmt REQUIRED)

add_subdirectory(extpp)
add_subdirectory(test)
