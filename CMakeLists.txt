cmake_minimum_required(VERSION 2.8)

project(extpp)

#TODO
set(CMAKE_CXX_STANDARD 17)
if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    # TODO remove -std=...
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall -Wextra -pedantic -Wno-noexcept-type -Wno-terminate -Wno-exceptions -Wno-unknown-warning-option")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror")
endif()



include_directories("deps" SYSTEM)

set(Boost_USE_STATIC_LIBS       ON)
set(Boost_USE_MULTITHREADED     ON)
set(Boost_USE_STATIC_RUNTIME    OFF)
find_package(Boost 1.60 REQUIRED)

find_package(fmt REQUIRED)

add_subdirectory(doc)
#add_subdirectory(extpp_old)
#add_subdirectory(test_old)
#add_subdirectory(bench)
#add_subdirectory(example)
add_subdirectory(src/extpp)
add_subdirectory(test)
