find_package(PkgConfig)

if (NOT PKG_CONFIG_FOUND)
    message(STATUS "Not building the block-filesystem because pkg-config was not found.")
    return()
endif()

pkg_check_modules(FUSE fuse)

if (NOT FUSE_FOUND)
    message(STATUS "Not building the block-filesystem because libfuse was not found.")
    return()
endif()

add_library(block-fs-static filesystem.hpp filesystem.cpp)
target_link_libraries(block-fs-static PUBLIC prequel)

add_executable(block-fs main.cpp)
target_compile_options(block-fs PRIVATE ${FUSE_CFLAGS})
target_link_libraries(block-fs PRIVATE block-fs-static ${FUSE_LIBRARIES})

add_executable(block-fs-format format.cpp)
target_link_libraries(block-fs-format PRIVATE block-fs-static)
