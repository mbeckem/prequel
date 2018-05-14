find_package(PkgConfig)

if (NOT PKG_CONFIG_FOUND)
    message(STATUS "Not building the filesystem example because pkg-config was not found.")
    return()
endif()

pkg_check_modules(FUSE fuse)

if (NOT FUSE_FOUND)
    message(STATUS "Not building the filesystem example because libfuse was not found.")
    return()
endif()

add_executable(example-fs main.cpp)
target_compile_options(example-fs PRIVATE ${FUSE_CFLAGS})
target_link_libraries(example-fs PRIVATE extpp ${FUSE_LIBRARIES})
