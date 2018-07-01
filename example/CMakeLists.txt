include_directories(SYSTEM ${PROJECT_SOURCE_DIR}/deps/clipp-1.1.0/include)

add_subdirectory(block-fs)
add_subdirectory(btree-bench)
add_subdirectory(fs)
add_subdirectory(serialization)
# TODO:
# add_subdirectory(graph)
