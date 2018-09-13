find_package(Doxygen)

if(NOT DOXYGEN_FOUND)
    message(WARNING "Doxygen is needed to build the documentation.")
    return()
endif()

message(STATUS "Doxygen was found.")

set(doxyfile_in ${CMAKE_CURRENT_SOURCE_DIR}/Doxyfile.in)
set(doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)

configure_file(${doxyfile_in} ${doxyfile} @ONLY)

add_custom_target(doxygen
    COMMAND ${DOXYGEN_EXECUTABLE} ${doxyfile}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    COMMENT "Build doxygen xml database"
    VERBATIM)

macro(add_sphinx_target TARGET_NAME ADD_DEPENDENCY)
    add_custom_target(${TARGET_NAME}
        COMMAND ${CMAKE_COMMAND} -E env DOXYGEN_XML=${CMAKE_CURRENT_BINARY_DIR}/doxygen/xml
            sphinx-build -b html ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/sphinx
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Build html documentation"
        VERBATIM
    )
    if (${ADD_DEPENDENCY})
        add_dependencies(${TARGET_NAME} doxygen)
    endif()
endmacro(add_sphinx_target)

add_sphinx_target(doc-html "FALSE")
add_sphinx_target(doc "TRUE")


