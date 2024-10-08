find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(PkgConfig REQUIRED)
find_program(CYTHON_FOUND "cython" REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_custom_command(
    OUTPUT arrow_tables.cc
    VERBATIM
    DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.pyx
        ${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.pxd
    COMMAND cython ${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.pyx
            --output-file ${CMAKE_CURRENT_BINARY_DIR}/arrow_tables.cc
            --cplus -vvv)

python_add_library(arrow_tables MODULE WITH_SOABI arrow_tables.cc)

target_link_libraries(arrow_tables PUBLIC arcae PkgConfig::casacore)
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC
                            PkgConfig::casacore
                            absl::span
                            ${CMAKE_CURRENT_SOURCE_DIR}
                            ${PYARROW_INCLUDE}
                            ${NUMPY_INCLUDE}
                            ${CMAKE_SOURCE_DIR}/cpp)

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

foreach(PYARROW_LIB ${_PYARROW_LIBS})
    message("Linking arrow_tables against ${PYARROW_LIB}")
    target_link_libraries(arrow_tables PUBLIC ${PYARROW_LIB})
endforeach()

set_target_properties(arrow_tables PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arrow_tables LIBRARY DESTINATION arcae/lib)
