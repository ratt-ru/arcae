find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(PkgConfig REQUIRED)
find_program(CYTHON, "cython")

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

python_add_library(arrow_tables MODULE arrow_tables.cc WITH_SOABI)

add_custom_command(
    TARGET arrow_tables
    BYPRODUCTS arrow_tables.cc
    DEPENDS arrow_tables.pyx
    VERBATIM
    COMMAND "${CYTHON}" "${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.pyx"
        --output-file "${CMAKE_CURRENT_BINARY_DIR}/arrow_tables.cc"
        -vvv)

message(INFO "CMAKE_CURRENT_BINARY_DIR=${CMAKE_CURRENT_BINARY_DIR}")

target_link_libraries(arrow_tables PUBLIC arcae PkgConfig::casacore)
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC
    PkgConfig::casacore ${PYARROW_INCLUDE} ${NUMPY_INCLUDE}
    ${CMAKE_SOURCE_DIR}/cpp)
target_compile_options(arrow_tables PUBLIC "-D_GLIBCXX_USE_CXX11_ABI=1")

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

foreach(PYARROW_LIB ${_PYARROW_LIBS})
    message("Linking arrow_tables against ${PYARROW_LIB}")
    target_link_libraries(arrow_tables PUBLIC ${PYARROW_LIB})
endforeach()

set_target_properties(arrow_tables PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arrow_tables LIBRARY DESTINATION ${ARCAE_LIB_INSTALL_PATH})
