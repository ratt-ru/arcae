find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(PkgConfig REQUIRED)
find_program(CYTHON_FOUND "cython" REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_custom_command(
    OUTPUT arrow_tables.cc
    VERBATIM
    COMMAND cython ${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.pyx
            --output-file ${CMAKE_CURRENT_BINARY_DIR}/arrow_tables.cc
            --cplus -vvv)

# Check and correct Python_SOABI if it has an .so suffix
if(${Python_SOABI} MATCHES "\\.so$")
    string(REGEX REPLACE "\\.so$" "" Python_SOABI_FIXED "${Python_SOABI}")
    message(STATUS "Removing extra .so from ${Python_SOABI}")
else()
    set(Python_SOABI_FIXED ${Python_SOABI})
endif()

message(STATUS "Python_SOABI_FIXED: ${Python_SOABI_FIXED}")

python_add_library(arrow_tables MODULE arrow_tables.cc)
set_target_properties(arrow_tables PROPERTIES OUTPUT_NAME "arrow_tables.${Python_SOABI_FIXED}")

target_link_libraries(arrow_tables PUBLIC arcae PkgConfig::casacore)
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC
                            PkgConfig::casacore
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
