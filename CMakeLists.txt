cmake_minimum_required(VERSION 3.26)
set(CMAKE_CXX_STANDARD 17)
project(casa-arrow)

find_package(PythonExtensions REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Cython REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

if(NOT DEFINED PYARROW_INCLUDE)
    message(FATAL_ERROR, "PYARROW_INCLUDE not defined")
endif()

if(NOT DEFINED PYARROW_LIBS)
    message(FATAL_ERROR, "PYARROW_LIBS not defined")
endif()

if(NOT DEFINED PYARROW_LIBDIRS)
    message(FATAL_ERROR, "PYARROW_LIBDIRS not defined")
endif()

message(PYARROW_INCLUDE: ${PYARROW_INCLUDE})
message(PYARROW_LIBS: ${PYARROW_LIBS})
message(PYARROW_LIBDIRS: ${PYARROW_LIBDIRS})

add_library(core STATIC
    src/casa_visitors.cpp
    src/column_convert_visitor.cpp
    src/complex_type.cpp
    src/configuration.cpp
    src/service_locator.cpp
    src/safe_table_proxy.cpp)

target_include_directories(core PUBLIC PkgConfig::casacore)
target_include_directories(core PUBLIC ${PYARROW_INCLUDE})
set_property(TARGET core PROPERTY POSITION_INDEPENDENT_CODE 1)

add_cython_target(_tables casa_arrow/arrow_tables.pyx CXX)
message("CYTHON table ${_tables}")
add_library(_tables SHARED ${_tables})
target_link_libraries(_tables core)
set_property(TARGET _tables PROPERTY POSITION_INDEPENDENT_CODE 1)
python_extension_module(_tables)

# find_package(Python3 COMPONENTS Interpreter REQUIRED)

# execute_process(
#     COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow as pa; pa.create_library_symlinks()"
#     OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PA_INCLUDE)

# execute_process(
#     COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow as pa; print(pa.get_include())"
#     OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PA_INCLUDE)

# execute_process(
#     COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow as pa; print(' '.join(pa.get_library_dirs()))"
#     OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PA_LIBDIRS)

# execute_process(
#     COMMAND "${Python3_EXECUTABLE}" -c "import pyarrow as pa; print(';'.join(pa.get_libraries()))"
#     OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PA_LIBS)

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

target_link_libraries(_tables PkgConfig::casacore)
foreach(PYARROW_LIB ${_PYARROW_LIBS})
    if(${PYARROW_LIB} STREQUAL "arrow_python")
        continue()
    endif()

    message("Linking core against ${PYARROW_LIB}")
    target_link_libraries(_tables ${PYARROW_LIB})
endforeach()

target_link_directories(_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(_tables PUBLIC PkgConfig::casacore)
target_include_directories(_tables PUBLIC ${PYARROW_INCLUDE} ${NUMPY_INCLUDE})
target_include_directories(_tables PUBLIC src)

install(TARGETS _tables DESTINATION ${PYTHON_RELATIVE_SITE_PACKAGES_DIR})
