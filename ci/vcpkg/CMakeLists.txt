cmake_minimum_required(VERSION 3.26)
set(CMAKE_CXX_STANDARD 17)
project(casa-arrow CXX C)

add_compile_options("-D_GLIBCXX_USE_CXX11_ABI=0")
# add_link_options("-D_GLIBCXX_USE_CXX11_ABI=0")

find_package(PythonExtensions REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Cython REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

if(NOT DEFINED NUMPY_INCLUDE)
    message(FATAL_ERROR "NUMPY_INCLUDE not defined")
endif()

if(NOT DEFINED PYARROW_INCLUDE)
    message(FATAL_ERROR, "PYARROW_INCLUDE not defined")
endif()

if(NOT DEFINED PYARROW_LIBS)
    message(FATAL_ERROR, "PYARROW_LIBS not defined")
endif()

if(NOT DEFINED PYARROW_LIBDIRS)
    message(FATAL_ERROR, "PYARROW_LIBDIRS not defined")
endif()

message(NUMPY_INCLUDE: ${NUMPY_INCLUDE})
message(PYARROW_INCLUDE: ${PYARROW_INCLUDE})
message(PYARROW_LIBS: ${PYARROW_LIBS})
message(PYARROW_LIBDIRS: ${PYARROW_LIBDIRS})

add_library(core OBJECT
    ../../src/casa_visitors.cpp
    ../../src/column_convert_visitor.cpp
    ../../src/complex_type.cpp
    ../../src/configuration.cpp
    ../../src/service_locator.cpp
    ../../src/safe_table_proxy.cpp)

target_link_libraries(core PkgConfig::casacore)
target_include_directories(core PUBLIC PkgConfig::casacore)
target_include_directories(core PUBLIC ${PYARROW_INCLUDE})
set_property(TARGET core PROPERTY POSITION_INDEPENDENT_CODE 1)

add_cython_target(arrow_tables ../../casa_arrow/arrow_tables.pyx CXX)
message("CYTHON table ${arrow_tables}")
add_library(arrow_tables MODULE ${arrow_tables} $<TARGET_OBJECTS:core>)
target_include_directories(arrow_tables PUBLIC ../../src)
target_link_libraries(arrow_tables ${PYARROW_LIB})
set_property(TARGET arrow_tables PROPERTY POSITION_INDEPENDENT_CODE 1)
python_extension_module(arrow_tables)

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

target_link_libraries(arrow_tables PkgConfig::casacore)
foreach(PYARROW_LIB ${_PYARROW_LIBS})
    message("Linking arrow_tables against ${PYARROW_LIB}")
    target_link_libraries(arrow_tables ${PYARROW_LIB})
endforeach()

set(CMAKE_INSTALL_RPATH ${PYARROW_LIBDIRS})
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC PkgConfig::casacore)
target_include_directories(arrow_tables PUBLIC ${PYARROW_INCLUDE} ${NUMPY_INCLUDE})
target_include_directories(arrow_tables PUBLIC src)

install(TARGETS arrow_tables LIBRARY DESTINATION casa_arrow)
