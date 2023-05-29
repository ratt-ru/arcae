find_package(PkgConfig)
pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_library(casa_arrow STATIC
    casa_visitors.cpp
    column_convert_visitor.cpp
    complex_type.cpp
    configuration.cpp
    service_locator.cpp
    safe_table_proxy.cpp)


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


message(PYARROW_INCLUDE: ${PYARROW_INCLUDE})
message(PYARROW_LIBS: ${PYARROW_LIBS})
message(PYARROW_LIBDIRS: ${PYARROW_LIBDIRS})

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

target_link_libraries(casa_arrow PUBLIC PkgConfig::casacore)
foreach(PYARROW_LIB ${_PYARROW_LIBS})
    if(${PYARROW_LIB} STREQUAL "arrow_python")
        continue()
    endif()

    message("Linking casa_arrow against ${PYARROW_LIB}")
    target_link_libraries(casa_arrow PUBLIC ${PA_LIB})
endforeach()
target_link_directories(casa_arrow PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(casa_arrow PUBLIC PkgConfig::casacore ${PYARROW_INCLUDE})

install(TARGETS casa_arrow
        CONFIGURATIONS Debug
        DESTINATION debug/lib)

install(TARGETS casa_arrow
        CONFIGURATIONS Release
        DESTINATION lib)
