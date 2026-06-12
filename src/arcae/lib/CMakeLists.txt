find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(absl CONFIG REQUIRED)
find_program(CYTHON_FOUND "cython" REQUIRED)

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

# arcae (STATIC) carries casacore (wrapped in a LINK_GROUP), arrow and the casacore
# include dirs on its PUBLIC interface, so the monolith is assembled simply by
# linking arcae here.
target_link_libraries(arrow_tables PRIVATE arcae)
string(REPLACE " " ";" _PYARROW_LIBDIRS "${PYARROW_LIBDIRS}")
target_link_directories(arrow_tables PUBLIC ${_PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC
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

# Hidden visibility for the module's own translation units. Combined with the
# static-built, hidden-visibility casacore/dep archives this leaves the final .so
# with no casacore symbols in its dynamic symbol table.
target_compile_options(arrow_tables PRIVATE -fvisibility=hidden -fvisibility-inlines-hidden)

# Isolation: export only the module init symbol so no casacore/dep symbol lands in
# the dynamic symbol table where a second casacore copy could bind to it.
if(APPLE)
    # ld64: restrict exports to _PyInit_arrow_tables (mangled with leading underscore).
    target_link_options(arrow_tables PRIVATE
        "-Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.exp")
else()
    # GNU ld belt-and-suspenders:
    #   --exclude-libs,ALL    localise every symbol pulled from static archives
    #   --version-script      export only PyInit_arrow_tables from the module
    target_link_options(arrow_tables PRIVATE
        -Wl,--exclude-libs,ALL
        -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/arrow_tables.map)
endif()

set_target_properties(arrow_tables PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arrow_tables LIBRARY DESTINATION arcae/lib)
