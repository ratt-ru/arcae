find_package(PythonExtensions REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Cython REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_cython_target(arrow_tables arrow_tables.pyx CXX)
message("CYTHON table ${arrow_tables}")
add_library(arrow_tables MODULE ${arrow_tables})
target_link_libraries(arrow_tables casaarrowcore PkgConfig::casacore)
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC
    PkgConfig::casacore ${PYARROW_INCLUDE} ${NUMPY_INCLUDE}
    ${CMAKE_SOURCE_DIR}/cpp)
set_property(TARGET arrow_tables PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_options(arrow_tables PUBLIC "-D_GLIBCXX_USE_CXX11_ABI=0")
python_extension_module(arrow_tables)

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

foreach(PYARROW_LIB ${_PYARROW_LIBS})
    message("Linking arrow_tables against ${PYARROW_LIB}")
    target_link_libraries(arrow_tables ${PYARROW_LIB})
endforeach()

install(TARGETS arrow_tables LIBRARY DESTINATION casa_arrow/lib)

