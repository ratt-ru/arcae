find_package(PythonExtensions REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(Cython REQUIRED)

pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_cython_target(arrow_tables arrow_tables.pyx CXX)
# message("CYTHON table ${arrow_tables}")
add_library(arrow_tables MODULE ${arrow_tables})
target_link_libraries(arrow_tables $<TARGET_FILE_BASE_NAME:casaarrowcore>)
target_link_directories(arrow_tables PUBLIC $<TARGET_FILE_DIR:casaarrowcore>)
target_link_directories(arrow_tables PUBLIC ${PYARROW_LIBDIRS})
set_property(TARGET arrow_tables PROPERTY POSITION_INDEPENDENT_CODE 1)
python_extension_module(arrow_tables)
target_compile_options(arrow_tables PUBLIC "-D_GLIBCXX_USE_CXX11_ABI=0")

string(REPLACE " " ";" _PYARROW_LIBS ${PYARROW_LIBS})

target_link_libraries(arrow_tables PkgConfig::casacore)
foreach(PYARROW_LIB ${_PYARROW_LIBS})
    message("Linking arrow_tables against ${PYARROW_LIB}")
    target_link_libraries(arrow_tables ${PYARROW_LIB})
endforeach()

# set(CMAKE_INSTALL_RPATH ${PYARROW_LIBDIRS})
target_include_directories(arrow_tables PUBLIC PkgConfig::casacore)
target_include_directories(arrow_tables PUBLIC ${PYARROW_INCLUDE} ${NUMPY_INCLUDE})
target_include_directories(arrow_tables PUBLIC ${CMAKE_SOURCE_DIR}/cpp)

install(TARGETS arrow_tables LIBRARY DESTINATION .)
