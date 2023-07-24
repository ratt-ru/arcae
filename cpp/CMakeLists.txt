find_package(PkgConfig REQUIRED)
pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_library(arcae SHARED
    casa_visitors.cpp
    column_convert_visitor.cpp
    complex_type.cpp
    configuration.cpp
    service_locator.cpp
    safe_table_proxy.cpp
    table_factory.cpp)

target_link_directories(arcae PUBLIC ${PYARROW_LIBDIRS})
target_link_libraries(arcae PkgConfig::casacore arrow)
target_include_directories(arcae PUBLIC PkgConfig::casacore ${PYARROW_INCLUDE})
target_compile_options(arcae PUBLIC "-D_GLIBCXX_USE_CXX11_ABI=0")

set_target_properties(arcae PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arcae LIBRARY DESTINATION ${CASAARROW_LIB_INSTALL_PATH})
