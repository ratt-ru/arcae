find_package(PkgConfig REQUIRED)
pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_library(arcae SHARED
    arcae/casa_visitors.cc
    arcae/column_convert_visitor.cc
    arcae/complex_type.cc
    arcae/configuration.cc
    arcae/descriptor.cc
    arcae/service_locator.cc
    arcae/safe_table_proxy.cc
    arcae/table_factory.cc
    arcae/utility.cc)

target_link_directories(arcae PUBLIC ${PYARROW_LIBDIRS})
target_link_libraries(arcae PkgConfig::casacore arrow)
target_include_directories(arcae PUBLIC PkgConfig::casacore ${PYARROW_INCLUDE} ${CMAKE_INCLUDE_CURRENT_DIR})
target_compile_options(arcae PUBLIC "-D_GLIBCXX_USE_CXX11_ABI=1")

set_target_properties(arcae PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arcae LIBRARY DESTINATION arcae/lib)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
