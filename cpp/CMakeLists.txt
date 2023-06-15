find_package(PkgConfig REQUIRED)
pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_library(casaarrowcore SHARED
    casa_visitors.cpp
    column_convert_visitor.cpp
    complex_type.cpp
    configuration.cpp
    service_locator.cpp
    safe_table_proxy.cpp)

target_link_directories(casaarrowcore PUBLIC ${PYARROW_LIBDIRS})
target_link_libraries(casaarrowcore PkgConfig::casacore arrow)
target_include_directories(casaarrowcore PUBLIC PkgConfig::casacore ${PYARROW_INCLUDE})
set_property(TARGET casaarrowcore PROPERTY POSITION_INDEPENDENT_CODE 1)
install(TARGETS casaarrowcore LIBRARY DESTINATION .)
