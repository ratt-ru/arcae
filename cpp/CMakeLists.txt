find_package(PkgConfig REQUIRED)
find_package(absl CONFIG REQUIRED)
pkg_check_modules(casacore REQUIRED IMPORTED_TARGET casacore)

add_library(arcae SHARED
    arcae/configuration.cc
    arcae/descriptor.cc
    arcae/data_partition.cc
    arcae/isolated_table_proxy.cc
    arcae/new_table_proxy.cc
    arcae/read_impl.cc
    arcae/write_impl.cc
    arcae/result_shape.cc
    arcae/service_locator.cc
    arcae/table_factory.cc
    arcae/table_utils.cc
    arcae/type_traits.cc)

target_compile_options(arcae PRIVATE
    $<$<OR:$<CXX_COMPILER_ID:Clang>,$<CXX_COMPILER_ID:AppleClang>,$<CXX_COMPILER_ID:GNU>>:
        -Wall -Werror -pedantic>)
target_link_directories(arcae PUBLIC ${PYARROW_LIBDIRS})
target_link_libraries(arcae PkgConfig::casacore arrow absl::span)
target_include_directories(arcae PUBLIC PkgConfig::casacore absl::span ${PYARROW_INCLUDE} ${CMAKE_INCLUDE_CURRENT_DIR})

set_target_properties(arcae PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

install(TARGETS arcae LIBRARY DESTINATION arcae/lib)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
