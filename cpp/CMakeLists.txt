find_package(PkgConfig REQUIRED)
find_package(absl CONFIG REQUIRED)

# casacore include directories (the CONFIG package below carries link info only,
# not INTERFACE_INCLUDE_DIRECTORIES, so pull the include path from pkg-config).
pkg_check_modules(CASACORE_PC REQUIRED casacore)

# Under static linkage the casacore targets reference ZLIB::ZLIB (a CMake imported
# target, via cfitsio) rather than an absolute libz path, so it must be found first.
find_package(ZLIB REQUIRED)

# casacore CMake CONFIG package: defines the ::casa_* imported targets with their
# full inter-target link interface. With the static triplet these resolve to .a
# archives. The targets export with an EMPTY namespace, i.e. "::casa_casa".
find_package(casacore CONFIG REQUIRED)

# The casacore static archives have circular inter-library dependencies. Listed
# dependents-first; the leaf numeric/system libraries (cfitsio, wcs, lapack,
# openblas, gsl, zlib, ...) are pulled in transitively via each target's
# INTERFACE_LINK_LIBRARIES. Attached to arcae's PUBLIC link interface so every
# consumer (the Python module and the C++ test executables) inherits it. The
# targets export with an EMPTY namespace, i.e. "::casa_casa".
set(CASACORE_LIBS
    ::casa_derivedmscal ::casa_msfits ::casa_ms ::casa_images ::casa_coordinates
    ::casa_lattices ::casa_fits ::casa_meas ::casa_measures ::casa_tables
    ::casa_scimath ::casa_scimath_f ::casa_mirlib ::casa_casa)

if(APPLE)
    # ld64 rescans archives automatically, so no start/end-group is needed (and
    # CMake's RESCAN LINK_GROUP feature is GNU-ld specific).
    set(CASACORE_LINK_GROUP ${CASACORE_LIBS})
else()
    # GNU ld is single-pass; wrap the set in a LINK_GROUP (RESCAN ==
    # --start-group/--end-group) so the linker rescans until the cycle resolves.
    string(REPLACE ";" "," _casa_group "${CASACORE_LIBS}")
    set(CASACORE_LINK_GROUP "$<LINK_GROUP:RESCAN,${_casa_group}>")
endif()

# casa_scimath_f is a Fortran archive whose objects reference the gfortran
# runtime (_gfortran_*), but the static casacore CONFIG targets don't propagate
# it through their link interface. The Python extension is a shared library, so
# Linux tolerates the unresolved symbols at link time; the fully-linked C++ test
# executables do not. Discover the Fortran implicit link libraries and add them
# to arcae's link interface. Guarded by check_language so machines without a
# Fortran compiler (which don't need one, since casacore is prebuilt by vcpkg)
# still configure.
include(CheckLanguage)
check_language(Fortran)
if(CMAKE_Fortran_COMPILER)
    enable_language(Fortran)
    set(FORTRAN_RUNTIME_LIBS ${CMAKE_Fortran_IMPLICIT_LINK_LIBRARIES})
else()
    message(WARNING
        "No Fortran compiler found; the gfortran runtime will not be added to "
        "the link interface. Linking the C++ test executables may fail with "
        "undefined _gfortran_* references.")
    set(FORTRAN_RUNTIME_LIBS "")
endif()

add_library(arcae STATIC
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
        -Wall -Werror -pedantic>
    -fvisibility=hidden -fvisibility-inlines-hidden)
string(REPLACE " " ";" _PYARROW_LIBDIRS "${PYARROW_LIBDIRS}")
target_link_directories(arcae PUBLIC ${_PYARROW_LIBDIRS})
target_link_libraries(arcae PUBLIC arrow absl::span "${CASACORE_LINK_GROUP}" ${FORTRAN_RUNTIME_LIBS})
target_include_directories(arcae PUBLIC
    ${CASACORE_PC_INCLUDE_DIRS}
    ${PYARROW_INCLUDE}
    ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(arcae PROPERTIES
    POSITION_INDEPENDENT_CODE 1)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
