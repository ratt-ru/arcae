include(vcpkg_find_fortran)

vcpkg_from_github(
    OUT_SOURCE_PATH src
    REPO "casacore/casacore"
    REF "v3.6.1"
    SHA512 2189d67650ffc9c4b6ca9d32e7ca90e2556e518c79f4df36d1fc75a0e4ad551e3b214226a8d13879a7e061a2b7bc53eb4ef5266f656c33d057d4b25ec8958a3e
    PATCHES
        001-casacore-cmake.patch
)

vcpkg_find_acquire_program(FLEX)
vcpkg_find_acquire_program(BISON)
vcpkg_find_fortran(fortran_args)

vcpkg_check_features(
    OUT_FEATURE_OPTIONS FEATURE_OPTIONS
    FEATURES
        tablelocking        ENABLE_TABLELOCKING
        deprecated          BUILD_DEPRECATED
        dysco               BUILD_DYSCO
        mutex               USE_THREADS
        readline            USE_READLINE
        adios2              USE_ADIOS2
        fftw3               BUILD_FFTW3
        hdf5                USE_HDF5
        mpi                 USE_MPI
        openmp              USE_OPENMP
        python3             BUILD_PYTHON3
        stacktrace          USE_STACKTRACE
        casabuild           CASA_BUILD)

if(${BUILD_PYTHON3} STREQUAL "ON")
    message(FATAL_ERROR "Python3 Support not available: https://github.com/microsoft/vcpkg/discussions/29645")
endif()

if(${USE_ADIOS2} STREQUAL "ON")
    message(FATAL_ERROR "Adios2 Support not available")
endif()

# This works around vcpkg_fixup_cmake searching for a non-existent ${debug_share} directory
if(NOT DEFINED VCPKG_BUILD_TYPE OR VCPKG_BUILD_TYPE STREQUAL "debug")
    if(NOT EXISTS "${CURRENT_PACKAGES_DIR}/debug/share/${PORT}")
        file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/debug/share/${PORT}")
    endif()
endif()


vcpkg_cmake_configure(
    SOURCE_PATH "${src}"
    OPTIONS
        ${FEATURE_OPTIONS}
        -DBUILD_FFTW3=${BUILD_FFTW3}
        -DBUILD_PYTHON=OFF
        -DBUILD_TESTING=OFF
        -DUSE_PCH=OFF
        -DCMAKE_CXX_STANDARD=17
        ${fortran_args}
)

vcpkg_cmake_install()
vcpkg_fixup_pkgconfig()
vcpkg_cmake_config_fixup()

vcpkg_install_copyright(FILE_LIST "${src}/COPYING")
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
