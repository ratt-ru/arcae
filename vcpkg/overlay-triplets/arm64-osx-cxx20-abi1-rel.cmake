set(VCPKG_TARGET_ARCHITECTURE arm64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_BUILD_TYPE release)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)

# Static archives linked into a single shared module. -fno-gnu-unique is GCC-only
# and is omitted here (macOS uses clang); -fvisibility=hidden is what prevents dyld
# from coalescing exported weak C++ symbols across casacore copies.
set(VCPKG_C_FLAGS "${VCPKG_C_FLAGS} -fPIC -fvisibility=hidden")
set(VCPKG_CXX_FLAGS "${VCPKG_CXX_FLAGS} -D_GLIBCXX_USE_CXX11_ABI=1 -fPIC -fvisibility=hidden -fvisibility-inlines-hidden")
set(VCPKG_Fortran_FLAGS "${VCPKG_Fortran_FLAGS} -fPIC")
set(VCPKG_LINKER_FLAGS "${VCPKG_LINKER_FLAGS} -Wl,-headerpad_max_install_names")

set(VCPKG_CMAKE_SYSTEM_NAME Darwin)
