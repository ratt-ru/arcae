include(FetchContent)

FetchContent_Declare(
    vcpkg
    URL https://github.com/microsoft/vcpkg/archive/1ba9a2591f15af5900f2ce2b3e2bf31771e3ac48.tar.gz)

FetchContent_MakeAvailable(vcpkg)
set(CMAKE_TOOLCHAIN_FILE "${vcpkg_SOURCE_DIR}/scripts/buildsystems/vcpkg.cmake" CACHE FILEPATH "")
set(VCPKG_MANIFEST_DIR "${CMAKE_SOURCE_DIR}/vcpkg")

set(VCPKG_OVERLAY_TRIPLETS "${VCPKG_MANIFEST_DIR}/custom-triplets")
set(VCPKG_OVERLAY_PORTS "${VCPKG_MANIFEST_DIR}/custom-ports/casacore")
if(UNIX)
    set(VCPKG_TARGET_TRIPLET "x64-linux-dynamic-cxx17-abi0")
else()
    message(FATAL_ERROR "Only unix currently supported")
endif()

message("CMAKE_TOOLCHAIN_FILE: ${CMAKE_TOOLCHAIN_FILE}")
message("VCPKG_OVERLAY_TRIPLETS: ${VCPKG_OVERLAY_TRIPLETS}")
message("VCPKG_OVERLAY_PORTS: ${VCPKG_OVERLAY_PORTS}")
message("VCPKG_MANIFEST_DIR ${VCPKG_MANIFEST_DIR}")
