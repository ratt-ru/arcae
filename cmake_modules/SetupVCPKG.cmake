include(FetchContent)

file(READ "${CMAKE_SOURCE_DIR}/.env" ENV_FILE)
string(REGEX MATCH "[ \t]*VCPKG_SHA1[ \t]*=[ \t]*([0-9a-f]*)" IGNORED ${ENV_FILE})

if(NOT DEFINED CMAKE_MATCH_1)
    message(FATAL_ERROR "Unable to find VCPKG_SHA1=version in ${CMAKE_SOURCE_DIR}/.env")
endif()

set(VCPKG_URL "https://github.com/microsoft/vcpkg/archive/${CMAKE_MATCH_1}.tar.gz")
message("VCPKG_VERSION=${CMAKE_MATCH_1}")
message("VCPKG_URL=${VCPKG_URL}")

string(REGEX MATCH "[ \t]*VCPKG_SHA256[ \t]*=[ \t]*([0-9a-f]*)" IGNORED ${ENV_FILE})

if(NOT DEFINED CMAKE_MATCH_1)
    message(FATAL_ERROR "Unable to find VCPKG_SHA256=version in ${CMAKE_SOURCE_DIR}/.env")
endif()

set(VCPKG_HASH ${CMAKE_MATCH_1})

FetchContent_Declare(vcpkg
    URL ${VCPKG_URL}
    URL_HASH SHA256=${VCPKG_HASH}
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/vcpkg/source)
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
