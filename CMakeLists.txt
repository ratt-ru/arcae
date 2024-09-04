cmake_minimum_required(VERSION 3.25)

# Compiler id for Apple Clang is now AppleClang
cmake_policy(SET CMP0025 NEW)

# Enable MACOS_RPATH
# https://cmake.org/cmake/help/latest/policy/CMP0042.html
cmake_policy(SET CMP0042 NEW)

cmake_host_system_information(RESULT ncores QUERY NUMBER_OF_PHYSICAL_CORES)
enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake_modules)

if(UNIX)
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
    if(APPLE)
        set(CMAKE_INSTALL_RPATH "@loader_path/")
        set(CMAKE_INSTALL_NAME_DIR "@rpath")
    else()
        set(CMAKE_INSTALL_RPATH "$ORIGIN")
    endif()
elseif(WIN32)
    message(FATAL_ERROR "Windows is not supported")
endif()

message(STATUS "CMAKE_INSTALL_RPATH=$CMAKE_INSTALL_RPATH")

set(CMAKE_JOB_POOL_COMPILE "compile")
set(CMAKE_JOB_POOL_LINK "link")
set(CMAKE_JOB_POOLS "compile=${ncores};link=2")

# Export compile_commands.json by default
if(NOT DEFINED CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# VCPKG must be setup before the project command
include(SetupVCPKG)

project(arcae CXX)

include(FindPyArrow)
include(CTest)

include_directories(cpp)
add_compile_options("-D_GLIBCXX_USE_CXX11_ABI=1")
add_subdirectory(cpp)

if(SKBUILD)
    add_subdirectory(src/arcae/lib)
else()
    message(WARNING "scikit-build not available, Cython extensions not built")
endif()
