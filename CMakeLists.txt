cmake_minimum_required(VERSION 3.25)
cmake_host_system_information(RESULT ncores QUERY NUMBER_OF_PHYSICAL_CORES)
enable_testing()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake_modules)

set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CMAKE_JOB_POOL_COMPILE "compile")
set(CMAKE_JOB_POOL_LINK "link")
set(CMAKE_JOB_POOLS "compile=${ncores};link=2")

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
add_subdirectory(cpp)

if(SKBUILD)
    add_subdirectory(src/arcae)
else()
    message(WARNING "scikit-build not available, Cython extensions not built")
endif()
