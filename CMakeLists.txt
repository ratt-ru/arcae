cmake_minimum_required(VERSION 3.26)
cmake_host_system_information(RESULT ncores QUERY NUMBER_OF_PHYSICAL_CORES)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake_modules)

set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CMAKE_JOB_POOL_COMPILE "compile")
set(CMAKE_JOB_POOL_LINK "link")
set(CMAKE_JOB_POOLS "compile=${ncores};link=2")

set(ARCAE_LIB_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/arcae/lib)

include(FindPyArrow)
include(SetupVCPKG)
project(main CXX C)
add_subdirectory(cpp)

if(SKBUILD)
    add_subdirectory(arcae)
else()
    message(WARNING "scikit-build not available, Cython extensions not built")
endif()
