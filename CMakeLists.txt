cmake_minimum_required(VERSION 3.26)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED On)
set(CMAKE_CXX_EXTENSIONS Off)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake_modules)

set(CMAKE_INSTALL_RPATH "$ORIGIN")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

set(CASAARROW_LIB_INSTALL_PATH ${CMAKE_INSTALL_PREFIX}/casa_arrow/lib)

include(FindPyArrow)
include(SetupVCPKG)
project(main CXX C)
add_subdirectory(cpp)

if(SKBUILD)
    add_subdirectory(casa_arrow)
else()
    message(WARNING "scikit-build not available, Cython extensions not built")
endif()
