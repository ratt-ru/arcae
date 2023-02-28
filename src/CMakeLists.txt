set(CMAKE_CXX_STANDARD 17)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY, ${PROJECT_BINARY_DIR}/lib)

add_library(casa_arrow SHARED
    safe_table_proxy.cpp
    casa_visitors.cpp
    column_convert_visitor.cpp
    complex_type.cpp)
