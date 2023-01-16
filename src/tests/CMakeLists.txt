link_directories(${PROJECT_BINARY_DIR}/lib)

find_library(casa_msfits NAMES casa_msfits)
find_library(casa_meas NAMES casa_meas)
find_library(casa_scimath_f NAMES casa_scimath_f)
find_library(casa_images NAMES casa_images)
find_library(casa_coordinates NAMES casa_coordinates)
find_library(casa_derivedmscal NAMES casa_derivedmscal)
find_library(casa_scimath NAMES casa_scimath)
find_library(casa_fits NAMES casa_fits)
find_library(casa_casa NAMES casa_casa)
find_library(casa_ms NAMES casa_ms)
find_library(casa_lattices NAMES casa_lattices)
find_library(casa_mirlib NAMES casa_mirlib)
find_library(casa_python3 NAMES casa_python3)
find_library(casa_measures NAMES casa_measures)
find_library(casa_tables NAMES casa_tables)

find_package(Arrow REQUIRED)

add_executable(test_runner test_runner.cpp)

target_link_libraries(test_runner
    PRIVATE
    casa_arrow
    Arrow::arrow_shared
    casa_msfits
    casa_scimath_f
    casa_images
    casa_coordinates
    casa_derivedmscal
    casa_scimath
    casa_fits
    casa_casa
    casa_ms
    casa_lattices
    casa_mirlib
    casa_python3
    casa_measures
    casa_tables)

# add_test(NAME test_runner COMMAND test_runner)
