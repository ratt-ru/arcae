find_package(GTest CONFIG REQUIRED)

add_library(test_utils test_utils.cc)

add_executable(basic_test basic_test.cc)
target_link_libraries(basic_test PRIVATE GTest::gtest_main arcae)
add_test(arcae_gtests basic_test)
