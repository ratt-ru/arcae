find_package(GTest CONFIG REQUIRED)

add_library(test_utils test_utils.cc)
add_compile_options("-fsanitize=address")
add_link_options("-fsanitize=address")

add_executable(basic_test basic_test.cc)
target_link_libraries(basic_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(basic_test basic_test)

add_executable(range_test range_test.cc)
target_link_libraries(range_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(range_test range_test)

add_executable(column_convert_test column_convert_test.cc)
target_link_libraries(column_convert_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(column_convert_test column_convert_test)

add_executable(parallel_write_test parallel_write_test.cc)
target_link_libraries(parallel_write_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(parallel_write_test parallel_write_test)
