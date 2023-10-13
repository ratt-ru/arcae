find_package(GTest CONFIG REQUIRED)

add_library(test_utils test_utils.cc)

add_executable(basic_test basic_test.cc)
target_link_libraries(basic_test PRIVATE GTest::gtest_main arcae test_utils)
target_compile_options(basic_test PUBLIC "-fsanitize=address")
target_link_options(basic_test PUBLIC "-fsanitize=address")
add_test(basic_test basic_test)

add_executable(range_test range_test.cc)
target_link_libraries(range_test PRIVATE GTest::gtest_main arcae test_utils)
target_compile_options(range_test PUBLIC "-fsanitize=address")
target_link_options(range_test PUBLIC "-fsanitize=address")
add_test(range_test range_test)