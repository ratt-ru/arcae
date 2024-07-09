find_package(GTest CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

add_library(test_utils test_utils.cc)
add_compile_options("-fsanitize=address")
add_link_options("-fsanitize=address")

add_executable(basic_test basic_test.cc)
target_link_libraries(basic_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(basic_test basic_test)

add_executable(range_test range_test.cc)
target_link_libraries(range_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(range_test range_test)

add_executable(result_shape_test result_shape_test.cc)
target_link_libraries(result_shape_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(result_shape_test result_shape_test)

add_executable(column_iterator_test column_iterator_test.cc)
target_link_libraries(column_iterator_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(column_iterator_test column_iterator_test)

add_executable(column_rw_test column_rw_test.cc)
target_link_libraries(column_rw_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(column_rw_test column_rw_test)

add_executable(column_map_test column_map_test.cc)
target_link_libraries(column_map_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(column_map_test column_map_test)

add_executable(isolated_table_proxy_test isolated_table_proxy_test.cc)
target_link_libraries(isolated_table_proxy_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(isolated_table_proxy_test isolated_table_proxy_test)

add_executable(parallel_write_test parallel_write_test.cc)
target_link_libraries(parallel_write_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(parallel_write_test parallel_write_test)

add_executable(data_partition_test data_partition_test.cc)
target_link_libraries(data_partition_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(data_partition_test data_partition_test)

add_executable(selection_test selection_test.cc)
target_link_libraries(selection_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(selection_test selection_test)

add_executable(var_write_test var_write_test.cc)
target_link_libraries(var_write_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(var_write_test var_write_test)

add_executable(in_place_test in_place_test.cc)
target_link_libraries(in_place_test PRIVATE GTest::gtest_main arcae test_utils)
add_test(in_place_test in_place_test)

add_executable(dev_transpose_test dev_transpose_test.cc)
target_link_libraries(dev_transpose_test PRIVATE GTest::gtest_main arcae absl::time absl::str_format test_utils)
add_test(dev_transpose_test dev_transpose_test)

set_tests_properties(basic_test range_test
                     column_iterator_test
                     result_shape_test
                     column_rw_test
                     column_map_test
                     data_partition_test
                     dev_transpose_test
                     isolated_table_proxy_test
                     range_test
                     parallel_write_test
                     in_place_test
                     selection_test
                     var_write_test
                     PROPERTIES ENVIRONMENT "LD_LIBRARY_PATH=${VCPKG_LIBDIRS}")