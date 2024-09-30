#include "arcae/partition_sort.h"

#include <memory>

#include <arrow/api.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::arrow::ipc::internal::json::ArrayFromJSON;

using ::arcae::MergePartitions;
using ::arcae::PartitionSortData;

namespace {

TEST(GroupSortTest, TestSort) {
  std::vector<std::shared_ptr<arrow::Array>> groups;

  ASSERT_OK_AND_ASSIGN(auto group1,
                       ArrayFromJSON(arrow::int32(), "[1, 1, 1, 2, 2, 2, 3, 3, 3]"));
  ASSERT_OK_AND_ASSIGN(auto group2,
                       ArrayFromJSON(arrow::int32(), "[1, 1, 1, 2, 2, 2, 3, 3, 3]"));
  groups.push_back(group1);
  groups.push_back(group2);
  ASSERT_OK_AND_ASSIGN(auto time,
                       ArrayFromJSON(arrow::float64(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]"));
  ASSERT_OK_AND_ASSIGN(auto ant1,
                       ArrayFromJSON(arrow::int32(), "[1, 1, 1, 2, 2, 2, 3, 3, 3]"));
  ASSERT_OK_AND_ASSIGN(auto ant2,
                       ArrayFromJSON(arrow::int32(), "[1, 1, 1, 2, 2, 2, 3, 3, 3]"));
  ASSERT_OK_AND_ASSIGN(auto rows,
                       ArrayFromJSON(arrow::int64(), "[1, 2, 3, 4, 5, 6, 7, 8, 9]"));

  ASSERT_OK_AND_ASSIGN(auto base,
                       PartitionSortData::Make(groups, time, ant1, ant2, rows));
  ASSERT_OK_AND_ASSIGN(auto sorted, base->Sort());
  ASSERT_OK_AND_ASSIGN(auto merged, MergePartitions({sorted, sorted, sorted, sorted}));
}

}  // namespace
