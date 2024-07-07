#include <optional>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>

#include "arcae/result_shape.h"
#include "arcae/data_partition.h"
#include "arcae/selection.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <arrow/testing/gtest_util.h>

using ::arcae::detail::IndexType;
using ::arcae::detail::SpanPairs;
using ::arcae::detail::DataPartition;
using ::arcae::detail::ResultShapeData;
using ::arcae::detail::SelectionBuilder;

using ::casacore::DataType;
using ::casacore::IPosition;

namespace {

TEST(DataPartitionTest, Fixed) {
  auto result_shape = ResultShapeData{
    "DATA",
    IPosition{2, 3, 4}, 3,
    DataType::TpComplex,
    std::nullopt};

  auto selection = SelectionBuilder()
                      .Add({2, 1, -1, 5, 6, -1})
                      .Add({3, 0, 1})
                      .Build();

  ASSERT_OK_AND_ASSIGN(auto partition, DataPartition::Make(selection, result_shape));
  EXPECT_EQ(partition.data_chunks_.size(), 6);

  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].disk, ::testing::ElementsAre(-1, -1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].mem, ::testing::ElementsAre(2, 5));

  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].mem, ::testing::ElementsAre(1, 0));

  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].mem, ::testing::ElementsAre(3, 4));

  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].disk, ::testing::ElementsAre(-1, -1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].mem, ::testing::ElementsAre(2, 5));

  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].mem, ::testing::ElementsAre(1, 0));

  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].mem, ::testing::ElementsAre(3, 4));
}

TEST(DataPartitionTest, Variable) {
  auto result_shape = ResultShapeData{
    "DATA",
    std::nullopt, 3,
    DataType::TpComplex,
    std::vector<IPosition>{IPosition({3, 4}), IPosition({1, 4})}};

  auto selection = SelectionBuilder()
                      .Add({-1, 0})
                      .Add({3, 0, 1})
                      .Build();

  ASSERT_OK_AND_ASSIGN(auto partition, DataPartition::Make(selection, result_shape));
  EXPECT_EQ(partition.data_chunks_.size(), 4);

  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].disk, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].mem, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].disk, ::testing::ElementsAre(-1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].mem, ::testing::ElementsAre(0));

  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].disk, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].mem, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].disk, ::testing::ElementsAre(-1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].mem, ::testing::ElementsAre(0));

  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].mem, ::testing::ElementsAre(1));

  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].mem, ::testing::ElementsAre(1));
}

}  // namespace