#include <absl/types/span.h>
#include <optional>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>

#include "arcae/result_shape.h"
#include "arcae/data_partition.h"
#include "arcae/selection.h"

#include <arrow/testing/gtest_util.h>

using ::arcae::detail::IndexType;
using ::arcae::detail::SpanPair;
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

  auto vec = std::vector<IndexType>{4, 3};
  auto span_pair = SpanPair{absl::MakeSpan(vec), absl::MakeSpan(vec)};
  SpanPairs span_pairs;
  span_pairs.push_back(span_pair);
  SpanPairs span_pairs2;
  span_pairs2 = std::move(span_pairs);
  EXPECT_THAT(span_pairs2[0].disk, ::testing::ElementsAre(4, 3));

  auto selection = SelectionBuilder()
                      .Add({2, 1, -1, 5, 6, -1})
                      .Add({3, 0, 1})
                      .Build();

  ASSERT_OK_AND_ASSIGN(auto partition, DataPartition::Make(selection, result_shape));
  EXPECT_EQ(partition.data_chunks_.size(), 6);
  EXPECT_TRUE(partition.data_chunks_[0].dim_spans_.size() > 0);
  EXPECT_TRUE(partition.data_chunks_[0].dim_spans_[0].disk.size() == 2);

  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].disk, ::testing::ElementsAre(-1, -1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[0].mem, ::testing::ElementsAre(2, 5));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[0].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));

  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].disk, ::testing::ElementsAre(-1, -1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].mem, ::testing::ElementsAre(2, 5));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));

  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].mem, ::testing::ElementsAre(1, 0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));

  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].mem, ::testing::ElementsAre(1, 0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));

  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].mem, ::testing::ElementsAre(3, 4));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));

  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].mem, ::testing::ElementsAre(3, 4));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].disk, ::testing::ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].mem, ::testing::ElementsAre(0, 1, 2, 3));
}

}  // namespace