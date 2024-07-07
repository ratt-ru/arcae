#include <optional>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
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
using ::casacore::Slicer;

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
  EXPECT_EQ(partition.data_chunks_[0].GetRowSlicer(),
            Slicer(IPosition({-1}), IPosition({-1}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[0].GetSectionSlicer(),
            Slicer(IPosition({0, 0}), IPosition({1, 1}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].mem, ::testing::ElementsAre(1, 0));
  EXPECT_EQ(partition.data_chunks_[1].GetRowSlicer(),
            Slicer(IPosition({1}), IPosition({2}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[1].GetSectionSlicer(),
            Slicer(IPosition({0, 0}), IPosition({1, 1}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].mem, ::testing::ElementsAre(3, 4));
  EXPECT_EQ(partition.data_chunks_[2].GetRowSlicer(),
            Slicer(IPosition({5}), IPosition({6}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[2].GetSectionSlicer(),
            Slicer(IPosition({0, 0}), IPosition({1, 1}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].disk, ::testing::ElementsAre(-1, -1));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].mem, ::testing::ElementsAre(2, 5));
  EXPECT_EQ(partition.data_chunks_[3].GetRowSlicer(),
            Slicer(IPosition({-1}), IPosition({-1}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[3].GetSectionSlicer(),
            Slicer(IPosition({0, 3}), IPosition({1, 3}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].disk, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[4].dim_spans_[2].mem, ::testing::ElementsAre(1, 0));
  EXPECT_EQ(partition.data_chunks_[4].GetRowSlicer(),
            Slicer(IPosition({1}), IPosition({2}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[4].GetSectionSlicer(),
            Slicer(IPosition({0, 3}), IPosition({1, 3}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[0].mem, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].disk, ::testing::ElementsAre(5, 6));
  EXPECT_THAT(partition.data_chunks_[5].dim_spans_[2].mem, ::testing::ElementsAre(3, 4));
  EXPECT_EQ(partition.data_chunks_[5].GetRowSlicer(),
            Slicer(IPosition({5}), IPosition({6}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[5].GetSectionSlicer(),
            Slicer(IPosition({0, 3}), IPosition({1, 3}), Slicer::endIsLast));
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
  EXPECT_EQ(partition.data_chunks_[0].GetRowSlicer(),
            Slicer(IPosition({-1}), IPosition({-1}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[0].GetSectionSlicer(),
            Slicer(IPosition({0, 0}), IPosition({2, 1}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].disk, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[0].mem, ::testing::ElementsAre(0, 1, 2));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].disk, ::testing::ElementsAre(-1));
  EXPECT_THAT(partition.data_chunks_[1].dim_spans_[2].mem, ::testing::ElementsAre(0));
  EXPECT_EQ(partition.data_chunks_[1].GetRowSlicer(),
            Slicer(IPosition({-1}), IPosition({-1}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[1].GetSectionSlicer(),
            Slicer(IPosition({0, 3}), IPosition({2, 3}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[0].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].disk, ::testing::ElementsAre(0, 1));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[1].mem, ::testing::ElementsAre(1, 2));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[2].dim_spans_[2].mem, ::testing::ElementsAre(1));
  EXPECT_EQ(partition.data_chunks_[2].GetRowSlicer(),
            Slicer(IPosition({0}), IPosition({0}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[2].GetSectionSlicer(),
            Slicer(IPosition({0, 0}), IPosition({0, 1}), Slicer::endIsLast));

  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[0].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].disk, ::testing::ElementsAre(3));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[1].mem, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].disk, ::testing::ElementsAre(0));
  EXPECT_THAT(partition.data_chunks_[3].dim_spans_[2].mem, ::testing::ElementsAre(1));
  EXPECT_EQ(partition.data_chunks_[3].GetRowSlicer(),
            Slicer(IPosition({0}), IPosition({0}), Slicer::endIsLast));
  EXPECT_EQ(partition.data_chunks_[3].GetSectionSlicer(),
            Slicer(IPosition({0, 3}), IPosition({0, 3}), Slicer::endIsLast));
}

}  // namespace