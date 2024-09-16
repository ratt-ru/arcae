#include "arcae/selection.h"

#include <cstdint>
#include <numeric>

#include <gtest/gtest.h>

using ::arcae::detail::Index;
using ::arcae::detail::IndexSpan;
using ::arcae::detail::SelectionBuilder;

TEST(SelectionTests, InitializerLists) {
  auto sel = SelectionBuilder::FromInit({{0, 1, 2}, {0, 1}});
  EXPECT_EQ(sel.nIndices(), 2);
  EXPECT_EQ(sel.Size(), 2);

  auto data = sel.GetRowSpan().data();
  auto size = sel.GetRowSpan().size();
  EXPECT_EQ(std::accumulate(data, data + size, 0.0), 3);

  data = sel[0].data();
  size = sel[0].size();
  EXPECT_EQ(std::accumulate(data, data + size, 0.0), 1);
}

TEST(SelectionTests, ArgumentPacks) {
  auto vec = std::vector<int64_t>{1, 2, 3};
  auto sel = SelectionBuilder::FromArgs(std::vector<int>{0, 1, 2},
                                        std::vector<uint32_t>{1, 2, 3},
                                        std::vector<int64_t>{1, 2, 3, 4}, vec);

  EXPECT_EQ(sel.nIndices(), 3);
  EXPECT_EQ(sel.Size(), 4);

  int sums[] = {3, 6, 10, 6};
  int sizes[] = {3, 3, 4, 3};
  EXPECT_EQ(sizeof(sums) / sizeof(int), sel.Size());

  for (int i = 0; i < sel.Size(); ++i) {
    auto data = sel[i].data();
    auto size = sel[i].size();
    EXPECT_EQ(size, sizes[i]);
    EXPECT_EQ(std::accumulate(data, data + size, 0.0), sums[i]);
  }
}

TEST(SelectionTests, Builder) {
  auto sel = SelectionBuilder().Add({0, 1, 2}).Add(std::vector<long>{1, 2, 3, 4}).Build();
  EXPECT_EQ(sel.nIndices(), 2);
  EXPECT_EQ(sel.Size(), 2);

  EXPECT_EQ(sel[0].size(), 4);
  EXPECT_EQ(sel[0][0], 1);
  EXPECT_EQ(sel[0][1], 2);
  EXPECT_EQ(sel[0][2], 3);
  EXPECT_EQ(sel[0][3], 4);

  EXPECT_EQ(sel[1].size(), 3);
  EXPECT_EQ(sel[1][0], 0);
  EXPECT_EQ(sel[1][1], 1);
  EXPECT_EQ(sel[1][2], 2);
}

TEST(SelectionTests, BuilderFortranOrder) {
  auto sel = SelectionBuilder()
                 .Add({0, 1, 2})
                 .Add(std::vector<long>{1, 2, 3, 4})
                 .Order('F')
                 .Build();
  EXPECT_EQ(sel.nIndices(), 2);
  EXPECT_EQ(sel.Size(), 2);

  EXPECT_EQ(sel[1].size(), 4);
  EXPECT_EQ(sel[1][0], 1);
  EXPECT_EQ(sel[1][1], 2);
  EXPECT_EQ(sel[1][2], 3);
  EXPECT_EQ(sel[1][3], 4);

  EXPECT_EQ(sel[0].size(), 3);
  EXPECT_EQ(sel[0][0], 0);
  EXPECT_EQ(sel[0][1], 1);
  EXPECT_EQ(sel[0][2], 2);
}
