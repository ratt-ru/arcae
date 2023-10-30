
#include <arcae/column_mapper.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arrow/testing/gtest_util.h>

using C = ColumnMapping<std::int32_t>;

TEST(RangeDeathTest, CheckMapsAndRangeNull) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(C(C::ColumnSelection{}),
                 "column_selection.size\\(\\) > 0");
    ASSERT_DEATH(C(C::ColumnSelection{{}, {0}}),
                 "c.size\\(\\) > 0");
}

TEST(RangeTest, CheckMapsAndRangesSingleton) {
    auto map = C(C::ColumnSelection{{0}});

    EXPECT_THAT(map.GetMaps()[0],
               ::testing::ElementsAre(C::IdMap{0, 0}));

    EXPECT_THAT(map.GetRanges()[0],
                ::testing::ElementsAre(C::Range{0, 1}));
}

TEST(RangeTest, CheckMapsAndRangesMultiple) {
    auto map = C({
        {4, 3, 2, 1, 8, 7},    // Two disjoint ranges
        {5, 6},                // One range
        {7, 9, 8, 12, 11}});   // Two disjoint ranges

    const auto & maps = map.GetMaps();
    const auto & ranges = map.GetRanges();

    EXPECT_EQ(maps.size(), 3);
    EXPECT_EQ(ranges.size(), 3);

    EXPECT_THAT(maps[0], ::testing::ElementsAre(
        C::IdMap{1, 3},
        C::IdMap{2, 2},
        C::IdMap{3, 1},
        C::IdMap{4, 0},
        C::IdMap{7, 5},
        C::IdMap{8, 4}));

    EXPECT_THAT(ranges[0], ::testing::ElementsAre(
        C::Range{0, 4},
        C::Range{4, 6}));

    EXPECT_THAT(maps[1], ::testing::ElementsAre(
        C::IdMap{5, 0}, C::IdMap{6, 1}));

    EXPECT_THAT(ranges[1], ::testing::ElementsAre(
        C::Range{0, 2}));

    EXPECT_THAT(maps[2], ::testing::ElementsAre(
        C::IdMap{7, 0},
        C::IdMap{8, 2},
        C::IdMap{9, 1},
        C::IdMap{11, 4},
        C::IdMap{12, 3}));

    EXPECT_THAT(ranges[2], ::testing::ElementsAre(
        C::Range{0, 3},
        C::Range{3, 5}));

    ASSERT_FALSE(map.IsSimple());
}

TEST(RangeTest, TestSimplicity) {
    EXPECT_TRUE(C({{1, 2, 3, 4}}, C::FORWARD).IsSimple());
    EXPECT_TRUE(C({{1, 2, 3, 4}}, C::BACKWARD).IsSimple());
    EXPECT_TRUE(C({{1, 2, 3, 4}, {5, 6}, {6, 7}}, C::FORWARD).IsSimple());
    EXPECT_TRUE(C({{1, 2, 3, 4}, {5, 6}, {6, 7}}, C::BACKWARD).IsSimple());

    // Multiple mapping ranges (discontiguous)
    EXPECT_FALSE(C({{1, 2, 4, 5}}).IsSimple());

    // Not monotically increasing
    EXPECT_FALSE(C({{4, 3, 2, 1}}).IsSimple());
}


TEST(RangeTest, IteratorTest) {
    auto map = C({
        {4, 3, 2, 1, 8, 7, 20},    // Three disjoint ranges
        {5, 6, 8, 9},              // Two disjoin range
        {7, 9, 8, 12, 11}});       // Two disjoint ranges

    auto it = map.RangeBegin();

    EXPECT_THAT(*it, ::testing::ElementsAre(0, 0, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(0, 0, 1)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(0, 1, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(0, 1, 1)); ++it;

    EXPECT_THAT(*it, ::testing::ElementsAre(1, 0, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(1, 0, 1)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(1, 1, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(1, 1, 1)); ++it;

    EXPECT_THAT(*it, ::testing::ElementsAre(2, 0, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(2, 0, 1)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(2, 1, 0)); ++it;
    EXPECT_THAT(*it, ::testing::ElementsAre(2, 1, 1)); ++it;

    EXPECT_EQ(it, map.RangeEnd());
}
