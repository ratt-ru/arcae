
#include <arcae/column_mapper.h>

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arrow/testing/gtest_util.h>



TEST(RangeTests, RangeTest) {
    using C = ColumnMapping<std::int32_t>;

    {
        auto map = C({{4, 3, 2, 1, 7, 6, 11, 10, 9, 13}});
        EXPECT_THAT(map.ranges_[0], ::testing::ElementsAre(
            C::Range{{1, 3}, {4, 0}},
            C::Range{{6, 5}, {7, 4}},
            C::Range{{9, 8}, {11, 6}},
            C::Range{{13, 9}, {-1, -1}}));
    }

    {
        auto map = C({{4, 3, 2, 1, 7, 6, 11, 10, 9, 13}}, C::BACKWARD);
        EXPECT_THAT(map.ranges_[0], ::testing::ElementsAre(
            C::Range{{4, 0}, {-1, -1}},
            C::Range{{3, 1}, {-1, -1}},
            C::Range{{2, 2}, {-1, -1}},
            C::Range{{1, 3}, {-1, -1}},
            C::Range{{7, 4}, {-1, -1}},
            C::Range{{6, 5}, {-1, -1}},
            C::Range{{11, 6}, {-1, -1}},
            C::Range{{10, 7}, {-1, -1}},
            C::Range{{9, 8}, {-1, -1}},
            C::Range{{13, 9}, {-1, -1}}));
    }
}