#include <iostream>
#include <numeric>
#include <vector>

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <arrow/testing/gtest_util.h>


template <typename T=std::int32_t>
class ColumnMapping {
  static_assert(std::is_integral_v<T>, "T is not integral");
  static_assert(std::is_signed_v<T>, "T is not signed");

public:
  enum Direction {
    FORWARD=0,
    BACKWARD
  };

  struct IdMap {
    T from;
    T to;

    bool operator==(const IdMap & lhs) const
        { return from == lhs.from && to == lhs.to; }
    static constexpr IdMap Empty() { return IdMap{-1, -1}; };
    constexpr bool IsEmpty() const { return *this == Empty(); }
  };

  struct Range {
    IdMap start;
    IdMap end;

    bool operator==(const Range & lhs) const
        { return start == lhs.start && end == lhs.end; }
  };

  using ValueType = T;
  using ColumnIds = std::vector<T>;
  using ColumnSelection = std::vector<ColumnIds>;
  using ColumnMap = std::vector<IdMap>;
  using ColumnRange = std::vector<Range>;
  using ColumnRanges = std::vector<ColumnRange>;

  static ColumnRanges DecomposeIntoRanges(const ColumnSelection & column_selection) {
    ColumnRanges ranges;
    ranges.reserve(column_selection.size());

    for(std::size_t c=0; c < column_selection.size(); ++c) {
        const auto & column_ids = column_selection[c];
        const auto & column_ranges = ranges[c];
    }

    return ranges;
  }

  ColumnRanges ranges_;

  ColumnMapping() = default;
  ColumnMapping(const ColumnIds & column_selection, Direction direction=FORWARD)
    : ranges_(DecomposeIntoRanges(column_selection)) {}

};

TEST(RangeTests, RangeTest) {
    {
        using C = ColumnMapping<std::int32_t>;
        C::ColumnIds col_ids = {4, 3, 2, 1, 7, 6, 11, 10, 9, 13};
        C::ColumnMap col_map;
        col_map.reserve(col_ids.size());

        for(auto [from_it, to] = std::tuple{std::begin(col_ids), C::ValueType(0)};
                from_it != std::end(col_ids); ++to, ++from_it) {
            col_map.push_back({*from_it, to});
        }

        EXPECT_THAT(col_map, ::testing::ElementsAre(
            C::IdMap{4, 0},
            C::IdMap{3, 1},
            C::IdMap{2, 2},
            C::IdMap{1, 3},
            C::IdMap{7, 4},
            C::IdMap{6, 5},
            C::IdMap{11, 6},
            C::IdMap{10, 7},
            C::IdMap{9, 8},
            C::IdMap{13, 9}));


        std::sort(std::begin(col_map), std::end(col_map),
            [](const auto & lhs, const auto & rhs) {
                return lhs.from < rhs.from;});

        EXPECT_THAT(col_map, ::testing::ElementsAre(
            C::IdMap{1, 3},
            C::IdMap{2, 2},
            C::IdMap{3, 1},
            C::IdMap{4, 0},
            C::IdMap{6, 5},
            C::IdMap{7, 4},
            C::IdMap{9, 8},
            C::IdMap{10, 7},
            C::IdMap{11, 6},
            C::IdMap{13, 9}));


        if(col_map.size() == 0) {
            abort();
        }

        std::vector<C::Range> ranges;
        auto current = C::Range{*std::begin(col_map), C::IdMap::Empty()};

        for(auto [prev, next] = std::tuple{std::begin(col_map), std::next(std::begin(col_map))};
            next != std::end(col_map); ++prev, ++next) {

            if(next->from - prev->from == 1) {
                current.end = *next;
            } else {
                ranges.push_back(current);
                current = C::Range{*next, C::IdMap::Empty()};
            }
        }

        ranges.push_back(current);

        for(const auto & range: ranges) {
            std::cout << range.start.from << " -> " << range.end.from << std::endl;
        }

        EXPECT_THAT(ranges, ::testing::ElementsAre(
            C::Range{{1, 3}, {4, 0}},
            C::Range{{6, 5}, {7, 4}},
            C::Range{{9, 8}, {11, 6}},
            C::Range{{13, 9}, {-1, -1}}));
    }
}