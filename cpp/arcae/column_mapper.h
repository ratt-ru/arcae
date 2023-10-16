#ifndef ARCAE_COLUMN_MAPPER_H
#define ARCAE_COLUMN_MAPPER_H

#include <algorithm>
#include <cstdint>
#include <vector>

/// Utility class for mapping between in-disk and
/// in-memory indices
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

    constexpr inline bool operator==(const IdMap & lhs) const
        { return from == lhs.from && to == lhs.to; }
    static constexpr inline IdMap Empty() { return IdMap{-1, -1}; };
    constexpr inline bool IsEmpty() const { return *this == Empty(); }
  };

  struct Range {
    IdMap start;
    IdMap end;

    constexpr inline bool operator==(const Range & lhs) const
        { return start == lhs.start && end == lhs.end; }
    constexpr inline bool IsValid() { return start.from >= 0 && start.to >= 0; }
    constexpr inline bool IsSingleton() { return IsValid() && end.IsEmpty(); }
    static constexpr inline Range Empty() { return {IdMap::Empty(), IdMap::Empty()}; }
  };

  using ValueType = T;
  using ColumnIds = std::vector<T>;
  using ColumnSelection = std::vector<ColumnIds>;
  using ColumnMap = std::vector<IdMap>;
  using ColumnRange = std::vector<Range>;
  using ColumnRanges = std::vector<ColumnRange>;

  ColumnMapping() = delete;
  ColumnMapping(const ColumnSelection & column_selection={}, Direction direction=FORWARD)
    : ranges_(DecomposeIntoRanges(column_selection, direction)) {}

  static ColumnRanges DecomposeIntoRanges(const ColumnSelection & column_selection,
                                          Direction direction);

  ColumnRanges ranges_;
};

template <typename T> typename ColumnMapping<T>::ColumnRanges
ColumnMapping<T>::DecomposeIntoRanges(const ColumnSelection & column_selection, Direction direction)
{
    ColumnRanges ranges;
    ranges.reserve(column_selection.size());

    for(std::size_t dim=0; dim < column_selection.size(); ++dim) {
        const auto & column_ids = column_selection[dim];
        ColumnRange column_range;

        ColumnMap column_map;
        column_map.reserve(column_ids.size());

        for(auto [from_it, to] = std::tuple{std::begin(column_ids), ValueType(0)};
                from_it != std::end(column_ids); ++to, ++from_it) {
            column_map.push_back({*from_it, to});
        }

        if(column_map.size() == 0) {
            column_range.push_back(Range::Empty());
            continue;
        }

        if(direction == Direction::FORWARD) {
            std::sort(std::begin(column_map), std::end(column_map),
                    [](const auto & lhs, const auto & rhs) {
                        return lhs.from < rhs.from; });
        } else {
            std::sort(std::begin(column_map), std::end(column_map),
                    [](const auto & lhs, const auto & rhs) {
                        return lhs.to < rhs.to; });
        }

        auto current = Range{*std::begin(column_map), IdMap::Empty()};

        for(auto [prev, next] = std::tuple{
                    std::begin(column_map),
                    std::next(std::begin(column_map))};
            next != std::end(column_map); ++prev, ++next) {

            if(next->from - prev->from == 1) {
                current.end = *next;
            } else {
                column_range.push_back(current);
                current = Range{*next, IdMap::Empty()};
            }
        }

        column_range.push_back(current);

        // for(auto & range: column_range) {
        //     std::cout << '[' << range.start.from << ", " << range.start.to
        //               << "] -> [" << range.end.from << ", " << range.end.to << "]" << std::endl;
        // }

        // std::cout << std::endl;

        ranges.emplace_back(std::move(column_range));
    }

    return ranges;
}



#endif // ARCAE_COLUMN_MAPPER_H
