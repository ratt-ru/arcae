#ifndef ARCAE_COLUMN_MAPPER_H
#define ARCAE_COLUMN_MAPPER_H

#include <iostream> //remove

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <numeric>
#include <vector>

#include <casacore/casa/Arrays/Slicer.h>

namespace arcae {

/// Utility class for mapping between in-disk and
/// in-memory indices
template <typename T=std::int32_t>
class ColumnMapping {
  static_assert(std::is_integral_v<T>, "T is not integral");
  //static_assert(std::is_signed_v<T>, "T is not signed");

public:
  // Direction of the map
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
    T start;
    T end;

    constexpr inline bool operator==(const Range & lhs) const
        { return start == lhs.start && end == lhs.end; }
  };

  using ColumnIds = std::vector<T>;
  using ColumnSelection = std::vector<ColumnIds>;
  using ColumnMap = std::vector<IdMap>;
  using ColumnMaps = std::vector<ColumnMap>;
  using ColumnRange = std::vector<Range>;
  using ColumnRanges = std::vector<ColumnRange>;

  // Forward declaration
  class RangeIterator;

  // Iterates over the current mapping in the RangeIterator
  class ChunkIterator {
    public:
      const RangeIterator & rit_;
      std::vector<T> current_;
      bool done_;

      // Initialise the current position from the range starts
      // of the encapsulated RangeIterator
      static std::vector<T> CurrentFromRangeIterator(const RangeIterator & rit) {
        auto result = std::vector<T>(rit.nDim(), 0);
        for(auto dim=0; dim < rit.nDim(); ++dim) result[dim] = rit.DimRange(dim).start;
        return result;
      }

      ChunkIterator(const RangeIterator & rit, bool done)
        : rit_(rit), current_(CurrentFromRangeIterator(rit)), done_(done) {}

      ChunkIterator & operator++() {
        assert(!done_);
        // Iterate from fastest to slowest changing dimension
        std::size_t dim = current_.size() - 1;

        while(dim >= 0) {
          current_[dim]++;
          // We've achieved a successful iteration in this dimension
          if(current_[dim] < rit_.DimRange(dim).end) { break; }
          // Reset to zero and retry in the next dimension
          else if(dim > 0) { current_[dim] = rit_.DimRange(dim).start; --dim; }
          // This was the slowest changing dimension so we're done
          else { done_ = true; break; }
        }

        return *this;
      }

      std::vector<IdMap> operator*() const {
        assert(!done_);
        auto result = std::vector<IdMap>(current_.size(), IdMap::Empty());

        for(auto dim=0; dim < current_.size(); ++dim) {
          result[dim] = rit_.DimMap(dim)[current_[dim]];
        }

        return result;
      }

      bool operator==(const ChunkIterator & other) const {
        if(&rit_ != &other.rit_) return false;
        if(done_ && other.done_) return true;
        return done_ == other.done_ && current_ == other.current_;
      }

      bool operator!=(const ChunkIterator & other) const {
        return !(*this == other);
      }
  };

  // Iterates over the Disjoint Ranges defined by the ColumnMapping
  class RangeIterator {
    public:
      const ColumnMapping & map_;
      std::vector<std::size_t> index_;
      bool done_;

      RangeIterator(ColumnMapping & column_map, bool done=false) :
        map_(column_map), done_(done), index_(column_map.nDim(), 0) {}

      // Return the number of dimensions in the index
      inline const std::size_t nDim() const {
        return index_.size();
      }

      // Return the Ranges for the given dimension
      inline const ColumnRange & DimRanges(std::size_t dim) const {
        return map_.ranges_[dim];
      }

      // Return the Maps for the given dimension
      inline const ColumnMap & DimMap(std::size_t dim) const {
        return map_.maps_[dim];
      }

      // Return the currently selected Range of the given dimension
      inline const Range & DimRange(std::size_t dim) const {
        return DimRanges(dim)[index_[dim]];
      }

      inline ChunkIterator ChunkBegin() const {
        return ChunkIterator(*this, false);
      }

      inline ChunkIterator ChunkEnd() const {
        return ChunkIterator(*this, true);
      }

      casacore::Slicer operator*() {
        assert(!done_);
        auto start = casacore::IPosition(index_.size());
        auto end = casacore::IPosition(index_.size());

        for(std::size_t dim=0; dim < index_.size(); ++dim) {
          const auto & dim_maps = DimMap(dim);
          const auto & range = DimRange(dim);

          if(map_.direction_ == FORWARD) {
            start[dim] = dim_maps[range.start].from;
            end[dim] = dim_maps[range.end - 1].from;
          } else {
            start[dim] = dim_maps[range.start].to;
            end[dim] = dim_maps[range.end - 1].to;
          }
        }

        return casacore::Slicer(start, end, casacore::Slicer::endIsLast);
      }

      RangeIterator & operator++() {
        assert(!done_);
        // Iterate from fastest to slowest changing dimension
        std::size_t dim = index_.size() - 1;

        while(dim >= 0) {
          index_[dim]++;
          // We've achieved a successful iteration in this dimension
          if(index_[dim] < map_.ranges_[dim].size()) { break; }
          // We've exceeded the size of the current dimension
          // reset to zero and retry the while loop
          else if(dim > 0) { index_[dim] = 0; --dim; }
          // This was the slowest changing dimension so we're done
          else { done_ = true; break; }
        }

        return *this;
      }

      bool operator==(const RangeIterator & other) const {
        if(&map_ != &other.map_) return false;
        if(done_ && other.done_) return true;
        return done_ == other.done_ && index_ == other.index_;
      }

      bool operator!=(const RangeIterator & other) const {
        return !(*this == other);
      }
  };


  ColumnMapping(const ColumnSelection & column_selection={},
                Direction direction=FORWARD)
    : maps_(MakeMaps(column_selection, direction)),
      ranges_(MakeRanges(maps_, direction)),
      direction_(direction) {}

  static ColumnMaps MakeMaps(const ColumnSelection & column_selection,
                             Direction direction=FORWARD);
  static ColumnRanges MakeRanges(const ColumnMaps & maps,
                                 Direction direction=FORWARD);

  std::size_t nElements() const {
    return std::accumulate(std::begin(maps_), std::end(maps_), std::size_t(1),
                           [](const auto & init, const auto & map)
                                { return init * map.size(); });
  }

  std::size_t nRanges() const {
    return std::accumulate(std::begin(ranges_), std::end(ranges_), std::size_t(1),
                           [](const auto & init, const auto & range)
                                { return init * range.size(); });
  }

  RangeIterator RangeBegin() const {
    return RangeIterator{const_cast<ColumnMapping<T> &>(*this), false};
  }

  RangeIterator RangeEnd() const {
    return RangeIterator{const_cast<ColumnMapping<T> &>(*this), true};
  }

  bool IsSimple() const;
  const ColumnMaps & GetMaps() const { return maps_;  }
  const ColumnRanges & GetRanges() const { return ranges_; }
  const std::size_t nDim() const { return ranges_.size(); }

  Direction direction_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
};

template <typename T> typename ColumnMapping<T>::ColumnMaps
ColumnMapping<T>::MakeMaps(const ColumnSelection & column_selection, Direction direction)
{
    assert(column_selection.size() > 0);
    for(const auto & c: column_selection) assert(c.size() > 0);

    ColumnMaps column_maps;
    column_maps.reserve(column_selection.size());

    for(std::size_t dim=0; dim < column_selection.size(); ++dim) {
        const auto & column_ids = column_selection[dim];
        auto column_map = ColumnMap{};
        column_map.reserve(column_ids.size());

        for(auto [from_it, to] = std::tuple{std::begin(column_ids), T{0}};
                from_it != std::end(column_ids); ++to, ++from_it) {
            column_map.push_back({*from_it, to});
        }

        if(direction == FORWARD) {
            std::sort(std::begin(column_map), std::end(column_map),
                    [](const auto & lhs, const auto & rhs) {
                        return lhs.from < rhs.from; });
        } else {
            std::sort(std::begin(column_map), std::end(column_map),
                    [](const auto & lhs, const auto & rhs) {
                        return lhs.to < rhs.to; });
        }

        column_maps.emplace_back(std::move(column_map));
    }

    return column_maps;
}

template <typename T> typename ColumnMapping<T>::ColumnRanges
ColumnMapping<T>::MakeRanges(const ColumnMaps & maps, Direction direction) {
  ColumnRanges column_ranges;
  column_ranges.reserve(maps.size());

  for(std::size_t dim=0; dim < maps.size(); ++dim) {
      const auto & column_map = maps[dim];
      auto column_range = ColumnRange{};
      assert(column_map.size() > 0);
      auto begin_it = std::begin(column_map);
      auto current = Range{0, 1};

      for(auto [i, prev, next] = std::tuple{
              T{1},
              std::begin(column_map),
              std::next(std::begin(column_map))};
          next != std::end(column_map); ++i, ++prev, ++next) {

          if(direction == FORWARD && next->from - prev->from == 1) {
              current.end += 1;
          } else if(direction == BACKWARD && next->to - prev->to == 1) {
              current.end += 1;
          } else {
              column_range.push_back(current);
              current = Range{i, i + 1};
          }
      }

      column_range.push_back(current);
      column_ranges.emplace_back(std::move(column_range));
  }

  return column_ranges;
}

/// Returns true if this is a simple mapping. A mapping is simple
/// if the following holds:
/// 1. There is a single mapping range in each dimension
/// 2. Each IdMap in the mapping range is monotically increasing
///    in both the from and to field
template <typename T>
bool ColumnMapping<T>::IsSimple() const {
  for(std::size_t dim=0; dim < maps_.size(); ++dim) {
    const auto & column_map = maps_[dim];
    const auto & column_range = ranges_[dim];

    // More than one range of row ids in a dimension
    if(column_range.size() > 1) return false;

    for(auto &[start, end]: column_range) {
      for(std::size_t i = start + 1; i < end; ++i) {
        if(column_map[i].from - column_map[i-1].from != 1) {
          return false;
        }
        if(column_map[i].to - column_map[i-1].to != 1) {
          return false;
        }
      }
    }
  }

  return true;
}

} // namespace arcae

#endif // ARCAE_COLUMN_MAPPER_H
