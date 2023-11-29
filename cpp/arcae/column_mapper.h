#ifndef ARCAE_COLUMN_MAPPER_H
#define ARCAE_COLUMN_MAPPER_H

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <numeric>
#include <type_traits>
#include <vector>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>

namespace arcae {
/// Utility class for mapping between on-disk
/// and in-memory indices
template <typename T=casacore::rownr_t>
class ColumnMapping {
  static_assert(std::is_integral_v<T>, "T is not integral");

public:
  // Public Types
  // Describes a mapping between two dimension id's
  struct IdMap {
    T from;
    T to;

    constexpr inline bool operator==(const IdMap & lhs) const
        { return from == lhs.from && to == lhs.to; }
  };

  // Describes a range along a dimension (end is exclusive)
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
  class MapIterator {
    private:
      const RangeIterator & rit_;
      std::vector<T> current_;
      bool done_;
    public:

      // Initialise the current map from the range starts
      // of the encapsulated RangeIterator
      static std::vector<T> CurrentFromRangeIterator(const RangeIterator & rit) {
        auto result = std::vector<T>(rit.nDim(), T{0});
        for(std::ptrdiff_t dim=0; dim < rit.nDim(); ++dim) {
          result[dim] = rit.DimRange(dim).start;
        }
        return result;
      }

      std::size_t FlatDestination(const casacore::IPosition & shape) {
        assert(rit_.nDim() > 0);
        auto product = std::size_t{1};
        auto result = std::size_t{0};

        for(std::ptrdiff_t dim = rit_.nDim() - 1; dim >= 0; --dim) {
          result += product * CurrentId(dim).to;
          product *= shape[dim];
        }

        return result;
      }

      std::size_t FlatSource(const casacore::IPosition & shape) {
        assert(rit_.nDim() > 0);
        auto product = std::size_t{1};
        auto result = std::size_t{0};

        for(std::ptrdiff_t dim = rit_.nDim() - 1; dim >= 0; --dim) {
          result += product * CurrentId(dim).from;
          product *= shape[dim];
        }

        return result;
      }

      std::pair<T, T> FlatMap(const casacore::IPosition & source, const casacore::IPosition & dest) {
        assert(rit_.nDim() > 0);
        assert(source.size() == dest.size());
        assert(source.size() == rit_.nDim());
        auto product = std::pair<T, T>{1, 1};
        auto result = std::pair<T, T>{0, 0};

        for(auto dim = rit_.nDim() - 1; dim >= 0; --dim) {
          const auto & id_map = CurrentId(dim);
          result.first += product.first * id_map.from;
          result.second += product.second * id_map.to;
          product.first *= source[dim];
          product.second *= dest[dim];
        }

        return result;
      }

      inline const IdMap & CurrentId(std::size_t dim) const {
        return rit_.DimMaps(dim)[current_[dim]];
      }

      MapIterator(const RangeIterator & rit, bool done)
        : rit_(rit), current_(CurrentFromRangeIterator(rit)), done_(done) {}

      MapIterator & operator++();
      std::vector<IdMap> operator*() const;
      bool operator==(const MapIterator & other) const;

      inline bool operator!=(const MapIterator & other) const {
        return !(*this == other);
      }
  };

  // Iterates over the Disjoint Ranges defined by a ColumnMapping
  class RangeIterator {
    private:
      const ColumnMapping & map_;
      std::vector<std::size_t> index_;
      bool done_;

    public:
      RangeIterator(ColumnMapping & column_map, bool done=false) :
        map_(column_map), done_(done), index_(column_map.nDim(), 0) {}

      // Return the number of dimensions in the index
      inline const std::ptrdiff_t nDim() const {
        return index_.size();
      }

      // Return the Ranges for the given dimension
      inline const ColumnRange & DimRanges(std::size_t dim) const {
        return map_.DimRanges(dim);
      }

      // Return the Maps for the given dimension
      inline const ColumnMap & DimMaps(std::size_t dim) const {
        return map_.DimMaps(dim);
      }

      // Return the currently selected Range of the given dimension
      inline const Range & DimRange(std::size_t dim) const {
        return DimRanges(dim)[index_[dim]];
      }

      inline MapIterator MapBegin() const {
        return MapIterator(*this, false);
      }

      inline MapIterator MapEnd() const {
        return MapIterator(*this, true);
      }

      // Returns a slicer for the row dimension
      casacore::Slicer GetRowSlicer() const;
      // Returns a slicer for secondary dimensions
      casacore::Slicer GetSectionSlicer() const;
      casacore::Slicer operator*() const;
      RangeIterator & operator++();
      bool operator==(const RangeIterator & other) const;

      inline bool operator!=(const RangeIterator & other) const {
        return !(*this == other);
      }
  };

private:
  // Private members
  ColumnMaps maps_;
  ColumnRanges ranges_;
  casacore::IPosition shape_;

public:
  // Public methods
  ColumnMapping(const ColumnSelection & column_selection={},
                const casacore::IPosition & shape=casacore::IPosition())
    : maps_(MakeMaps(column_selection)),
      ranges_(MakeRanges(maps_)),
      shape_(shape) {}

  /// Construct ColumnMaps from the provided Column Selection
  static ColumnMaps MakeMaps(const ColumnSelection & column_selection);
  /// Construct ColumnRanges from the provided maps (derived from MakeMaps)
  static ColumnRanges MakeRanges(const ColumnMaps & maps);

  /// Number of elements in this mapping
  inline std::ptrdiff_t nElements() const {
    return std::accumulate(std::begin(maps_), std::end(maps_), std::ptrdiff_t(1),
                           [](const auto & init, const auto & map)
                                { return init * map.size(); });
  }

  /// Number of disjoint ranges in this mapping
  inline std::ptrdiff_t nRanges() const {
    return std::accumulate(std::begin(ranges_), std::end(ranges_), std::ptrdiff_t(1),
                           [](const auto & init, const auto & range)
                                { return init * range.size(); });
  }

  inline RangeIterator RangeBegin() const {
    return RangeIterator{const_cast<ColumnMapping<T> &>(*this), false};
  }

  inline RangeIterator RangeEnd() const {
    return RangeIterator{const_cast<ColumnMapping<T> &>(*this), true};
  }

  // Return the Ranges for the given dimension
  inline const ColumnRange & DimRanges(std::size_t dim) const {
    return ranges_[dim];
  }

  // Return the Maps for the given dimension
  inline const ColumnMap & DimMaps(std::size_t dim) const {
    return maps_[dim];
  }

  /// Returns true if this is a simple mapping. A mapping is simple
  /// if the following holds:
  /// 1. There is a single mapping range in each dimension
  /// 2. Each IdMap in the mapping range is monotically increasing
  ///    in both the from and to field
  bool IsSimple() const;
  casacore::IPosition GetShape() const;

  inline const ColumnMaps & GetMaps() const { return maps_;  }
  inline const ColumnRanges & GetRanges() const { return ranges_; }
  inline const std::size_t nDim() const { return ranges_.size(); }
};


template <typename T>
std::vector<typename ColumnMapping<T>::IdMap>
ColumnMapping<T>::MapIterator::operator*() const {
  assert(!done_);
  auto result = std::vector<IdMap>(current_.size(), {0, 0});

  for(auto dim=0; dim < current_.size(); ++dim) {
    result[dim] = CurrentId(dim);
  }

  return result;
}


template <typename T>
typename ColumnMapping<T>::MapIterator &
ColumnMapping<T>::MapIterator::operator++() {
  assert(!done_);
  // Iterate from fastest to slowest changing dimension
  for(std::size_t dim = current_.size() - 1; dim >= 0;) {
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

template <typename T>
bool ColumnMapping<T>::MapIterator::operator==(const MapIterator & other) const {
  if(&rit_ != &other.rit_ || done_ != other.done_) return false;
  return done_ ? true : current_ == other.current_;
}

template <typename T>
casacore::Slicer
ColumnMapping<T>::RangeIterator::GetRowSlicer() const {
  assert(!done_);
  assert(index_.size() > 0);
  const auto & dim_maps = DimMaps(0);
  const auto & range = DimRange(0);
  auto start = dim_maps[range.start].from;
  auto end = dim_maps[range.end - 1].from;
  // TODO: casacore rownr_t is unsigned but ssize_t is signed
  // Find a proper solution to the narrowing conversion and
  // possible resulting issues
  return casacore::Slicer(
    casacore::IPosition({static_cast<ssize_t>(start)}),
    casacore::IPosition({static_cast<ssize_t>(end)}),
    casacore::Slicer::endIsLast);
}

template <typename T>
casacore::Slicer
ColumnMapping<T>::RangeIterator::GetSectionSlicer() const {
  assert(!done_);
  assert(nDim() > 1);
  casacore::IPosition start(nDim() - 1, 0);
  casacore::IPosition end(nDim() - 1, 0);

  for(std::ptrdiff_t dim = 1; dim < nDim(); ++dim) {
    const auto & dim_maps = DimMaps(dim);
    const auto & range = DimRange(dim);
    // TODO: casacore rownr_t is unsigned but ssize_t is signed
    // Find a proper solution to the narrowing conversion and
    // possible resulting issues
    start[nDim() - 1 - dim] = static_cast<ssize_t>(dim_maps[range.start].from);
    end[nDim() - 1 - dim] = static_cast<ssize_t>(dim_maps[range.end - 1].from);
  }

  return casacore::Slicer(start, end, casacore::Slicer::endIsLast);
}

template <typename T>
casacore::Slicer
ColumnMapping<T>::RangeIterator::operator*() const {
  assert(!done_);
  auto start = casacore::IPosition(index_.size());
  auto end = casacore::IPosition(index_.size());

  for(std::size_t dim=0; dim < index_.size(); ++dim) {
    const auto & dim_maps = DimMaps(dim);
    const auto & range = DimRange(dim);
    // TODO: casacore rownr_t is unsigned but ssize_t is signed
    // Find a proper solution to the narrowing conversion and
    // possible resulting issues
    start[dim] = static_cast<ssize_t>(dim_maps[range.start].from);
    end[dim] = static_cast<ssize_t>(dim_maps[range.end - 1].from);
  }

  return casacore::Slicer(start, end, casacore::Slicer::endIsLast);
}

template <typename T>
typename ColumnMapping<T>::RangeIterator &
ColumnMapping<T>::RangeIterator::operator++() {
  assert(!done_);
  // Iterate from fastest to slowest changing dimension
  for(std::size_t dim = index_.size() - 1; dim >= 0;) {
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

template <typename T>
bool ColumnMapping<T>::RangeIterator::operator==(const RangeIterator & other) const {
  if(&map_ != &other.map_ || done_ != other.done_) return false;
  return done_ ? true : index_ == other.index_;
}

template <class>
inline constexpr bool always_false_v = false;

template <typename T> typename ColumnMapping<T>::ColumnMaps
ColumnMapping<T>::MakeMaps(const ColumnSelection & column_selection)
{
  ColumnMaps column_maps;
  column_maps.reserve(column_selection.size());

  for(std::size_t dim=0; dim < column_selection.size(); ++dim) {
      const auto & column_ids = column_selection[dim];
      if(column_ids.size() == 0) {
        continue;
      }
      // if(column_ids.size() == 0) {
      //   auto dim_size = std::visit([&](auto && arg) -> std::size_t {
      //     using VT = std::decay_t<decltype(arg)>;
      //     if constexpr (std::is_same_v<VT, casacore::IPosition>) {
      //       assert(arg.size() == column_selection.size());
      //       return arg[arg.size() - dim - 1];
      //     } else if constexpr (std::is_same_v<VT, std::vector<casacore::IPosition>>) {
      //       // rows
      //       if(dim == 0) {
      //         return arg.size();
      //       }
      //       return std::accumulate(std::begin(arg), std::end(arg), std::size_t{0},
      //         [&](const auto & init, auto & row_shape) {
      //           assert(row_shape.size() + 1 < dim);
      //           return std::max(init, row_shape.size() - dim - 1);
      //       });
      //     } else {
      //       static_assert(always_false_v<VT>, "non-exhaustive visitor");
      //     }
      //   }, shape);

      //   auto ids = ColumnMap(dim_size, {0, 0});
      //   for(std::size_t i=0; i < ids.size(); ++i) {
      //     ids[i] = {i, i};
      //   }

      //   column_maps.emplace_back(std::move(ids));
      //   continue;
      // }

      ColumnMap column_map;
      column_map.reserve(column_ids.size());

      for(auto [it, to] = std::tuple{std::begin(column_ids), T{0}};
          it != std::end(column_ids); ++to, ++it) {
            column_map.push_back({*it, to});
      }

      std::sort(std::begin(column_map), std::end(column_map),
              [](const auto & lhs, const auto & rhs) {
                  return lhs.from < rhs.from; });

      column_maps.emplace_back(std::move(column_map));
  }

  return column_maps;
}

template <typename T> typename ColumnMapping<T>::ColumnRanges
ColumnMapping<T>::MakeRanges(const ColumnMaps & maps) {
  ColumnRanges column_ranges;
  column_ranges.reserve(maps.size());

  for(std::size_t dim=0; dim < maps.size(); ++dim) {
      const auto & column_map = maps[dim];
      if(column_map.size() == 0) {
        continue;
      }

      auto column_range = ColumnRange{};
      auto current = Range{0, 1};

      for(auto [i, prev, next] = std::tuple{
              T{1},
              std::begin(column_map),
              std::next(std::begin(column_map))};
          next != std::end(column_map); ++i, ++prev, ++next) {

          if(next->from - prev->from == 1) {
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

template <typename T>
bool ColumnMapping<T>::IsSimple() const {
  for(std::size_t dim=0; dim < maps_.size(); ++dim) {
    const auto & column_map = DimMaps(dim);
    const auto & column_range = DimRanges(dim);

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


template <typename T>
casacore::IPosition ColumnMapping<T>::GetShape() const {
    casacore::IPosition shape(nDim(), 0);

    for(auto dim = std::size_t{0}; dim < nDim(); ++dim) {
      shape[dim] = DimMaps(dim).size();
    }

    return shape;
  }

} // namespace arcae

#endif // ARCAE_COLUMN_MAPPER_H
