#ifndef ARCAE_COLUMN_MAPPER_2
#define ARCAE_COLUMN_MAPPER_2

#include <cstddef>
#include <iterator>
#include <memory>
#include <numeric>
#include <optional>
#include <sys/types.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

using RowIds = std::vector<casacore::rownr_t>;
using ColumnSelection = std::vector<RowIds>;

// Describes a mapping between two dimension id's
struct IdMap {
  casacore::rownr_t from;
  casacore::rownr_t to;

  constexpr inline bool operator==(const IdMap & lhs) const
      { return from == lhs.from && to == lhs.to; }
};

// Vectors of ids
using ColumnMap = std::vector<IdMap>;
using ColumnMaps = std::vector<ColumnMap>;

// Describes a range along a dimension (end is exclusive)
struct Range {
  casacore::rownr_t start = 0;
  casacore::rownr_t end = 0;
  enum Type {
    // Refers to a series of specific row ids
    MAP=0,
    // A contiguous range of row ids
    FREE,
    // Specifies a range whose size we don't know
    UNCONSTRAINED
  } type = FREE;

  constexpr inline bool IsMap() const
    { return type == MAP; }

  constexpr inline bool IsFree() const
    { return type == FREE; }

  constexpr inline bool IsUnconstrained() const
    { return type == UNCONSTRAINED; }

  constexpr casacore::rownr_t nRows() const
    { return end - start; }

  constexpr inline bool IsSingleRow() const
    { return nRows() == 1; }

  constexpr inline bool IsValid() const
    { return start <= end; }

  constexpr inline bool operator==(const Range & lhs) const
      { return start == lhs.start && end == lhs.end; }
};

// Vectors of ranges
using ColumnRange = std::vector<Range>;
using ColumnRanges = std::vector<ColumnRange>;

// Holds variable shape data for a column
struct VariableShapeData {
  // Factory method for creating Variable Shape Data
  static arrow::Result<std::unique_ptr<VariableShapeData>>
  Make(const casacore::TableColumn & column, const ColumnSelection & selection) {
    assert(!column.columnDesc().isFixedShape());
    auto row_shapes = decltype(VariableShapeData::row_shapes_){};
    bool fixed_shape = true;
    bool fixed_dims = true;

    // No selection
    // Create row shape data from column.nrow()
    if(selection.size() == 0 || selection[0].size() == 0) {
      row_shapes.reserve(column.nrow());

      for(auto [r, first] = std::tuple{std::size_t{0}, true}; r < column.nrow(); ++r, first=false) {
        if(!column.isDefined(r)) {
          return arrow::Status::NotImplemented("Row ", r, " in column ",
                                               column.columnDesc().name(),
                                               " is not defined.");
        }
        row_shapes.push_back(column.shape(r));
        if(first) continue;
        fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
        fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
      }
    } else {
      // Create row shape data from row id selection
      const auto & row_ids = selection[0];
      row_shapes.reserve(row_ids.size());

      for(auto [r, first] = std::tuple{0, true}; r < row_ids.size(); ++r, first=false) {
        if(!column.isDefined(row_ids[r])) {
          return arrow::Status::NotImplemented("Row ", r, " in column ",
                                               column.columnDesc().name(),
                                               " is not defined.");
        }
        row_shapes.push_back(column.shape(row_ids[r]));
        if(first) continue;
        fixed_shape = fixed_shape && *std::rbegin(row_shapes) == *std::begin(row_shapes);
        fixed_dims = fixed_dims && std::rbegin(row_shapes)->size() == std::begin(row_shapes)->size();
      }
    }

    // Arrow can't handle differing dimensions per row, so we quit here.
    if(!fixed_dims) {
      return arrow::Status::NotImplemented("Column ", column.columnDesc().name(),
                                           " dimensions vary per row.");
    }

    // We may have a fixed shape in practice
    auto shape = fixed_shape ? std::make_optional(*std::begin(row_shapes))
                             : std::nullopt;

    return std::unique_ptr<VariableShapeData>(
      new VariableShapeData{std::move(row_shapes),
                            std::begin(row_shapes)->size(),
                            std::move(shape)});
  }

  // Returns true if the data shapes are fixed in practice
  inline bool IsActuallyFixed() const { return shape_.has_value(); }
  // Number of dimensions, excluding row
  inline std::size_t nDim() const { return ndim_; }

  std::vector<casacore::IPosition> row_shapes_;
  std::size_t ndim_;
  std::optional<casacore::IPosition> shape_;
};


// Provides Shape information for this column
// This easy in the case of Fixed Shape columns.
// This may not be possible in the Variable column case.
struct ShapeProvider {
public:
  const casacore::TableColumn & column_;
  const ColumnSelection & selection_;
  std::unique_ptr<VariableShapeData> var_data_;

  static arrow::Result<ShapeProvider> Make(const casacore::TableColumn & column,
                                           const ColumnSelection & selection) {

    if(column.columnDesc().isFixedShape()) {
      return ShapeProvider{column, selection, nullptr};
    }

    ARROW_ASSIGN_OR_RAISE(auto var_data, VariableShapeData::Make(column, selection));
    return ShapeProvider{column, selection, std::move(var_data)};
  }

  // Returns true if the column is defined as having a fixed shape
  inline bool IsDefinitelyFixed() const {
    return var_data_ == nullptr;
  }

  // Return true if the column is defined has having a varying shape
  inline bool IsVarying() const {
    return !IsDefinitelyFixed();
  }

  // Return true if the column has a fixed shape in practice
  inline bool IsActuallyFixed() const {
    return IsDefinitelyFixed() || var_data_->IsActuallyFixed();
  }

  // Returns the number of dimensions, including row
  std::size_t nDim() const {
    return (IsDefinitelyFixed() ? column_.columnDesc().ndim() : var_data_->nDim()) + 1;
  }

  // Returns the Row-Major (C) dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const {
    // If we have a selection of row id's,
    // derive the dimension size from these
    if(dim < selection_.size() && selection_[dim].size() > 0) {
      return selection_[dim].size();
    }

    assert(dim < nDim());

    // There's no selection for this dimension
    // so we must derive the dimension size
    // from the column shape information
    if(dim == 0) {
      // First dimension is just row
      return column_.nrow();
    } else if(IsDefinitelyFixed()) {
      // Fixed shape column, we have the size information
      return column_.shapeColumn()[nDim() - dim - 1];
    } else {
      const auto & shape = var_data_->shape_;

      if(!shape) {
        return arrow::Status::IndexError("Dimension ", dim, " in  column ",
                                         column_.columnDesc().name(),
                                         " is not fixed.");
      }

      // Even though the column is marked as variable
      // the individual row shapes are the same
      return shape.value()[nDim() - dim - 1];
    }
  }

  // Returns the Row-Major (C) dimension size of the column
  // for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    assert(IsVarying());
    assert(row < var_data_->row_shapes_.size());
    assert(dim > 0);
    return var_data_->row_shapes_[row][nDim() - dim - 1];
  }
};

class ColMap2 {
public:
  class RangeIterator;

  // Iterates over the current mapping in the RangeIterator
  class MapIterator {
    private:
      const RangeIterator & rit_;
      std::vector<casacore::rownr_t> current_;
      bool done_;
    public:
      // Initialise the current map from the range starts
      // of the encapsulated RangeIterator
      static std::vector<casacore::rownr_t> CurrentFromRangeIterator(const RangeIterator & rit) {
        auto result = std::vector<casacore::rownr_t>(rit.nDim(), 0);
        for(std::size_t dim=0; dim < rit.nDim(); ++dim) {
          result[dim] = rit.DimRange(dim).start;
        }
        return result;
      }

      MapIterator(const RangeIterator & rit, bool done)
        : rit_(rit), current_(CurrentFromRangeIterator(rit)), done_(done) {}

      inline IdMap CurrentId(std::size_t dim) const {
        assert(dim < rit_.nDim());
        const auto & range = rit_.DimRange(dim);

        switch(range.type) {
          case Range::FREE:
          case Range::UNCONSTRAINED:
            // Free and unconstrained ranges have contiguous from and to ranges
            return IdMap{rit_.offset_[dim] + current_[dim] - range.start, current_[dim]};
            // return IdMap{current_[dim], rit_.offset_[dim] + current_[dim] - range.start};
            break;
          case Range::MAP:
            // Maps refer to individual
            return rit_.DimMaps(dim)[current_[dim]];
            break;
          default:
            assert((false) && "Unhandled range.type switch case");
        }
      }

      std::size_t FlatDestination(const casacore::IPosition & shape) {
        auto product = std::size_t{1};
        auto result = std::size_t{0};
        auto ndim = rit_.nDim();
        assert(ndim > 0);

        for(auto dim = std::ptrdiff_t(ndim) - 1; dim >= 1; --dim) {
          result += product * CurrentId(dim).to;
          product *= shape[ndim - dim - 1];
        }

        return result + product * CurrentId(0).to;
      }

      std::size_t FlatSource(const casacore::IPosition & shape) {
        auto product = std::size_t{1};
        auto result = std::size_t{0};
        auto ndim = rit_.nDim();
        assert(ndim > 0);

        for(auto dim = std::ptrdiff_t(ndim) - 1; dim >= 1; --dim) {
          result += product * CurrentId(dim).from;
          product *= shape[ndim - dim - 1];
        }

        return result + product * CurrentId(0).from;
      }

      MapIterator & operator++() {
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

      bool operator==(const MapIterator & other) const {
        if(&rit_ != &other.rit_ || done_ != other.done_) return false;
        return done_ ? true : current_ == other.current_;
      }

      inline bool operator!=(const MapIterator & other) const {
        return !(*this == other);
      }
  };


  // Iterates over the Disjoint Ranges defined by a ColumnMapping
  class RangeIterator {
    public:
      const ColMap2 & map_;
      std::vector<std::size_t> index_;
      std::vector<casacore::rownr_t> offset_;
      bool done_;

    public:
      RangeIterator(ColMap2 & column_map, bool done=false) :
        map_(column_map),
        index_(column_map.nDim(), 0),
        offset_(column_map.nDim(), 0),
        done_(done) {}

      // Return the number of dimensions in the index
      inline std::size_t nDim() const {
        return index_.size();
      }

      // Return the Ranges for the given dimension
      inline const ColumnRange & DimRanges(std::size_t dim) const {
        assert(dim < nDim());
        return map_.DimRanges(dim);
      }

      // Return the Maps for the given dimension
      inline const ColumnMap & DimMaps(std::size_t dim) const {
        assert(dim < nDim());
        return map_.DimMaps(dim);
      }

      // Return the currently selected Range of the given dimension
      inline const Range & DimRange(std::size_t dim) const {
        assert(dim < nDim());
        return DimRanges(dim)[index_[dim]];
      }

      inline MapIterator MapBegin() const {
        return MapIterator(*this, false);
      }

      inline MapIterator MapEnd() const {
        return MapIterator(*this, true);
      }

      RangeIterator & operator++() {
        assert(!done_);

        // Iterate from fastest to slowest changing dimension
        for(auto dim = std::ptrdiff_t(index_.size()) - 1; dim >= 0;) {
          const auto & range = DimRange(dim);

          switch(range.type) {
            case Range::FREE:
            case Range::MAP:
              offset_[dim] += range.nRows();
              break;
            case Range::UNCONSTRAINED:
              {
                const auto & row_range = DimRange(0);
                assert(row_range.IsSingleRow());
                offset_[dim] += map_.RowDimSize(row_range.start, dim);
              }
              break;
            default:
              assert((false) && "Unhandled range.type switch case");
          }

          index_[dim]++;

          if(index_[dim] < map_.DimRanges(dim).size()) {
            break;  // We've achieved a successful iteration in this dimension
          } else if(dim > 0) {
            // We've exceeded the size of the current dimension
            // reset to zero and retry the while loop
            index_[dim] = 0;
            offset_[dim] = 0;
            --dim;
          } else {
            // This was the slowest changing dimension so we're done
            done_ = true;
            break;
          }
        }

        return *this;
      };

      // Returns a slicer for the row dimension
      casacore::Slicer GetRowSlicer() const {
        assert(!done_);
        assert(index_.size() > 0);
        assert(nDim() > 0);
        const auto & range = DimRange(0);

        ssize_t start;
        ssize_t end;

        switch(range.type) {
          case Range::FREE:
            start = static_cast<ssize_t>(range.start);
            end = static_cast<ssize_t>(range.end - 1);
            break;
          case Range::MAP:
          {
            const auto & dim_maps = DimMaps(0);
            assert(range.start < dim_maps.size());
            assert(range.end - 1 < dim_maps.size());
            start = static_cast<ssize_t>(dim_maps[range.start].from);
            end = static_cast<ssize_t>(dim_maps[range.end - 1].from);
          }
          break;
          case Range::UNCONSTRAINED:
            assert((false) && "Unconstrained Range forbidden for the row dimension");
            break;
          default:
            assert((false) && "Unhandled Range.type switch case");
            break;
        }

        return casacore::Slicer(
          casacore::IPosition({start}),
          casacore::IPosition({end}),
          casacore::Slicer::endIsLast);
      };

      // Returns a slicer for secondary dimensions
      casacore::Slicer GetSectionSlicer() const {
        assert(!done_);
        assert(index_.size() > 1);
        assert(nDim() > 1);
        auto ndim = nDim();

        casacore::IPosition start(ndim - 1, 0);
        casacore::IPosition end(ndim - 1, 0);

        for(std::size_t dim = 1; dim < ndim; ++dim) {
          const auto & range = DimRange(dim);

          // TODO: casacore rownr_t is unsigned but ssize_t is signed
          // Find a proper solution to the narrowing conversion and
          // possible resulting issues
          switch(range.type) {
            case Range::FREE:
              start[ndim - 1 - dim] = static_cast<ssize_t>(range.start);
              end[ndim - 1 - dim] = static_cast<ssize_t>(range.end - 1);
              break;
            case Range::MAP:
              {
                const auto & dim_maps = DimMaps(dim);
                start[ndim - 1 - dim] = static_cast<ssize_t>(dim_maps[range.start].from);
                end[ndim - 1 - dim] = static_cast<ssize_t>(dim_maps[range.end - 1].from);
              }
              break;
            case Range::UNCONSTRAINED:
              {
                // In case of variably shaped columns, the dimension size will vary by row
                const auto & rr = DimRange(0);
                assert(rr.IsSingleRow());
                start[ndim - 1 - dim] = 0;
                end[ndim - 1 - dim] = static_cast<ssize_t>(map_.RowDimSize(rr.start, dim)) - 1;
              }
              break;
            default:
              assert((false) && "Unhandled Range.type switch case");
          }
        }

        return casacore::Slicer(start, end, casacore::Slicer::endIsLast);
      };

      bool operator==(const RangeIterator & other) const {
        if(&map_ != &other.map_ || done_ != other.done_) return false;
        return done_ ? true : index_ == other.index_;
      };

      inline bool operator!=(const RangeIterator & other) const {
        return !(*this == other);
      }
  };

public:
  const casacore::TableColumn & column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
  ShapeProvider shape_provider_;

public:
  inline const ColumnMap & DimMaps(std::size_t dim) const { return maps_[dim]; }
  inline const ColumnRange & DimRanges(std::size_t dim) const { return ranges_[dim]; }
  inline std::size_t nDim() const { return shape_provider_.nDim(); }

  inline RangeIterator RangeBegin() const {
    return RangeIterator{const_cast<ColMap2 &>(*this), false};
  }

  inline RangeIterator RangeEnd() const {
    return RangeIterator{const_cast<ColMap2 &>(*this), true};
  }

  inline std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    return shape_provider_.RowDimSize(row, dim);
  }

  // Create a Column Map from a selection of row id's in different dimensions
  static ColumnMaps MakeMaps(const ColumnSelection & selection) {
    // Empty selection
    if(selection.size() == 0) {
      return ColumnMaps{ColumnMap{}};
    }

    ColumnMaps column_maps;
    column_maps.reserve(selection.size());

    for(std::size_t dim=0; dim < selection.size(); ++dim) {
        const auto & dim_ids = selection[dim];
        ColumnMap column_map;
        column_map.reserve(dim_ids.size());

        for(auto [it, to] = std::tuple{std::begin(dim_ids), casacore::rownr_t{0}};
            it != std::end(dim_ids); ++to, ++it) {
              column_map.push_back({*it, to});
        }

        std::sort(std::begin(column_map), std::end(column_map),
                 [](const auto & lhs, const auto & rhs) {
                    return lhs.from < rhs.from; });

        column_maps.emplace_back(std::move(column_map));
    }

    return column_maps;
  }

  // Make ranges for fixed shape columns
  // In this case, each row has the same shape
  // so we can make ranges that span multiple rows
  static arrow::Result<ColumnRanges>
  MakeFixedRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
    assert(shape_prov.IsActuallyFixed());
    auto ndim = shape_prov.nDim();
    ColumnRanges column_ranges;
    column_ranges.reserve(ndim);

    for(std::size_t dim=0; dim < ndim; ++dim) {
      // If no mapping exists for this dimension, create a range
      // from the column shape
      if(dim >= maps.size() || maps[dim].size() == 0) {
        ARROW_ASSIGN_OR_RAISE(auto dim_size, shape_prov.DimSize(dim));
        column_ranges.emplace_back(ColumnRange{Range{0, dim_size, Range::FREE}});
        continue;
      }

      // A mapping exists for this dimension, create ranges
      // from contiguous segments
      const auto & column_map = maps[dim];
      auto column_range = ColumnRange{};
      auto current = Range{0, 1, Range::MAP};

      for(auto [i, prev, next] = std::tuple{
              casacore::rownr_t{1},
              std::begin(column_map),
              std::next(std::begin(column_map))};
          next != std::end(column_map); ++i, ++prev, ++next) {

        if(next->from - prev->from == 1) {
          current.end += 1;
        } else {
          column_range.push_back(current);
          current = Range{i, i + 1, Range::MAP};
        }
      }

      column_range.emplace_back(std::move(current));
      column_ranges.emplace_back(std::move(column_range));
    }

    assert(ndim == column_ranges.size());
    return column_ranges;
  }

  // Make ranges for variably shaped columns
  // In this case, each row may have a different shape
  // so we create a separate range for each row and unconstrained
  // ranges for other dimensions whose size cannot be determined.
  static arrow::Result<ColumnRanges>
  MakeVariableRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
    assert(!shape_prov.IsActuallyFixed());
    auto ndim = shape_prov.nDim();
    ColumnRanges column_ranges;
    column_ranges.reserve(ndim);
    auto row_range = ColumnRange{};

    // Split the row dimension into ranges of exactly one row
    if(maps.size() == 0 || maps[0].size() == 0) {
      // No maps provided, derive from shape
      ARROW_ASSIGN_OR_RAISE(auto dim_size, shape_prov.DimSize(0));
      row_range.reserve(dim_size);
      for(std::size_t r=0; r < dim_size; ++r) {
        row_range.emplace_back(Range{r, r + 1, Range::FREE});
      }
    } else {
      // Derive from mapping
      const auto & row_maps = maps[0];
      row_range.reserve(row_maps.size());
      for(std::size_t r=0; r < row_maps.size(); ++r) {
        row_range.emplace_back(Range{r, r + 1, Range::MAP});
      }
    }

    column_ranges.emplace_back(std::move(row_range));

    // Now handle the non-row dimensions
    for(std::size_t dim=1; dim < ndim; ++dim) {
      // If no mapping exists for this dimension
      // create a single unconstrained range
      if(dim >= maps.size() || maps[dim].size() == 0) {
        column_ranges.emplace_back(ColumnRange{Range{0, 0, Range::UNCONSTRAINED}});
        continue;
      }

      // A mapping exists for this dimension, create ranges
      // from contiguous segments
      const auto & column_map = maps[dim];
      auto column_range = ColumnRange{};
      auto current = Range{0, 1, Range::MAP};

      for(auto [i, prev, next] = std::tuple{
              casacore::rownr_t{1},
              std::begin(column_map),
              std::next(std::begin(column_map))};
          next != std::end(column_map); ++i, ++prev, ++next) {

        if(next->from - prev->from == 1) {
          current.end += 1;
        } else {
          column_range.push_back(current);
          current = Range{i, i + 1, Range::MAP};
        }
      }

      column_range.emplace_back(std::move(current));
      column_ranges.emplace_back(std::move(column_range));
    }

    assert(ndim == column_ranges.size());
    return column_ranges;
  }

  // Make ranges for each dimension
  static arrow::Result<ColumnRanges>
  MakeRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps) {
    if(shape_prov.IsActuallyFixed()) {
      return MakeFixedRanges(shape_prov, maps);
    }

    return MakeVariableRanges(shape_prov, maps);
  }

  // Factory method for making a ColumnMapping object
  static arrow::Result<ColMap2> Make(
      const casacore::TableColumn & column,
      const ColumnSelection & selection) {

    ARROW_ASSIGN_OR_RAISE(auto shape_prov, ShapeProvider::Make(column, selection));
    auto maps = MakeMaps(selection);
    ARROW_ASSIGN_OR_RAISE(auto ranges, MakeRanges(shape_prov, maps));

    if(ranges.size() == 0) {
      return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                           column.columnDesc().name());
    }

    return ColMap2{column, std::move(maps), std::move(ranges), std::move(shape_prov)};
  }

  // Number of disjoint ranges in this map
  std::size_t nRanges() const {
    return std::accumulate(std::begin(ranges_), std::end(ranges_), std::size_t{1},
                          [](const auto init, const auto & range)
                            { return init * range.size(); });
  }

  // Find the total number of elements formed
  // by the disjoint ranges in this map
  std::size_t nElements() const {
    assert(ranges_.size() > 0);
    const auto & row_ranges = ranges_[0];
    auto elements = std::size_t{0};
    auto row_range_id = std::size_t{0};

    for(auto rr_id = std::size_t{0}; rr_id < row_ranges.size(); ++rr_id) {
      const auto & row_range = row_ranges[rr_id];
      auto row_elements = std::size_t{row_range.nRows()};
      for(std::size_t dim = 1; dim < ranges_.size(); ++dim) {
        const auto & dim_range = ranges_[dim];
        auto dim_elements = std::size_t{0};
        for(const auto & range: dim_range) {
          if(range.IsUnconstrained()) {
            assert(row_range.IsSingleRow());
            dim_elements += shape_provider_.RowDimSize(rr_id, dim);
          } else {
            assert(range.IsValid());
            dim_elements += range.nRows();
          }
        }
        row_elements *= dim_elements;
      }
      elements += row_elements;
    }

    return elements;
  }
};




} // namespace arcae

#endif // ARCAE_COLUMN_MAPPER_2