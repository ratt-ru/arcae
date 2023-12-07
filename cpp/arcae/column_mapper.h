#ifndef ARCAE_COLUMN_MAPPER_H
#define ARCAE_COLUMN_MAPPER_H

#include <cassert>
#include <cstddef>
#include <iterator>
#include <functional>
#include <memory>
#include <numeric>
#include <optional>
#include <sys/types.h>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

enum InputOrder {C=0, F};

using RowIds = std::vector<casacore::rownr_t>;
using ColumnSelection = std::vector<RowIds>;

// Describes a mapping between disk and memory
struct IdMap {
  casacore::rownr_t disk;
  casacore::rownr_t mem;

  constexpr inline bool operator==(const IdMap & lhs) const
      { return disk == lhs.disk && mem == lhs.mem; }
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
    // Row dimension is last in FORTRAN ordering
    auto row_dim = selection.size() - 1;
    using ItType = std::tuple<std::size_t, bool>;

    // No selection
    // Create row shape data from column.nrow()
    if(selection.size() == 0 || selection[row_dim].size() == 0) {
      row_shapes.reserve(column.nrow());

      for(auto [r, first] = ItType{0, true}; r < column.nrow(); ++r, first=false) {
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
      const auto & row_ids = selection[row_dim];
      row_shapes.reserve(row_ids.size());

      for(auto [r, first] = ItType{0, true}; r < row_ids.size(); ++r, first=false) {
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

    // Create offset arrays
    auto nrow = row_shapes.size();
    // Number of dimensions without row
    auto ndim = std::begin(row_shapes)->size();
    auto offsets = decltype(VariableShapeData::offsets_)(ndim, std::vector<std::size_t>(nrow, 0));

    for(auto r=0; r < nrow; ++r) {
      using ItType = std::tuple<std::size_t, std::size_t>;
      for(auto [dim, product]=ItType{0, 1}; dim < ndim; ++dim) {
        product = offsets[dim][r] = product * row_shapes[r][dim];
      }
    }


    // We may have a fixed shape in practice
    auto shape = fixed_shape ? std::make_optional(*std::begin(row_shapes))
                             : std::nullopt;


    return std::unique_ptr<VariableShapeData>(
      new VariableShapeData{std::move(row_shapes),
                            std::move(offsets),
                            ndim,
                            std::move(shape)});
  }

  // Returns true if the data shapes are fixed in practice
  inline bool IsActuallyFixed() const { return shape_.has_value(); }
  // Number of dimensions, excluding row
  inline std::size_t nDim() const { return ndim_; }

  std::vector<casacore::IPosition> row_shapes_;
  std::vector<std::vector<std::size_t>> offsets_;
  std::size_t ndim_;
  std::optional<casacore::IPosition> shape_;
};


// Provides Shape information for this column
// This easy in the case of Fixed Shape columns.
// This may not be possible in the Variable column case.
struct ShapeProvider {
public:
  std::reference_wrapper<const casacore::TableColumn> column_;
  const ColumnSelection & selection_;
  std::unique_ptr<VariableShapeData> var_data_;

  static arrow::Result<ShapeProvider> Make(const casacore::TableColumn & column,
                                           const ColumnSelection & selection) {

    if(column.columnDesc().isFixedShape()) {
      return ShapeProvider{std::cref(column), selection, nullptr};
    }

    ARROW_ASSIGN_OR_RAISE(auto var_data, VariableShapeData::Make(column, selection));
    return ShapeProvider{std::cref(column), selection, std::move(var_data)};
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
    return (IsDefinitelyFixed() ? column_.get().columnDesc().ndim() : var_data_->nDim()) + 1;
  }

  std::optional<casacore::IPosition> MaybeGetShape() const {
    if(IsDefinitelyFixed()) {
      return column_.get().shapeColumn();
    }

    return var_data_->shape_;
  }

  inline std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const {
    // Dimension needs to be adjusted for
    // 1. We may not have selections matching all dimensions
    // 2. Selections are FORTRAN ordered
    auto sdim = std::ptrdiff_t(dim + selection_.size()) - std::ptrdiff_t(nDim());
    // If we have a selection of row id's,
    // derive the dimension size from these
    if(sdim > 0 && selection_.size() > 0 && selection_[sdim].size() > 0) {
      return selection_[sdim].size();
    }

    assert(dim < nDim());

    // There's no selection for this dimension
    // so we must derive the dimension size
    // from the column shape information
    if(dim == RowDim()) {
      // First dimension is just row
      return column_.get().nrow();
    } else if(IsDefinitelyFixed()) {
      // Fixed shape column, we have the size information
      return column_.get().shapeColumn()[dim];
    } else {
      const auto & shape = var_data_->shape_;

      if(!shape) {
        return arrow::Status::IndexError("Dimension ", dim, " in  column ",
                                         column_.get().columnDesc().name(),
                                         " is not fixed.");
      }

      // Even though the column is marked as variable
      // the individual row shapes are the same
      return shape.value()[dim];
    }
  }

  std::size_t FlatOffset(const std::vector<std::size_t> & index) const {
    if(auto opt_shape = MaybeGetShape(); opt_shape) {
      // Fixed shape output, easy case
      const auto & shape = opt_shape.value();
      auto result = std::size_t{0};
      auto product = std::size_t{1};

      for(auto dim = 0; dim < RowDim(); ++dim) {
        result += index[dim] * product;
        product *= shape[dim];
      }

      return result + product * index[RowDim()];
    } else {
      // Variably shaped output, per-row offsets are needed
      // There is no offset array for the fast changing dimension
      auto result = index[0];
      auto row = index[RowDim()];
      const auto & offsets = var_data_->offsets_;

      for(auto dim = 1; dim < RowDim(); ++dim) {
        result += index[dim] * offsets[dim - 1][row];
      }

      const auto & row_offsets = offsets[offsets.size() - 1];
      return std::accumulate(std::begin(row_offsets),
                             std::begin(row_offsets) + row,
                             result);
    }
  }

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    assert(IsVarying());
    assert(row < var_data_->row_shapes_.size());
    assert(dim < RowDim());
    return var_data_->row_shapes_[row][dim];
  }
};

class ColumnMapping {
public:
  class RangeIterator;

  // Iterates over the current mapping in the RangeIterator
  class MapIterator {
    public:
      std::reference_wrapper<const RangeIterator> rit_;
      std::vector<std::size_t> current_;
      std::vector<std::size_t> strides_;
      bool done_;

      MapIterator(const RangeIterator & rit,
                  std::vector<std::size_t> && current,
                  std::vector<std::size_t> && strides,
                  bool done) :
        rit_(std::cref(rit)),
        current_(std::move(current)),
        strides_(std::move(strides)),
        done_(done) {}
    public:

      static MapIterator Make(const RangeIterator & rit, bool done) {
        auto current = decltype(MapIterator::current_)(rit.nDim(), 0);
        auto strides = decltype(MapIterator::strides_)(rit.nDim(), 1);
        using ItType = std::tuple<std::size_t, std::size_t>;

        for(auto [dim, product]=ItType{1, 1}; dim < rit.nDim(); ++dim) {
          auto diff = rit.range_length_[dim] - rit.disk_start_[dim];
          product = strides[dim] = product * diff;
        }

        return MapIterator{rit, std::move(current), std::move(strides), done};
      }

      inline std::size_t nDim() const {
        return current_.size();
      }

      inline std::size_t RowDim() const {
        return nDim() - 1;
      }

      std::size_t FromBufferOffset() const {
        std::size_t offset = 0;
        for(auto dim = std::size_t{0}; dim < nDim(); ++dim) {
          offset += current_[dim] * strides_[dim];
        }
        return offset;
      }

      std::size_t ToBufferOffset() const {
        return rit_.get().base_out_offset_ + FromBufferOffset();
      }

      inline std::size_t RangeSize(std::size_t dim) const {
        return rit_.get().range_length_[dim];
      }

      MapIterator & operator++() {
        assert(!done_);

        // Iterate from fastest to slowest changing dimension
        for(auto dim = std::size_t{0}; dim < nDim();) {
          current_[dim]++;
          // We've achieved a successful iteration in this dimension
          if(current_[dim] < RangeSize(dim)) { break; }
          // Reset to zero and retry in the next dimension
          else if(dim < RowDim()) { current_[dim] = 0; ++dim; }
          // This was the slowest changing dimension so we're done
          else { done_ = true; break; }
        }

        return *this;
      }

      bool operator==(const MapIterator & other) const {
        if(&rit_.get() != &other.rit_.get() || done_ != other.done_) return false;
        return done_ ? true : current_ == other.current_;
      }

      inline bool operator!=(const MapIterator & other) const {
        return !(*this == other);
      }
  };


  // Iterates over the Disjoint Ranges defined by a ColumnMapping
  class RangeIterator {
    public:
      std::reference_wrapper<const ColumnMapping> map_;
      // Index of the Disjoint Range
      std::vector<std::size_t> index_;
      // Starting position of the disk index
      std::vector<std::size_t> disk_start_;
      // Length of the range
      std::vector<std::size_t> range_length_;
      // Base offset into the output buffer for the current range
      std::size_t base_out_offset_;
      bool done_;

    public:
      RangeIterator(ColumnMapping & column_map, bool done=false) :
        map_(std::cref(column_map)),
        index_(column_map.nDim(), 0),
        disk_start_(column_map.nDim(), 0),
        range_length_(column_map.nDim(), 0),
        base_out_offset_(0),
        done_(done) {
          UpdateState();
      }

      // Return the number of dimensions in the index
      inline std::size_t nDim() const {
        return index_.size();
      }

      inline std::size_t RowDim() const {
        return nDim() - 1;
      }

      // Return the Ranges for the given dimension
      inline const ColumnRange & DimRanges(std::size_t dim) const {
        assert(dim < nDim());
        return map_.get().DimRanges(dim);
      }

      // Return the Maps for the given dimension
      inline const ColumnMap & DimMaps(std::size_t dim) const {
        assert(dim < nDim());
        return map_.get().DimMaps(dim);
      }

      // Return the currently selected Range of the given dimension
      inline const Range & DimRange(std::size_t dim) const {
        assert(dim < nDim());
        return DimRanges(dim)[index_[dim]];
      }

      inline MapIterator MapBegin() const {
        return MapIterator::Make(*this, false);
      }

      inline MapIterator MapEnd() const {
        return MapIterator::Make(*this, true);
      }

      inline std::size_t RangeElements() const {
        return std::accumulate(std::begin(index_), std::end(index_), std::size_t{1},
                               [](const auto i, auto v) { return i*v; });
      }

      RangeIterator & operator++() {
        assert(!done_);
        // Iterate from fastest to slowest changing dimension: FORTRAN order
        for(auto dim = 0; dim < nDim();) {
          index_[dim]++;

          // We've achieved a successful iteration in this dimension
          if(index_[dim] < DimRanges(dim).size()) { break; }
          // We've exceeded the size of the current dimension
          // reset to zero and retry the while loop
          else if(dim < RowDim()) { index_[dim] = 0; ++dim; }
          // Row is the slowest changing dimension so we're done
          // return without updating the iterator state
          else { done_ = true; return *this; }
        }

        // Increment output memory buffer offset
        UpdateState();
        return *this;
      };

      void UpdateState() {
        for(auto dim=std::size_t{0}; dim < nDim(); ++dim) {
          const auto & range = DimRange(dim);
          switch(range.type) {
            case Range::FREE: {
              disk_start_[dim] = range.start;
              range_length_[dim] = range.end - range.start;
              break;
            }
            case Range::MAP: {
              const auto & dim_maps = DimMaps(dim);
              assert(range.start < dim_maps.size());
              assert(range.end - 1 < dim_maps.size());
              auto start = disk_start_[dim] = dim_maps[range.start].disk;
              range_length_[dim] = dim_maps[range.end - 1].disk - start + 1;
              break;
            }
            case Range::UNCONSTRAINED: {
              // In case of variably shaped columns,
              // the dimension size will vary by row
              // and there will only be a single row
              const auto & rr = DimRange(RowDim());
              assert(rr.IsSingleRow());
              disk_start_[dim] = 0;
              range_length_[dim] = map_.get().RowDimSize(rr.start, dim);
              break;
            }
            default:
              assert(false && "Unhandled Range::Type enum");
          }
        }

        base_out_offset_ = map_.get().FlatOffset(disk_start_);
      }

      // Returns a slicer for the row dimension
      casacore::Slicer GetRowSlicer() const {
        assert(!done_);
        assert(nDim() > 0);
        auto start = static_cast<ssize_t>(disk_start_[RowDim()]);
        auto length = static_cast<ssize_t>(range_length_[RowDim()]);

        return casacore::Slicer(
          casacore::IPosition({start}),
          casacore::IPosition({start + length - 1}),
          casacore::Slicer::endIsLast);
      };

      // Returns a slicer for secondary dimensions
      casacore::Slicer GetSectionSlicer() const {
        assert(!done_);
        assert(nDim() > 1);
        casacore::IPosition start(RowDim(), 0);
        casacore::IPosition length(RowDim(), 0);

        for(auto dim=std::size_t{0}; dim < RowDim(); ++dim) {
          start[dim] = static_cast<ssize_t>(disk_start_[dim]);
          length[dim] = start[dim] + static_cast<ssize_t>(range_length_[dim]) - 1;
        }

        return casacore::Slicer(start, length, casacore::Slicer::endIsLast);
      };

      bool operator==(const RangeIterator & other) const {
        if(&map_.get() != &other.map_.get() || done_ != other.done_) return false;
        return done_ ? true : index_ == other.index_;
      };

      inline bool operator!=(const RangeIterator & other) const {
        return !(*this == other);
      }
  };

public:
  std::reference_wrapper<const casacore::TableColumn> column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
  ShapeProvider shape_provider_;

public:
  inline const ColumnMap & DimMaps(std::size_t dim) const { return maps_[dim]; }
  inline const ColumnRange & DimRanges(std::size_t dim) const { return ranges_[dim]; }
  inline std::size_t nDim() const { return shape_provider_.nDim(); }
  inline std::size_t RowDim() const { return nDim() - 1; }
  inline std::size_t FlatOffset(const std::vector<std::size_t> & index) const {
    return shape_provider_.FlatOffset(index);
  }

  inline RangeIterator RangeBegin() const {
    return RangeIterator{const_cast<ColumnMapping &>(*this), false};
  }

  inline RangeIterator RangeEnd() const {
    return RangeIterator{const_cast<ColumnMapping &>(*this), true};
  }

  inline std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    return shape_provider_.RowDimSize(row, dim);
  }

  inline bool IsFixedShape() const {
    return shape_provider_.IsActuallyFixed();
  }

  // Create a Column Map from a selection of row id's in different dimensions
  static ColumnMaps MakeMaps(const ShapeProvider & shape_prov, const ColumnSelection & selection) {
    ColumnMaps column_maps;
    auto ndim = shape_prov.nDim();
    column_maps.reserve(ndim);

    for(auto dim=std::size_t{0}; dim < ndim; ++dim) {
        // Dimension needs to be adjusted for
        // 1. We may not have selections matching all dimensions
        // 2. Selections are FORTRAN ordered
        auto sdim = std::ptrdiff_t(dim + selection.size()) - std::ptrdiff_t(ndim);

        if(sdim < 0 || selection.size() == 0 || selection[sdim].size() == 0) {
          column_maps.emplace_back(ColumnMap{});
          continue;
        }

        const auto & dim_ids = selection[sdim];
        ColumnMap column_map;
        column_map.reserve(dim_ids.size());

        for(auto [disk_it, mem] = std::tuple{std::begin(dim_ids), casacore::rownr_t{0}};
            disk_it != std::end(dim_ids); ++mem, ++disk_it) {
              column_map.push_back({*disk_it, mem});
        }

        std::sort(std::begin(column_map), std::end(column_map),
                 [](const auto & lhs, const auto & rhs) {
                    return lhs.disk < rhs.disk; });

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

        if(next->disk - prev->disk == 1) {
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
    auto row_dim = ndim - 1;
    ColumnRanges column_ranges;
    column_ranges.reserve(ndim);


    // Handle non-row dimensions first
    for(std::size_t dim=0; dim < row_dim; ++dim) {
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

        if(next->disk - prev->disk == 1) {
          current.end += 1;
        } else {
          column_range.push_back(current);
          current = Range{i, i + 1, Range::MAP};
        }
      }

      column_range.emplace_back(std::move(current));
      column_ranges.emplace_back(std::move(column_range));
    }

    // Lastly, the row dimension
    auto row_range = ColumnRange{};

    // Split the row dimension into ranges of exactly one row
    if(maps.size() == 0 || maps[row_dim].size() == 0) {
      // No maps provided, derive from shape
      ARROW_ASSIGN_OR_RAISE(auto dim_size, shape_prov.DimSize(row_dim));
      row_range.reserve(dim_size);
      for(std::size_t r=0; r < dim_size; ++r) {
        row_range.emplace_back(Range{r, r + 1, Range::FREE});
      }
    } else {
      // Derive from mapping
      const auto & row_maps = maps[row_dim];
      row_range.reserve(row_maps.size());
      for(std::size_t r=0; r < row_maps.size(); ++r) {
        row_range.emplace_back(Range{r, r + 1, Range::MAP});
      }
    }

    column_ranges.emplace_back(std::move(row_range));


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
  static arrow::Result<ColumnMapping> Make(
      const casacore::TableColumn & column,
      ColumnSelection selection,
      InputOrder order=InputOrder::C) {

    // Convert to FORTRAN ordering, which the casacore internals use
    if(order == InputOrder::C) {
      std::reverse(std::begin(selection), std::end(selection));
    }

    ARROW_ASSIGN_OR_RAISE(auto shape_prov, ShapeProvider::Make(column, selection));
    auto maps = MakeMaps(shape_prov, selection);
    ARROW_ASSIGN_OR_RAISE(auto ranges, MakeRanges(shape_prov, maps));

    if(ranges.size() == 0) {
      return arrow::Status::ExecutionError("Zero ranges generated for column ",
                                           column.columnDesc().name());
    }

    return ColumnMapping{column, std::move(maps), std::move(ranges), std::move(shape_prov)};
  }

  // Number of disjoint ranges in this map
  std::size_t nRanges() const {
    return std::accumulate(std::begin(ranges_), std::end(ranges_), std::size_t{1},
                          [](const auto init, const auto & range)
                            { return init * range.size(); });
  }

  // Returns true if this is a simple map or, a map that only contains
  // a single range and thereby removes the need to read separate ranges of
  // data and copy those into a final buffer.
  bool IsSimple() const {
    for(std::size_t dim=0; dim < nDim(); ++dim) {
      const auto & column_map = DimMaps(dim);
      const auto & column_range = DimRanges(dim);

      // More than one range of row ids in a dimension
      if(column_range.size() > 1) {
        return false;
      }

      for(auto & range: column_range) {
        switch(range.type) {
          // These are trivially contiguous
          case Range::FREE:
          case Range::UNCONSTRAINED:
            break;
          case Range::MAP:
            for(std::size_t i = range.start + 1; i < range.end; ++i) {
              if(column_map[i].mem - column_map[i-1].mem != 1) {
                return false;
              }
              if(column_map[i].disk - column_map[i-1].disk != 1) {
                return false;
              }
            }
            break;
        }
      }
    }

    return true;
  }

  // Get the output shape, if the data selection is fixed
  arrow::Result<casacore::IPosition> GetOutputShape() const {
    assert(ranges_.size() > 0);
    const auto & row_ranges = DimRanges(RowDim());
    auto shape = casacore::IPosition(nDim(), 0);

    if(!IsFixedShape()) {
      return arrow::Status::Invalid("Column ", column_.get().columnDesc().name(),
                                    "does not have a fixed shape");
    }

    for(auto [dim, size]=std::tuple{std::size_t{0}, std::size_t{0}}; dim < nDim(); ++dim) {
      for(const auto & range: DimRanges(dim)) {
        switch(range.type) {
          case Range::FREE:
          case Range::MAP:
            assert(range.IsValid());
            size += range.nRows();
            break;
          case Range::UNCONSTRAINED:
            assert(false && "Unconstrained range in fixed shape");
          default:
            assert(false && "Unhandled Range::Type enum");
        }
      }
      shape[dim] = size;
    }

    return shape;
  }


  // Find the total number of elements formed
  // by the disjoint ranges in this map
  std::size_t nElements() const {
    assert(ranges_.size() > 0);
    const auto & row_ranges = DimRanges(RowDim());
    auto elements = std::size_t{0};

    for(std::size_t rr_id=0; rr_id < row_ranges.size(); ++rr_id) {
      const auto & row_range = row_ranges[rr_id];
      auto row_elements = std::size_t{row_range.nRows()};
      for(std::size_t dim = 0; dim < RowDim(); ++dim) {
        const auto & dim_range = DimRanges(dim);
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

#endif // ARCAE_COLUMN_MAPPER_H