#ifndef ARCAE_COLUMN_MAPPER_H
#define ARCAE_COLUMN_MAPPER_H

#include <cstddef>
#include <functional>
#include <memory>
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
  Make(const casacore::TableColumn & column, const ColumnSelection & selection);
  // Returns true if the data shapes are fixed in practice
  bool IsActuallyFixed() const;
  // Number of dimensions, excluding row
  std::size_t nDim() const;

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
  std::reference_wrapper<const ColumnSelection> selection_;
  std::unique_ptr<VariableShapeData> var_data_;

  static arrow::Result<ShapeProvider> Make(const casacore::TableColumn & column,
                                           const ColumnSelection & selection);

  // Returns true if the column is defined as having a fixed shape
  inline bool IsDefinitelyFixed() const {
    return var_data_ == nullptr;
  }

  // Return true if the column is defined as having a varying shape
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

  inline std::size_t RowDim() const { return nDim() - 1; }

  // Returns the dimension size of this column
  arrow::Result<std::size_t> DimSize(std::size_t dim) const;

  // Returns the dimension size of the colum for the given row
  std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const;
};



class RangeIterator;
class ColumnMapping;

// Iterates over the current mapping in the RangeIterator
class MapIterator {
  public:
    // Reference to RangeIterator
    std::reference_wrapper<const RangeIterator> rit_;
    // Reference to ColumnMapping
    std::reference_wrapper<const ColumnMapping> map_;
    // ND index in the local buffer holding the values
    // described by this chunk
    std::vector<std::size_t> chunk_index_;
    // ND index in the global buffer
    std::vector<std::size_t> global_index_;
    std::vector<std::size_t> strides_;
    bool done_;

    MapIterator(const RangeIterator & rit,
                const ColumnMapping & map,
                std::vector<std::size_t> chunk_index,
                std::vector<std::size_t> global_index,
                std::vector<std::size_t> strides,
                bool done);
  public:
    static MapIterator Make(const RangeIterator & rit, bool done);
    inline std::size_t nDim() const {
      return chunk_index_.size();
    };

    inline std::size_t RowDim() const {
      return nDim() - 1;
    };

    std::size_t ChunkOffset() const;
    std::size_t GlobalOffset() const;
    std::size_t RangeSize(std::size_t dim) const;
    std::size_t MemStart(std::size_t dim) const;

    MapIterator & operator++();
    bool operator==(const MapIterator & other) const;
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
    // Start position of the memory index
    std::vector<std::size_t> mem_start_;
    // Length of the range
    std::vector<std::size_t> range_length_;
    bool done_;

  public:
    RangeIterator(ColumnMapping & column_map, bool done=false);

    // Return the number of dimensions in the index
    inline std::size_t nDim() const {
      return index_.size();
    }

    // Index of he row dimension
    inline std::size_t RowDim() const {
      return nDim() - 1;
    }

    // Return the Ranges for the given dimension
    const ColumnRange & DimRanges(std::size_t dim) const;

    // Return the Maps for the given dimension
    inline const ColumnMap & DimMaps(std::size_t dim) const;

    // Return the currently selected Range of the given dimension
    inline const Range & DimRange(std::size_t dim) const;

    inline MapIterator MapBegin() const {
      return MapIterator::Make(*this, false);
    };

    MapIterator MapEnd() const {
      return MapIterator::Make(*this, true);
    };

    inline std::size_t RangeElements() const;

    RangeIterator & operator++();
    void UpdateState();

    // Returns a slicer for the row dimension
    casacore::Slicer GetRowSlicer() const;
    // Returns a slicer for secondary dimensions
    casacore::Slicer GetSectionSlicer() const;

    bool operator==(const RangeIterator & other) const;
    bool operator!=(const RangeIterator & other) const;

};

class ColumnMapping {
public:
  enum InputOrder {C_ORDER=0, F_ORDER};

public:
  std::reference_wrapper<const casacore::TableColumn> column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;
  ShapeProvider shape_provider_;
  std::optional<casacore::IPosition> output_shape_;

public:
  inline const ColumnMap & DimMaps(std::size_t dim) const {
    return maps_[dim];
  }

  inline const ColumnRange & DimRanges(std::size_t dim) const {
    return ranges_[dim];
  }

  inline std::size_t nDim() const {
    return shape_provider_.nDim();
  }

  inline std::size_t RowDim() const {
    return nDim() - 1;
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

  // Flattened offset in the output buffer
  std::size_t FlatOffset(const std::vector<std::size_t> & index) const;

  // Get the output shape, returns Status::Invalid if undefined
  arrow::Result<casacore::IPosition> GetOutputShape() const {
    if(output_shape_) return output_shape_.value();
    return arrow::Status::Invalid("Column ", column_.get().columnDesc().name(),
                                  " does not have a fixed shape");
  }

  inline bool IsFixedShape() const {
    return shape_provider_.IsActuallyFixed();
  }

  // Create a Column Map from a selection of row id's in different dimensions
  static ColumnMaps MakeMaps(const ShapeProvider & shape_prov, const ColumnSelection & selection);

  // Make ranges for fixed shape columns
  // In this case, each row has the same shape
  // so we can make ranges that span multiple rows
  static arrow::Result<ColumnRanges>
  MakeFixedRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps);
  // Make ranges for variably shaped columns
  // In this case, each row may have a different shape
  // so we create a separate range for each row and unconstrained
  // ranges for other dimensions whose size cannot be determined.
  static arrow::Result<ColumnRanges>
  MakeVariableRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps);
  // Make ranges for each dimension
  static arrow::Result<ColumnRanges>
  MakeRanges(const ShapeProvider & shape_prov, const ColumnMaps & maps);

  // Derive an output shape from the selection ranges
  // This may not be possible for variably shaped columns
  static std::optional<casacore::IPosition> MaybeMakeOutputShape(const ColumnRanges & ranges);

  // Factory method for making a ColumnMapping object
  static arrow::Result<ColumnMapping> Make(
      const casacore::TableColumn & column,
      ColumnSelection selection,
      InputOrder order=InputOrder::C_ORDER);

  // Number of disjoint ranges in this map
  std::size_t nRanges() const;

  // Returns true if this is a simple map or, a map that only contains
  // a single range and thereby removes the need to read separate ranges of
  // data and copy those into a final buffer.
  bool IsSimple() const;

  // Find the total number of elements formed
  // by the disjoint ranges in this map
  std::size_t nElements() const;
};


} // namespace arcae

#endif // ARCAE_COLUMN_MAPPER_H