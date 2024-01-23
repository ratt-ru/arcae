#ifndef ARCAE_MAP_ITERATOR_H
#define ARCAE_MAP_ITERATOR_H

#include <cassert>
#include <functional>
#include <numeric>
#include <vector>

#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>
#include <casacore/tables/Tables/TableColumn.h>

namespace arcae {

enum MapOrder {C_ORDER=0, F_ORDER};

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
    // Specifies a range whose size varies
    VARYING
  } type = FREE;

  constexpr casacore::rownr_t nRows() const
    { return end - start; }

  constexpr inline bool IsSingleRow() const
    { return nRows() == 1; }

  constexpr inline bool IsValid() const
    { return start <= end; }

  constexpr inline bool operator==(const Range & lhs) const
      { return start == lhs.start && end == lhs.end && type == lhs.type; }
};

// Vectors of ranges
using ColumnRange = std::vector<Range>;
using ColumnRanges = std::vector<ColumnRange>;


template <typename ColumnMapping> struct RangeIterator;


// Iterates over the current mapping in the RangeIterator
template <typename ColumnMapping>
struct MapIterator {
  // Reference to RangeIterator
  std::reference_wrapper<const RangeIterator<ColumnMapping>> rit_;
  // Reference to ColumnMapping
  std::reference_wrapper<const ColumnMapping> map_;
  // ND index in the local buffer holding the values
  // described by this chunk
  std::vector<std::size_t> chunk_index_;
  // ND index in the global buffer
  std::vector<std::size_t> global_index_;
  std::vector<std::size_t> strides_;
  bool done_;

  MapIterator(const RangeIterator<ColumnMapping> & rit,
              const ColumnMapping & map,
              std::vector<std::size_t> chunk_index,
              std::vector<std::size_t> global_index,
              std::vector<std::size_t> strides,
              bool done);

  static MapIterator Make(const RangeIterator<ColumnMapping> & rit, bool done);
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
template <typename ColumnMapping>
struct RangeIterator {
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

  RangeIterator(ColumnMapping & column_map, bool done=false);

  // Return the number of dimensions in the index
  inline std::size_t nDim() const {
    return index_.size();
  }

  // Index of he row dimension
  inline std::size_t RowDim() const {
    assert(nDim() > 0);
    return nDim() - 1;
  }

  // Return the Ranges for the given dimension
  const ColumnRange & DimRanges(std::size_t dim) const;

  // Return the Maps for the given dimension
  inline const ColumnMap & DimMaps(std::size_t dim) const;

  // Return the currently selected Range of the given dimension
  inline const Range & DimRange(std::size_t dim) const;

  inline MapIterator<ColumnMapping> MapBegin() const {
    return MapIterator<ColumnMapping>::Make(*this, false);
  };

  MapIterator<ColumnMapping> MapEnd() const {
    return MapIterator<ColumnMapping>::Make(*this, true);
  };

  inline std::size_t RangeElements() const;

  RangeIterator & operator++();
  void UpdateState();

  // Returns a slicer for the row dimension
  casacore::Slicer GetRowSlicer() const;
  // Returns a slicer for secondary dimensions
  casacore::Slicer GetSectionSlicer() const;
  // Returns shape of this chunk
  casacore::IPosition GetShape() const;

  bool operator==(const RangeIterator & other) const;
  bool operator!=(const RangeIterator & other) const;
};


template <typename ColumnMapping>
MapIterator<ColumnMapping>::MapIterator(const RangeIterator<ColumnMapping> & rit,
                  const ColumnMapping & map,
                  std::vector<std::size_t> chunk_index,
                  std::vector<std::size_t> global_index,
                  std::vector<std::size_t> strides,
                  bool done) :
        rit_(std::cref(rit)),
        map_(std::cref(map)),
        chunk_index_(std::move(chunk_index)),
        global_index_(std::move(global_index)),
        strides_(std::move(strides)),
        done_(done) {}

template <typename ColumnMapping>
MapIterator<ColumnMapping>
MapIterator<ColumnMapping>::Make(const RangeIterator<ColumnMapping> & rit, bool done) {
  auto chunk_index = decltype(MapIterator::chunk_index_)(rit.nDim(), 0);
  auto global_index = decltype(MapIterator::global_index_)(rit.mem_start_);
  auto strides = decltype(MapIterator::strides_)(rit.nDim(), 1);
  using ItType = std::tuple<std::size_t, std::size_t>;

  for(auto [dim, product]=ItType{1, 1}; dim < rit.nDim(); ++dim) {
    product = strides[dim] = product * rit.range_length_[dim - 1];
  }

  return MapIterator{std::cref(rit), std::cref(rit.map_.get()),
                     std::move(chunk_index), std::move(global_index),
                     std::move(strides), done};
}

template <typename ColumnMapping>
std::size_t
MapIterator<ColumnMapping>::ChunkOffset() const {
  std::size_t offset = 0;
  for(auto dim = std::size_t{0}; dim < nDim(); ++dim) {
    offset += chunk_index_[dim] * strides_[dim];
  }
  return offset;
}

template <typename ColumnMapping>
std::size_t
MapIterator<ColumnMapping>::GlobalOffset() const {
  return map_.get().FlatOffset(global_index_);
}

template <typename ColumnMapping>
inline std::size_t
MapIterator<ColumnMapping>::RangeSize(std::size_t dim) const {
  return rit_.get().range_length_[dim];
}

template <typename ColumnMapping>
inline std::size_t MapIterator<ColumnMapping>::MemStart(std::size_t dim) const {
  return rit_.get().mem_start_[dim];
}


template <typename ColumnMapping>
MapIterator<ColumnMapping> & MapIterator<ColumnMapping>::operator++() {
  assert(!done_);

  // Iterate from fastest to slowest changing dimension
  for(auto dim = std::size_t{0}; dim < nDim();) {
    chunk_index_[dim]++;
    global_index_[dim]++;
    // We've achieved a successful iteration in this dimension
    if(chunk_index_[dim] < RangeSize(dim)) { break; }
    // Reset to zero and retry in the next dimension
    else if(dim < RowDim()) {
      chunk_index_[dim] = 0;
      global_index_[dim] = MemStart(dim);
      ++dim;
    }
    // This was the slowest changing dimension so we're done
    else { done_ = true; break; }
  }

  return *this;
}

template <typename ColumnMapping>
bool MapIterator<ColumnMapping>::operator==(const MapIterator<ColumnMapping> & other) const {
  if(&rit_.get() != &other.rit_.get() || done_ != other.done_) return false;
  return done_ ? true : chunk_index_ == other.chunk_index_;
}


template <typename ColumnMapping>
RangeIterator<ColumnMapping>::RangeIterator(ColumnMapping & column_map, bool done) :
  map_(std::cref(column_map)),
  index_(column_map.nDim(), 0),
  disk_start_(column_map.nDim(), 0),
  mem_start_(column_map.nDim(), 0),
  range_length_(column_map.nDim(), 0),
  done_(done) {
    UpdateState();
}

// Return the Ranges for the given dimension
template <typename ColumnMapping>
const ColumnRange & RangeIterator<ColumnMapping>::DimRanges(std::size_t dim) const {
  assert(dim < nDim());
  return map_.get().DimRanges(dim);
}

// Return the Maps for the given dimension
template <typename ColumnMapping>
const ColumnMap & RangeIterator<ColumnMapping>::DimMaps(std::size_t dim) const {
  assert(dim < nDim());
  return map_.get().DimMaps(dim);
}

// Return the currently selected Range of the given dimension
template <typename ColumnMapping>
const Range & RangeIterator<ColumnMapping>::DimRange(std::size_t dim) const {
  assert(dim < nDim());
  return DimRanges(dim)[index_[dim]];
}

template <typename ColumnMapping>
std::size_t RangeIterator<ColumnMapping>::RangeElements() const {
  return std::accumulate(std::begin(index_), std::end(index_), std::size_t{1},
                          [](const auto i, auto v) { return i*v; });
}

template <typename ColumnMapping>
RangeIterator<ColumnMapping> & RangeIterator<ColumnMapping>::operator++() {
  assert(!done_);
  // Iterate from fastest to slowest changing dimension: FORTRAN order
  for(auto dim = 0; dim < nDim();) {
    index_[dim]++;
    mem_start_[dim] += range_length_[dim];

    // We've achieved a successful iteration in this dimension
    if(index_[dim] < DimRanges(dim).size()) { break; }
    // We've exceeded the size of the current dimension
    // reset to zero and retry the while loop
    else if(dim < RowDim()) { index_[dim] = 0; mem_start_[dim] = 0; ++dim; }
    // Row is the slowest changing dimension so we're done
    // return without updating the iterator state
    else { done_ = true; return *this; }
  }

  // Increment output memory buffer offset
  UpdateState();
  return *this;
};

template <typename ColumnMapping>
void RangeIterator<ColumnMapping>::UpdateState() {
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
      case Range::VARYING: {
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
}

template <typename ColumnMapping>
bool RangeIterator<ColumnMapping>::operator==(const RangeIterator<ColumnMapping> & other) const {
  if(&map_.get() != &other.map_.get() || done_ != other.done_) return false;
  return done_ ? true : index_ == other.index_;
};

template <typename ColumnMapping>
bool RangeIterator<ColumnMapping>::operator!=(const RangeIterator<ColumnMapping> & other) const {
  return !(*this == other);
}


// Returns a slicer for the row dimension
template <typename ColumnMapping>
casacore::Slicer RangeIterator<ColumnMapping>::GetRowSlicer() const {
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
template <typename ColumnMapping>
casacore::Slicer RangeIterator<ColumnMapping>::GetSectionSlicer() const {
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

// Returns the shape of this range
template <typename ColumnMapping>
casacore::IPosition RangeIterator<ColumnMapping>::GetShape() const {
  assert(!done_);
  auto shape = casacore::IPosition(nDim(), 0);
  for(std::size_t dim=0; dim < nDim(); ++dim) {
    shape[dim] = range_length_[dim];
  }
  return shape;
}


// CRTP base class for Read/Write Column Mappings
//
// Defines a mapping between ranges of a casacore column
// and an Arrow Array. If there is a single contiguous range
// then the mapping is regarded as simple -- the data can
// be read/written in one operation. Otherwise,
// multiple operations are required to read/write multiple
// disjoint ranges.
//
// BaseColumnMap should be subclassed to handle the different
// cases for reading and writing. Sub-classes should implement
// - std::size_t nDim() const
// - std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const
template <typename T>
struct BaseColumnMap {
  std::reference_wrapper<const casacore::TableColumn> column_;
  ColumnMaps maps_;
  ColumnRanges ranges_;

  // Return number of dimensions in the map
  inline std::size_t nDim() const {
    return static_cast<const T*>(this)->nDim();
  }

  // Return the dimension size for the specified row and dimension
  // This is appropriate for variably-shaped data.
  inline std::size_t RowDimSize(casacore::rownr_t row, std::size_t dim) const {
    return static_cast<const T*>(this)->RowDimSize(row, dim);
  }


  // Return the ColumnRange for the given dimension
  inline const ColumnMap & DimMaps(std::size_t dim) const {
    assert(dim < nDim());
    return maps_[dim];
  }

  // Return the ColumnRange for the given dimension
  inline const ColumnRange & DimRanges(std::size_t dim) const {
    assert(dim < nDim());
    return ranges_[dim];
  }

  // Return the row dimension in FORTRAN order
  inline std::size_t RowDim() const {
    assert(nDim() > 0);
    return nDim() - 1;
  }

  RangeIterator<T> RangeBegin() const {
    return RangeIterator{const_cast<T &>(*static_cast<const T*>(this)), false};
  }

  RangeIterator<T> RangeEnd() const {
    return RangeIterator{const_cast<T &>(*static_cast<const T*>(this)), true};
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
  bool IsSimple() const;

  // Find the total number of elements formed
  // by the possibly disjoint ranges in this map
  std::size_t nElements() const;
};

template <typename T>
bool BaseColumnMap<T>::IsSimple() const {
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
        case Range::VARYING:
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

template <typename T>
std::size_t BaseColumnMap<T>::nElements() const {
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
        switch(range.type) {
          case Range::VARYING:
            assert(row_range.IsSingleRow());
            dim_elements += RowDimSize(rr_id, dim);
            break;
          case Range::FREE:
          case Range::MAP:
            assert(range.IsValid());
            dim_elements += range.nRows();
            break;
          default:
            assert(false && "Unhandled Range::Type enum");
        }
      }
      row_elements *= dim_elements;
    }
    elements += row_elements;
  }

  return elements;
}


} // namespace arcea

#endif // ARCAE_MAP_ITERATOR_H