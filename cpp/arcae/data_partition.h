#ifndef ARCAE_DATA_PARTITION_H
#define ARCAE_DATA_PARTITION_H

#include <vector>

#include <arrow/result.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>

#include "arcae/result_shape.h"
#include "arcae/selection.h"

namespace arcae {
namespace detail {

// A pair of disk and memory index spans
struct SpanPair {
  IndexSpan disk;
  IndexSpan mem;
};

// A vector of SpanPairs
using SpanPairs = std::vector<SpanPair>;

// A mapping of a chunk of data from
// contiguous areas of disk to possibly
// contiguous areas of memory.
// Multiple chunks form part of a
// DataPartition
struct DataChunk {
  // Disk and memory spans for each dimension
  SpanPairs dim_spans_;
  // Minimum memory indices
  std::vector<IndexType> min_mem_indices_;
  // Flattened chunk offset within output
  std::size_t flat_offset_;
  // Is the memory layout contiguous?
  bool contiguous_;
  // Does this data chunk represent an empty selection?
  // i.e. it contains negative disk id ranges
  bool empty_;

  static arrow::Result<DataChunk>
  Make(SpanPairs && dim_spans, const ResultShapeData & data_shape, bool contiguous=false);

  // Return the disk span at the specified dimension
  const IndexSpan & Disk(std::size_t dim) const noexcept {
    return dim_spans_[dim].disk;
  }

  // Return the memory span at the specified dimension
  const IndexSpan & Mem(std::size_t dim) const noexcept {
    return dim_spans_[dim].mem;
  }

  // Get a Row Slicer for the disk span
  casacore::Slicer GetRowSlicer() const noexcept;
  // Get a Section Slicer for the disk span
  casacore::Slicer GetSectionSlicer() const noexcept;
  // Number of chunk dimensions
  std::size_t nDim() const noexcept { return dim_spans_.size(); }
  // Number of elements in the chunk
  std::size_t nElements() const noexcept;
  // Shape of the chunk
  casacore::IPosition GetShape() const noexcept;
  // Is the chunk contiguous
  constexpr bool IsContiguous() const noexcept { return contiguous_; }
  // Is the chunk negative
  constexpr bool IsEmpty() const noexcept { return empty_; }
};

// A partition of the data into a series
// of chunks that can be independently processed
struct DataPartition {
  static arrow::Result<DataPartition> Make(
    const Selection & selection,
    const ResultShapeData & result_shape);

  std::vector<DataChunk> data_chunks_;
  casacore::DataType casa_dtype_;
  std::vector<Index> id_cache_;

  // Consume chunks in the partition
  std::vector<DataChunk> && TakeChunks() noexcept {
    return std::move(data_chunks_);
  }

  // Get the casa data type associated with the data partition;
  casacore::DataType GetDataType() const noexcept { return casa_dtype_; }

  // Return the number of chunks in the partition
  std::size_t nChunks() const noexcept { return data_chunks_.size(); }

  // Return the number of elements in the partition
  std::size_t nElements() const noexcept {
    return std::accumulate(
      std::begin(data_chunks_),
      std::end(data_chunks_),
      std::size_t(0),
      [](auto i, auto v) { return i + v.nElements(); });
  }

  // Return the DataChunk at index
  const DataChunk & Chunk(std::size_t index) const noexcept {
    return data_chunks_[index];
  }
};

}  // namespace detail
}  // namespace arcae


// Define arrow IterationTraits for DataChunk
template <>
struct arrow::IterationTraits<arcae::detail::DataChunk> {
  static arcae::detail::DataChunk End() { return {}; }
  static bool IsEnd(const arcae::detail::DataChunk& val) {
    return val.nDim() == 0;
  }
};

#endif // ARCAE_DATA_PARTITION_H