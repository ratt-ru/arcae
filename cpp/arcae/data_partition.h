#ifndef ARCAE_DATA_PARTITION_H
#define ARCAE_DATA_PARTITION_H

#include <vector>

#include <arrow/result.h>

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
  // Is the memory layout contiguous
  unsigned int contiguous_:1;
  // Does this data chunk contain empty disk ranges
  unsigned int empty_:1;

  static arrow::Result<DataChunk>
  Make(SpanPairs && dim_spans, const ResultShapeData & data_shape);

  // Get a Row Slicer for the disk span
  casacore::Slicer GetRowSlicer() const noexcept;
  // Get a Section Slicer for the disk span
  casacore::Slicer GetSectionSlicer() const noexcept;
  // Number of chunk dimensions
  std::size_t nDim() const noexcept { return dim_spans_.size(); }
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
  std::vector<Index> id_cache_;

};

}  // namespace detail
}  // namespace arcae


#endif // ARCAE_DATA_PARTITION_H