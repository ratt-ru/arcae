#ifndef ARCAE_DATA_PARTITION_H
#define ARCAE_DATA_PARTITION_H

#include <cassert>
#include <vector>

#include <absl/types/span.h>

#include <arrow/result.h>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables/RefRows.h>

#include "arcae/result_shape.h"
#include "arcae/selection.h"
#include "arcae/type_traits.h"

namespace arcae {
namespace detail {

// A pair of disk and memory index spans
struct SpanPair {
  IndexSpan disk;
  IndexSpan mem;
};

// A vector of SpanPairs
using SpanPairs = std::vector<SpanPair>;

// Data shared by chunks in an
// Array of Structs configuration
struct SharedChunkData {
  // Number of chunks
  std::size_t nchunks_;
  // Number of dimensions of each chunk
  std::size_t ndim_;
  // Casa Data Type
  casacore::DataType casa_dtype_;
  // Cache of disk and memory indexes
  // These are the backing values for the Dimension Spans below
  std::vector<Index> id_cache_;
  // Vector span pairs for each chunk
  // There are a total of ndim_ SpanPairs
  std::vector<SpanPairs> dim_spans_;
  // Vector of minimum memory index elements for each chunk
  // These values are used to compute the flat offset below
  // but are also used when transposing data into the output buffer
  // There are a total of nchunks_ * ndim_ values
  std::vector<IndexType> min_mem_index_;
  // Vector of starting offsets for the location
  // in the output buffer associated with this chunk
  // Used when transposing data into the output buffer.
  // There are a total of nchunks_ values
  std::vector<std::size_t> flat_offsets_;
  // Vector of output buffer strides for each chunk
  // There are a total of nchunks_ * ndim_ values
  std::vector<std::size_t> buffer_strides_;
  // Vector of strides for each chunk
  // Used when when transposing data into the output buffer.
  // There are a total of nchunks_ * ndim_ values
  std::vector<std::size_t> chunk_strides_;
  // Scratch space for position iterator in transpose function
  // There are a total of nchunks_ * ndim_ values
  std::vector<std::size_t> position_;
  // Vector indicating whether each chunk is contiguous
  std::vector<bool> contiguous_;

  // Number of dimensions in each chunk
  std::size_t nDim() const { return ndim_; }

  // Get the Casa Data Type
  casacore::DataType CasaDataType() const { return casa_dtype_; }

  // A span over the index pairs of this hcunk
  absl::Span<const SpanPair> DimensionSpans(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return absl::MakeSpan(dim_spans_[chunk]);
  }

  // A span over the minimum indices in each dimension of the chunk
  absl::Span<const IndexType> MinMemIndex(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return absl::MakeSpan(&min_mem_index_[chunk * ndim_], ndim_);
  }

  // A span over the chunk strides for each dimension of the chunk
  absl::Span<const std::size_t> ChunkStrides(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return absl::MakeSpan(&chunk_strides_[chunk * ndim_], ndim_);
  }

  // A span over the strides in the output buffer location associated
  // with the chunk
  absl::Span<const std::size_t> BufferStrides(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return absl::MakeSpan(&buffer_strides_[chunk * ndim_], ndim_);
  }

  // A span over the position scratch space associated with the chunk
  absl::Span<std::size_t> ScratchPositions(std::size_t chunk) {
    assert(chunk < nchunks_);
    return absl::MakeSpan(&position_[chunk * ndim_], ndim_);
  }

  // The starting position in the output buffer location associated
  // with the chunk
  std::size_t FlatOffset(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return flat_offsets_[chunk];
  }

  // Is the chunk contiguous
  bool IsContiguous(std::size_t chunk) const {
    assert(chunk < nchunks_);
    return contiguous_[chunk];
  }
};

// A mapping of a chunk of data from
// contiguous areas of disk to possibly
// contiguous areas of memory.
// Multiple chunks form part of a
// DataPartition
struct DataChunk {
  // Index into SharedChunkData
  std::size_t chunk_id_;
  // Shared Chunk Data
  std::shared_ptr<SharedChunkData> shared_;

  // Is this a valid chunk
  explicit operator bool() const { return bool(shared_); }

  // Get the Casa Data Type associated with the chunk
  casacore::DataType CasaDataType() const { return shared_->CasaDataType(); }

  // Return the disk span at the specified dimension
  const IndexSpan& Disk(std::size_t dim) const { return DimensionSpans()[dim].disk; }

  // Return the memory span at the specified dimension
  const IndexSpan& Mem(std::size_t dim) const { return DimensionSpans()[dim].mem; }
  // Obtain the disk and memory index spans for this chunk
  absl::Span<const SpanPair> DimensionSpans() const {
    return shared_->DimensionSpans(chunk_id_);
  }

  // Obtain the minimum elements of the memory spans
  absl::Span<const IndexType> MinMemIndex() const {
    return shared_->MinMemIndex(chunk_id_);
  }

  // Obtain the strides for this chunk
  absl::Span<const std::size_t> ChunkStrides() const {
    return shared_->ChunkStrides(chunk_id_);
  }

  // Obtain the initial starting offset within the output buffer
  std::size_t FlatOffset() const { return shared_->FlatOffset(chunk_id_); }

  // Obtain the strides for the location in the output buffer
  // associated with this chunk
  absl::Span<const std::size_t> BufferStrides() const {
    return shared_->BufferStrides(chunk_id_);
  }

  // Obtain the position scratch values associated with this chunk
  absl::Span<std::size_t> ScratchPositions() {
    return shared_->ScratchPositions(chunk_id_);
  }

  // Is the chunk contiguous?
  bool IsContiguous() const { return shared_->IsContiguous(chunk_id_); }

  // Get a Row Slicer for the disk span
  casacore::Slicer RowSlicer() const noexcept;

  // Get Reference Rows for the disk span
  casacore::RefRows ReferenceRows() const noexcept;

  // Get a Section Slicer for the disk span
  casacore::Slicer SectionSlicer() const noexcept;

  // Number of chunk dimensions
  std::size_t nDim() const noexcept { return shared_->nDim(); }

  // Number of elements in the chunk
  std::size_t nElements() const noexcept;

  std::size_t nBytes() const {
    std::size_t nbytes = CasaDataTypeSize(CasaDataType()).ValueOrDie();
    return nbytes * nElements();
  }

  // Shape of the chunk
  casacore::IPosition GetShape() const noexcept;

  // Does the chunk refer to negative disk indices
  bool IsEmpty() const noexcept;
};

// A partition of the data into a series
// of chunks that can be independently processed
struct DataPartition {
  static arrow::Result<DataPartition> Make(const Selection& selection,
                                           const ResultShapeData& result_shape);

  std::vector<DataChunk> data_chunks_;
  casacore::DataType casa_dtype_;
  std::vector<Index> id_cache_;

  // Consume chunks in the partition
  std::vector<DataChunk>&& TakeChunks() noexcept { return std::move(data_chunks_); }

  // Get the casa data type associated with the data partition;
  casacore::DataType GetDataType() const noexcept { return casa_dtype_; }

  // Return the number of chunks in the partition
  std::size_t nChunks() const noexcept { return data_chunks_.size(); }

  // Return the number of elements in the partition
  std::size_t nElements() const noexcept {
    return std::accumulate(std::begin(data_chunks_), std::end(data_chunks_),
                           std::size_t(0),
                           [](auto i, auto v) { return i + v.nElements(); });
  }

  // Return the DataChunk at index
  const DataChunk& Chunk(std::size_t index) const noexcept { return data_chunks_[index]; }
};

}  // namespace detail
}  // namespace arcae

// Define arrow IterationTraits for DataChunk
template <>
struct arrow::IterationTraits<arcae::detail::DataChunk> {
  static arcae::detail::DataChunk End() { return {}; }
  static bool IsEnd(const arcae::detail::DataChunk& val) { return !val; }
};

#endif  // ARCAE_DATA_PARTITION_H
