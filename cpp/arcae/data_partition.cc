#include "arcae/data_partition.h"

#include <algorithm>
#include <cstddef>
#include <numeric>

#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/Arrays/Slicer.h>

#include <arrow/result.h>

#include <arcae/result_shape.h>
#include <arcae/selection.h>

using casacore::IPosition;
using casacore::Slicer;

namespace arcae {
namespace detail {

namespace {

struct IndexResult {
  std::vector<IndexType> disk;
  std::vector<IndexType> mem;
};


bool IsMemoryContiguous(const SpanPairs & spans) {
  for(auto &[disk, mem]: spans) {
    for(std::size_t i=1; i < mem.size(); ++i) {
      if(mem[i] - mem[i - 1] != 1) return false;
    }
  }
  return true;
}

arrow::Result<bool> DiskSpansContainNegativeRanges(const SpanPairs & spans) {
  for(auto &[disk, mem]: spans) {
    std::size_t negative = 0;
    for(auto d: disk) negative += int(d < 0);
    if(negative == disk.size()) return true;
    if(negative > 0) {
      return arrow::Status::Invalid("Partially negative disk span");
    }
  }
  return false;
}

arrow::Status AreDiskSpansMonotonic(const SpanPairs & spans) {
  for(auto &[disk, mem]: spans) {
    for(std::size_t i=1; i < disk.size(); ++i) {
      if(disk[i] - disk[i - 1] != 1) {
        return arrow::Status::Invalid(
          "DataChunk disk span is not monotonic");
      }
    }
  }
  return arrow::Status::OK();
}


// Given a span of disk indices,
// return the sorted indices, as well as
// the associated argsort (which form the memory indices)
IndexResult MakeSortedIndices(const IndexSpan & ids) {
  std::vector<IndexType> mem(ids.size(), 0);
  std::iota(mem.begin(), mem.end(), 0);
  std::sort(mem.begin(), mem.end(), [&ids](auto l, auto r) { return ids[l] < ids[r]; });

  std::vector<IndexType> disk(ids.size(), 0);
  for(std::size_t i = 0; i < ids.size(); ++i) disk[i] = ids[mem[i]];
  return IndexResult{std::move(disk), std::move(mem)};
}

// Decompose disk and memory spans into span pairs
// associated with contiguous disk id ranges
arrow::Result<std::vector<SpanPair>> MakeSubSpans(
  const IndexSpan & disk_span,
  const IndexSpan & mem_span) {
    assert(disk_span.size() == mem_span.size());
    std::vector<SpanPair> result;
    std::size_t start = 0;

    auto MakeSubSpan = [&](std::size_t current) {
      auto disk = disk_span.subspan(start, current - start);
      auto mem = mem_span.subspan(start, current - start);
      result.emplace_back(SpanPair{std::move(disk), std::move(mem)});
      start = current;
    };

    for(std::size_t i = 1; i < disk_span.size(); ++i) {
      if(disk_span[i] < 0) {
        // negative indices, advance
        continue;
      } else if(disk_span[start] < 0) {
        // start was negative, but disk_span[i] is not
        // create the negative range
        MakeSubSpan(i);
      } else if(disk_span[i] == disk_span[i - 1]) {
        return arrow::Status::IndexError("Duplicate positive row indices encountered");
      } else if(disk_span[i] - disk_span[i - 1] == 1) {
        // monotonic range, advance
        continue;
      } else {
        // discontinuity in positive indices
        // create a positive range
        MakeSubSpan(i);
      }
    }

    MakeSubSpan(disk_span.size());
    return result;
}

// Creates a cartesian product of span pairs
arrow::Result<std::vector<DataChunk>>
MakeDataChunks(
    const std::vector<SpanPairs> & dim_spans,
    const ResultShapeData & data_shape) {
  std::vector<SpanPairs> product = {{}};

  for(const auto & span_pairs: dim_spans) {
    std::vector<SpanPairs> next_product;
    for(const auto & result: product) {
      for(const auto & span_pair: span_pairs) {
        next_product.push_back(result);
        next_product.back().push_back(span_pair);
      }
    }
    product = std::move(next_product);
  }

  std::vector<DataChunk> chunks(product.size());
  for(std::size_t i=0; i < product.size(); ++i) {
    ARROW_ASSIGN_OR_RAISE(chunks[i], DataChunk::Make(std::move(product[i]), data_shape));
  }
  return chunks;
}

} // namespace


// Get a Row Slicer for the disk span
Slicer
DataChunk::GetRowSlicer() const noexcept {
  auto row_span = dim_spans_[nDim() - 1];
  return Slicer(
    IPosition({row_span.disk[0]}),
    IPosition({row_span.disk[row_span.disk.size() - 1]}),
    Slicer::endIsLast);
}

// Get a Section Slicer for the disk span
Slicer
DataChunk::GetSectionSlicer() const noexcept {
  auto row_dim = nDim() - 1;
  IPosition start(row_dim, 0);
  IPosition end(row_dim, 0);

  for(std::size_t d = 0; d < row_dim; ++d) {
    auto span = dim_spans_[d];
    start[d] = span.disk[0];
    end[d] = span.disk[span.disk.size() - 1];
  }
  return Slicer(std::move(start), std::move(end), Slicer::endIsLast);
}

// Number of elements in the chunk
std::size_t
DataChunk::nElements() const noexcept {
  return std::accumulate(
    std::begin(dim_spans_),
    std::end(dim_spans_),
    std::size_t(1),
    [](auto i, auto v) {return i * v.disk.size(); });
}

// FORTRAN ordered shape of the chunk
casacore::IPosition
DataChunk::GetShape() const noexcept {
  casacore::IPosition shape(dim_spans_.size(), 0);
  for(std::size_t d=0; d < dim_spans_.size(); ++d) shape[d] = dim_spans_[d].disk.size();
  return shape;
}

// Factory function for creating a DataChunk
arrow::Result<DataChunk>
DataChunk::Make(SpanPairs &&dim_spans, const ResultShapeData & data_shape) {
  for(auto &[disk, mem]: dim_spans) {
    if(disk.size() != mem.size()) return arrow::Status::Invalid("disk and memory span size mismatch");
  }
  ARROW_ASSIGN_OR_RAISE(auto empty, DiskSpansContainNegativeRanges(dim_spans));
  if(!empty) ARROW_RETURN_NOT_OK(AreDiskSpansMonotonic(dim_spans));
  auto contiguous = IsMemoryContiguous(dim_spans);
  std::vector<IndexType> mem_index(dim_spans.size(), 0);
  for(std::size_t d = 0; d < dim_spans.size(); ++d) {
    mem_index[d] = *std::min_element(
      std::begin(dim_spans[d].mem),
      std::end(dim_spans[d].mem));
  }
  auto flat_offset = data_shape.FlatOffset(mem_index);
  return DataChunk{
    std::move(dim_spans),
    std::move(mem_index),
    flat_offset,
    contiguous,
    empty};
}

arrow::Result<DataPartition> DataPartition::Make(
    const Selection &selection,
    const ResultShapeData &result_shape) {

  // Construct an id cache, holding Index vectors
  // Spans will be constructed over these vectors
  // Initially, this holds indices that monotically
  // up to the maximum dimension size.
  auto id_cache = std::vector<Index>{Index(result_shape.MaxDimensionSize())};
  std::iota(id_cache.back().begin(), id_cache.back().end(), 0);
  auto monotonic_ids = IndexSpan(id_cache.back());
  auto ndim = result_shape.nDim();

  // Generate disk and memory spans for the specified dimension
  auto GetSpanPair = [&](auto dim, auto dim_size) -> SpanPair {
    if(auto dim_span = selection.FSpan(dim, ndim); dim_span.ok()) {
      // Sort selection indices to create
      // disk and memory indices
      auto [disk_ids, mem_ids] = MakeSortedIndices(dim_span.ValueOrDie());
      id_cache.emplace_back(std::move(disk_ids));
      auto disk_span = IndexSpan(id_cache.back());
      id_cache.emplace_back(std::move(mem_ids));
      auto mem_span = IndexSpan(id_cache.back());
      return SpanPair{std::move(disk_span), std::move(mem_span)};
    }
    assert(std::size_t(dim_size) <= monotonic_ids.size());
    auto disk_span = monotonic_ids.subspan(0, dim_size);
    auto mem_span = monotonic_ids.subspan(0, dim_size);
    return SpanPair{std::move(disk_span), std::move(mem_span)};
  };

  // In the fixed case, create disk and memory spans
  // over each dimension in FORTRAN order
  if(result_shape.IsFixed()) {
    std::vector<SpanPairs> dim_spans;
    dim_spans.reserve(result_shape.nDim());
    const auto & shape = result_shape.GetShape();
    for(std::size_t dim=0; dim < result_shape.nDim(); ++dim) {
      auto [disk_span, mem_span] = GetSpanPair(dim, shape[dim]);
      ARROW_ASSIGN_OR_RAISE(auto spans, MakeSubSpans(disk_span, mem_span));
      dim_spans.emplace_back(std::move(spans));
    }
    ARROW_ASSIGN_OR_RAISE(auto chunks, MakeDataChunks(dim_spans, result_shape))
    return DataPartition{
      std::move(chunks),
      result_shape.GetDataType(),
      std::move(id_cache)};
  }

  // In the varying case, start with the row dimension
  auto nrows = result_shape.nRows();
  auto row_dim = result_shape.nDim() - 1;
  auto [row_disk_span, row_mem_span] = GetSpanPair(row_dim, nrows);
  std::vector<DataChunk> chunks;

  for(std::size_t r = 0; r < nrows; ++r) {
    std::vector<SpanPairs> dim_spans;
    dim_spans.reserve(result_shape.nDim());
    const auto & row_shape = result_shape.GetRowShape(r);

    // Create span pairs for the secondary dimensions
    for(std::size_t dim=0; dim < row_dim; ++dim) {
      auto [disk_span, mem_span] = GetSpanPair(dim, row_shape[dim]);
      ARROW_ASSIGN_OR_RAISE(auto spans, MakeSubSpans(disk_span, mem_span));
      dim_spans.emplace_back(std::move(spans));
    }

    // Create span pairs for the row dimension
    dim_spans.emplace_back(SpanPairs{
      {
        row_disk_span.subspan(r, 1),
        row_mem_span.subspan(r, 1)
      }
    });

    // Create data chunks for this row and add
    // to the greater chunk collection
    ARROW_ASSIGN_OR_RAISE(auto row_chunks, MakeDataChunks(dim_spans, result_shape));
    chunks.insert(
      chunks.end(),
      std::move_iterator(row_chunks.begin()),
      std::move_iterator(row_chunks.end()));
  }

  if(chunks.size() == 0) return arrow::Status::Invalid("Data partition produced no chunks!");

  return DataPartition{
    std::move(chunks),
    result_shape.GetDataType(),
    std::move(id_cache)};
}

}  // namespace detail
}  // namespace arcae
