#include "arcae/read_impl.h"

#include <cstddef>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/array/array_base.h>
#include <arrow/array/array_nested.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/logging.h>
#include <arrow/util/thread_pool.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>

#include "arcae/array_util.h"
#include "arcae/data_partition.h"
#include "arcae/isolated_table_proxy.h"
#include "arcae/result_shape.h"
#include "arcae/selection.h"
#include "arcae/service_locator.h"
#include "arcae/type_traits.h"

using ::arrow::Array;
using ::arrow::Buffer;
using ::arrow::CallbackOptions;
using ::arrow::CollectAsyncGenerator;
using ::arrow::Int32Builder;
using ::arrow::ListArray;
using ::arrow::FixedSizeListArray;
using ::arrow::PrimitiveArray;
using ::arrow::MakeVectorGenerator;
using ::arrow::MakeMappedGenerator;
using ::arrow::Future;
using ::arrow::Status;
using ::arrow::StringBuilder;
using ::arrow::Result;
using ::arrow::ShouldSchedule;
using ::arrow::internal::GetCpuThreadPool;

template <class CT> using CasaArray = ::casacore::Array<CT>;
using ::casacore::ArrayColumn;
using ::casacore::DataType;
using ::casacore::ScalarColumn;
using ::casacore::TableColumn;
using ::casacore::TableProxy;
using CasaTable = ::casacore::Table;
template <class CT> using CasaVector = ::casacore::Vector<CT>;
using ::casacore::String;

using ::arcae::detail::DataChunk;

namespace arcae {
namespace detail {

namespace {

// Maximum number of DataChunk dimensions on the fast path
static constexpr std::size_t kMaxTransposeDims = 5;

template <typename CT>
bool TransposeData(
    const CasaArray<CT> & data,
    const SpanPairs & spans,
    CT * out_ptr,
    std::size_t flat_offset,
    const std::vector<std::size_t> & buffer_strides) {
  const CT * in_ptr = data.data();
  std::ptrdiff_t ndim = spans.size();
  std::ptrdiff_t row_dim = std::ptrdiff_t(ndim) - 1;

  // Initialise minimum memory index
  std::array<IndexType, kMaxTransposeDims> min_mem;
  for(std::ptrdiff_t d=0; d < ndim; ++d) {
    min_mem[d] = *std::min_element(std::begin(spans[d].mem), std::end(spans[d].mem));
  }

  auto DimSize = [&](auto d) -> std::ptrdiff_t { return spans[d].mem.size(); };

  // Initialise strides
  std::array<std::ptrdiff_t, kMaxTransposeDims> in_strides{1};
  const auto & data_shape = data.shape();
  for(std::ptrdiff_t d = 1, product = 1; d < ndim; ++d) {
    product *= data_shape[d- 1];
    in_strides[d] = product;
  }

  // Initialise position array
  std::array<std::ptrdiff_t, kMaxTransposeDims> pos;
  pos.fill(0);

  auto MemRelative = [&](auto d, auto i) -> std::ptrdiff_t {
    return spans[d].mem[i] - min_mem[d];
  };

  // Iterate over the spans in memory, copying data
  while(true) {
    std::ptrdiff_t in_offset = 0;
    std::ptrdiff_t out_offset = 0;
    for(std::ptrdiff_t d = 0; d < ndim; ++d) {
      in_offset += pos[d] * in_strides[d];
      out_offset += MemRelative(d, pos[d]) * buffer_strides[d];
    }

    // std::stringstream oss;
    // oss << "Position [";
    // for(std::ptrdiff_t d = 0; d < ndim; ++d) {
    //   if(d > 0) oss << ',';
    //   oss << spans[d].mem[pos[d]];
    // }
    // oss << "] Relative [";
    // for(std::ptrdiff_t d = 0; d < ndim; ++d) {
    //   if(d > 0) oss << ',';
    //   oss << MemRelative(d, pos[d]);
    // }

    // oss << "] Strides [";
    // for(std::ptrdiff_t d = 0; d < ndim; ++d) {
    //   if(d > 0) oss << ',';
    //   oss << in_strides[d];
    // }

    // oss << "] " << flat_offset << ' '  << in_offset << ' ' << out_offset << ' ' << flat_offset + out_offset << ' ' << in_ptr[in_offset];
    // ARROW_LOG(INFO) << oss.str();

    // Moves degrade to copies for simple (i.e. numeric) types
    // but it should make casacore::String more efficient
    out_ptr[out_offset] = std::move(in_ptr[in_offset]);

    // Iterate in FORTRAN order
    for(std::ptrdiff_t d=0; d < ndim; ++d) {
      if(++pos[d] < DimSize(d)) break;
      pos[d] = 0;
      if(d == row_dim) return true;
    }
  }
}

// Reads a chunk of data from disk
// before tranposing it into the desired order
// in the output buffer
template <
  DataType CDT,
  typename CT = typename CasaDataTypeTraits<CDT>::CasaType>
struct ReadAndTransposeImpl {
  std::string column;
  std::shared_ptr<IsolatedTableProxy> itp;
  std::shared_ptr<Buffer> buffer;

  Future<bool> operator()(const DataChunk & chunk) const {
    assert(itp != nullptr);
    auto read_fut = itp->RunAsync([
      column = std::move(column),
      ndim = chunk.nDim(),
      row_slicer = chunk.GetRowSlicer(),
      section_slicer = chunk.GetSectionSlicer()
    ](const TableProxy & tp) -> Future<CasaArray<CT>> {
      if(ndim == 1) {
        auto data = ScalarColumn<CT>(tp.table(), column);
        return data.getColumnRange(row_slicer);
      }
      auto data = ArrayColumn<CT>(tp.table(), column);
      return data.getColumnRange(row_slicer, section_slicer);
    });

    // Perform the transpose
    return read_fut.Then([
      dim_spans = std::move(chunk.dim_spans_),
      out_ptr = buffer->mutable_data_as<CT>() + chunk.flat_offset_,
      offset = chunk.flat_offset_,
      buffer_strides = std::move(chunk.buffer_strides_)
    ](const CasaArray<CT> & data) mutable -> bool {
      return TransposeData(data, dim_spans, out_ptr, offset, buffer_strides);
    }, {}, CallbackOptions{ShouldSchedule::Always, GetCpuThreadPool()});
  }
};

// Reads a chunk of data from disk
// directly into the output buffer
template <
  DataType CDT,
  typename CT = typename CasaDataTypeTraits<CDT>::CasaType>
struct ReadInPlaceImpl {
  std::string column;
  std::shared_ptr<IsolatedTableProxy> itp;
  std::shared_ptr<Buffer> buffer;

  Future<bool> operator()(const DataChunk & chunk) const {
    assert(itp != nullptr);
    assert(chunk.IsContiguous());
    return itp->RunAsync([
      column = std::move(column),
      row_slicer = chunk.GetRowSlicer(),
      section_slicer = chunk.GetSectionSlicer(),
      shape = chunk.GetShape(),
      out_ptr = buffer->mutable_data_as<CT>() + chunk.flat_offset_
    ](const TableProxy & tp) -> Future<bool> {
      if(shape.size() == 1) {
        auto data = ScalarColumn<CT>(tp.table(), column);
        auto vector = CasaVector<CT>(shape, out_ptr, casacore::SHARE);
        data.getColumnRange(row_slicer, vector);
        return true;
      }
      auto data = ArrayColumn<CT>(tp.table(), column);
      auto array = CasaArray<CT>(shape, out_ptr, casacore::SHARE);
      data.getColumnRange(row_slicer, section_slicer, array);
      return true;
    });
  }
};


// Functor implementing dispatch of disk reading functionality
struct ReadCallback {
  // Column name
  std::string column;
  // Isolated TableProxy
  std::shared_ptr<IsolatedTableProxy> itp;
  // Required to keep Chunk Indices alive
  std::shared_ptr<DataPartition> partition;
  // Output buffer
  std::shared_ptr<Buffer> buffer;
  // CASA Data Type
  casacore::DataType dtype;

  // Dispatch to different callback implementations
  // dependent on the casacore DataType and
  // chunk characteristics
  template <DataType CDT>
  inline Future<bool> Dispatch(const DataChunk & chunk) const {
    if(chunk.IsEmpty()) return true;
    if(chunk.IsContiguous()) {
      return ReadInPlaceImpl<CDT>{
        std::move(column),
        std::move(itp),
        std::move(buffer)}(chunk);
    }
    return ReadAndTransposeImpl<CDT>{
      std::move(column),
      std::move(itp),
      std::move(buffer)}(chunk);
  }

  // Read a chunk of data into the encapsulated buffer
  // returning true on success
  Future<bool> operator()(const DataChunk & chunk) const {
    if(chunk.nDim() == 0) return Status::Invalid("Zero dimension chunk");
    if(chunk.nDim() >= kMaxTransposeDims) {
      return Status::Invalid("Chunk has dimension > ", kMaxTransposeDims);
    }
    switch(dtype) {
      case DataType::TpBool:
        return Dispatch<DataType::TpBool>(chunk);
        break;
      case DataType::TpChar:
        return Dispatch<DataType::TpChar>(chunk);
        break;
      case DataType::TpUChar:
        return Dispatch<DataType::TpUChar>(chunk);
        break;
      case DataType::TpShort:
        return Dispatch<DataType::TpShort>(chunk);
        break;
      case DataType::TpUShort:
        return Dispatch<DataType::TpUShort>(chunk);
        break;
      case DataType::TpInt:
        return Dispatch<DataType::TpInt>(chunk);
        break;
      case DataType::TpUInt:
        return Dispatch<DataType::TpUInt>(chunk);
        break;
      case DataType::TpInt64:
        return Dispatch<DataType::TpInt64>(chunk);
        break;
      case DataType::TpFloat:
        return Dispatch<DataType::TpFloat>(chunk);
        break;
      case DataType::TpDouble:
        return Dispatch<DataType::TpDouble>(chunk);
        break;
      case DataType::TpComplex:
        return Dispatch<DataType::TpComplex>(chunk);
        break;
      case DataType::TpDComplex:
        return Dispatch<DataType::TpDComplex>(chunk);
        break;
      case DataType::TpString:
        return Dispatch<DataType::TpString>(chunk);
        break;
      default:
        return Status::NotImplemented(
          "Column ", column,
          " with data type ", dtype);
    }
  }
};


// Extracts the data buffer of the underlying result array
// ensuring it equals nbytes.
// Otherwise allocates a buffer of nbytes
arrow::Result<std::shared_ptr<Buffer>>
GetResultBufferOrAllocate(
    std::size_t nelements,
    DataType casa_type,
    const std::shared_ptr<Array> & result) {

  ARROW_ASSIGN_OR_RAISE(auto casa_type_size, CasaDataTypeSize(casa_type));
  auto nbytes = nelements * casa_type_size;
  if(auto r = GetResultBuffer(result, nbytes); r.ok()) return r;
  ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nbytes, casa_type_size));

  if(casacore::isNumeric(casa_type)) {
    return std::shared_ptr<Buffer>(std::move(allocation));
  } else if(casa_type == DataType::TpString) {
    // We need to use placement new and delete for non-POD types
    // Probably could use some std::is_pod_v<T> strategy,
    // just use if(casa_type == TpString) for now
    for(auto & s: allocation->mutable_span_as<String>()) {
      new (&s) String;
    }

    return std::shared_ptr<Buffer>(
      std::unique_ptr<Buffer, void(*)(Buffer*)>(
        allocation.release(),
        [](Buffer * buffer) {
          for(auto & s: buffer->mutable_span_as<String>()) {
            s.String::~String();
          }
          delete buffer;
        }));
  }
  return Status::TypeError("Unhandled casa type", casa_type);
}

enum ConvertStrategy { FIXED, LIST };

Result<ConvertStrategy> GetConvertStrategy() {
  auto & config = ServiceLocator::configuration();
  auto strategy = config.GetDefault("casa.convert.strategy", "fixed");
  if(auto pos = strategy.find("fixed"); pos != std::string::npos) {
    return ConvertStrategy::FIXED;
  } else if(auto pos = strategy.find("list"); pos != std::string::npos) {
    return ConvertStrategy::LIST;
  }
  return Status::Invalid("Invalid 'casa.convert.strategy=", strategy, "'");
}

// Given a buffer populated with values
// and an expected result shape,
// create an output array
Result<std::shared_ptr<Array>> MakeArray(
  const ResultShapeData & result_shape,
  const std::shared_ptr<Buffer> & buffer
) {
  auto casa_type = result_shape.GetDataType();
  ARROW_ASSIGN_OR_RAISE(auto arrow_dtype, ArrowDataType(casa_type));
  auto nelements = result_shape.nElements();
  std::shared_ptr<arrow::Array> result;
  ARROW_ASSIGN_OR_RAISE(auto strat, GetConvertStrategy());

  if(casa_type == casacore::TpString) {
    // Buffer holds casacore::Strings, convert to arrow strings
    StringBuilder builder;
    auto span = buffer->mutable_span_as<casacore::String>();
    for(std::size_t i = 0; i < span.size(); ++i) {
      ARROW_RETURN_NOT_OK(builder.Append(std::move(span[i])));
    }
    ARROW_ASSIGN_OR_RAISE(result, builder.Finish());
  } else if(casacore::isComplex(casa_type)) {
    // Buffer holds complex values, created a fixed or nested list
    auto base = std::make_shared<PrimitiveArray>(arrow_dtype, 2*nelements, buffer);
    if(strat == ConvertStrategy::FIXED) {
      ARROW_ASSIGN_OR_RAISE(result, FixedSizeListArray::FromArrays(base, 2));
    } else if(strat == ConvertStrategy::LIST) {
      arrow::Int32Builder builder;
      ARROW_RETURN_NOT_OK(builder.Reserve(nelements + 1));
      for(std::size_t i = 0; i < nelements + 1; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(2*i));
      }
      ARROW_ASSIGN_OR_RAISE(auto offsets, builder.Finish())
      ARROW_ASSIGN_OR_RAISE(result, ListArray::FromArrays(*offsets, *base));
    }
  } else if(casacore::isNumeric(casa_type)) {
    result = std::make_shared<PrimitiveArray>(arrow_dtype, nelements, buffer);
  } else {
    return Status::TypeError("Unhandled CASA Type ", casa_type);
  }

  // Introduce shape nesting
  if(result_shape.IsFixed() && strat == ConvertStrategy::FIXED) {
    // Exclude the row dimension
    auto ndim = result_shape.nDim();
    auto shape = result_shape.GetShape().getFirst(ndim - 1);
    for(auto dim: shape) {
      ARROW_ASSIGN_OR_RAISE(result, FixedSizeListArray::FromArrays(result, dim));
    }
  } else {
    ARROW_ASSIGN_OR_RAISE(auto offsets, result_shape.GetOffsets());
    assert(offsets.size() == result_shape.nDim() - 1);
    for(const auto & offset: offsets) {
      ARROW_ASSIGN_OR_RAISE(result, ListArray::FromArrays(*offset, *result));
    }
  }

  return result;
}

Status ColumnExists(const CasaTable & table, const std::string & column) {
  if(table.tableDesc().isColumn(column)) return Status::OK();
  return Status::Invalid("Column ", column, " does not exist");
}

} // namespace

arrow::Future<std::shared_ptr<Array>>
ReadImpl(
    const std::shared_ptr<IsolatedTableProxy> & itp,
    const std::string & column,
    const Selection & selection,
    const std::shared_ptr<Array> & result) {
  // Get the shape of the result, given the supplied selection
  // and the given result array
  struct ShapeResult {
    std::shared_ptr<ResultShapeData> shape;
    std::shared_ptr<Selection> selection;
  };

  auto shape_fut = itp->RunAsync([
      column = column,
      selection = selection,
      result = result
    ](const TableProxy & tp) mutable -> Result<ShapeResult>  {
      ARROW_RETURN_NOT_OK(ColumnExists(tp.table(), column));
      auto table_column = TableColumn(tp.table(), column);
      ARROW_ASSIGN_OR_RAISE(
        auto shape_data,
        ResultShapeData::MakeRead(table_column, selection, result));
      return ShapeResult{
        std::make_shared<ResultShapeData>(std::move(shape_data)),
        std::make_shared<Selection>(std::move(selection))
      };
    });

  // Partition the resulting shape into contiguous chunks
  // of data to read from disk
  struct PartitionResult {
    std::shared_ptr<DataPartition> partition;
    std::shared_ptr<ResultShapeData> shape;
  };
  auto part_fut = shape_fut.Then(
    [](const ShapeResult & result) mutable -> Result<PartitionResult> {
      ARROW_ASSIGN_OR_RAISE(
        auto partition,
        DataPartition::Make(*result.selection, *result.shape));
      return PartitionResult{
        std::make_shared<DataPartition>(std::move(partition)),
        std::move(result.shape)
      };
    }, {}, CallbackOptions{ShouldSchedule::Always, GetCpuThreadPool()});

  // Read each contiguous chunk of data on disk independently
  // then create an output array
  auto read_fut = part_fut.Then([
      column = column,
      itp = itp,
      result_array = result
    ](const PartitionResult & result) mutable -> Future<std::shared_ptr<Array>> {
      auto casa_dtype = result.partition->GetDataType();
      auto nelements = result.partition->nElements();
      ARROW_ASSIGN_OR_RAISE(auto buffer,
        GetResultBufferOrAllocate(nelements, casa_dtype, result_array));

      // Make an async generator over the data chunks
      // Map ReadCallBack over all data chunks
      auto data_chunk_gen = MakeVectorGenerator(std::move(result.partition->TakeChunks()));
      auto read_and_copy_data_gen = MakeMappedGenerator(
        std::move(data_chunk_gen),
        ReadCallback{std::move(column), itp, result.partition, buffer, casa_dtype});

      // Collect the read results
      auto collect = CollectAsyncGenerator(std::move(read_and_copy_data_gen));

      // Make the array from the now populated buffer
      return collect.Then([
        result_shape = result.shape,
        buffer = buffer
      ](const std::vector<bool> & result) {
        return MakeArray(*result_shape, buffer);
      });
    }, {}, CallbackOptions{ShouldSchedule::Always, GetCpuThreadPool()});

  return read_fut;
}

}  // namespace detail
}  // namespace arcae