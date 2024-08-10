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
#include <casacore/tables/Tables/ArrayColumn.h>
#include <casacore/tables/Tables/ScalarColumn.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableProxy.h>

#include "arcae/array_util.h"
#include "arcae/data_partition.h"
#include "arcae/isolated_table_proxy.h"
#include "arcae/result_shape.h"
#include "arcae/selection.h"
#include "arcae/table_utils.h"
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

// Functor implementing dispatch of disk writing functionality
struct WriteCallback {
  // Column name
  std::string column;
  // Isolated TableProxy
  std::shared_ptr<IsolatedTableProxy> itp;
  // Output buffer
  std::shared_ptr<Buffer> buffer;

  // Write data, dependent on chunk characteristics and CASA data type
  template <DataType CDT>
  inline Future<bool> DoWrite(const DataChunk & chunk) const {
    using CT = typename CasaDataTypeTraits<CDT>::CasaType;

    // If the chunk is contiguous in memory, we can write
    // from a CASA Array view over that position in the input
    if(chunk.IsContiguous()) {
      return itp->RunAsync([
        column_name = std::move(column),
        chunk = chunk,
        buffer = buffer
      ](const TableProxy & tp) -> Future<bool> {
        auto in_ptr = buffer->template mutable_data_as<CT>() + chunk.FlatOffset();
        auto shape = chunk.GetShape();
        if(shape.size() == 1) {
          auto column = ScalarColumn<CT>(tp.table(), column_name);
          auto vector = CasaVector<CT>(shape, in_ptr, casacore::SHARE);
          column.putColumnRange(chunk.RowSlicer(), vector);
          return true;
        }
        auto column = ArrayColumn<CT>(tp.table(), column_name);
        auto array = CasaArray<CT>(shape, in_ptr, casacore::SHARE);
        column.putColumnRange(chunk.RowSlicer(), chunk.SectionSlicer(), array);
        return true;
      });
    }

    // Transpose the array into the output buffer
    return arrow::DeferNotOk(
      GetCpuThreadPool()->Submit([
          chunk = chunk,
          buffer = buffer,
          itp = itp,
          column_name = std::move(column)
        ]() mutable -> Future<bool> {
          std::ptrdiff_t ndim = chunk.nDim();
          std::ptrdiff_t last_dim = ndim - 1;
          auto spans = chunk.DimensionSpans();
          auto min_mem = chunk.MinMemIndex();
          auto chunk_strides = chunk.ChunkStrides();
          auto buffer_strides = chunk.BufferStrides();
          const CT * in_ptr = buffer->template mutable_data_as<CT>() + chunk.FlatOffset();
          auto array = CasaArray<CT>(chunk.GetShape());
          CT * out_ptr = array.data();
          auto pos = chunk.ScratchPositions();
          for(std::size_t i = 0; i < pos.size(); ++i) pos[i] = 0;

          // Iterate over the spans in memory, copying data
          for(bool done = false; !done; ) {
            std::size_t i = 0, o = 0;
            for(std::ptrdiff_t d = 0; d < ndim; ++d) {
              i += (spans[d].mem[pos[d]] - min_mem[d]) * buffer_strides[d];
              o += pos[d] * chunk_strides[d];
            }
            // Moves degrade to copies for simple (i.e. numeric) types
            // but it should make casacore::String more efficient by avoiding copies
            out_ptr[o] = std::move(in_ptr[i]);
            for(std::ptrdiff_t d=0; d < ndim; ++d) {     // Iterate in FORTRAN order
              if(++pos[d] < spans[d].mem.size()) break;  // Iteration doesn't reach dim end
              pos[d] = 0;                                // OtherwBasic iteration worksise reset, next dim
              if(d == last_dim) done = true;             // The last dim is reset, we're done
            }
          }

          // Write from CASA Array into the CASA column
          return itp->RunAsync([
            column_name = std::move(column_name),
            array = std::move(array),
            chunk = std::move(chunk),
            itp = std::move(itp)
          ](const TableProxy & tp) -> Future<bool> {
            if(chunk.nDim() == 1) {
              auto column = ScalarColumn<CT>(tp.table(), column_name);
              column.putColumnRange(chunk.RowSlicer(), array);
              return true;
            }
            auto column = ArrayColumn<CT>(tp.table(), column_name);
            column.putColumnRange(chunk.RowSlicer(), chunk.SectionSlicer(), array);
            return true;
          });
        }));
  }

  // Read a chunk of data into the encapsulated buffer
  // returning true on success
  Future<bool> operator()(const DataChunk & chunk) const {
    if(!chunk) return Status::Invalid("Invalid chunk");
    if(chunk.IsEmpty()) return true;
    switch(chunk.CasaDataType()) {
      case DataType::TpBool:
        return DoWrite<DataType::TpBool>(chunk);
        break;
      case DataType::TpChar:
        return DoWrite<DataType::TpChar>(chunk);
        break;
      case DataType::TpUChar:
        return DoWrite<DataType::TpUChar>(chunk);
        break;
      case DataType::TpShort:
        return DoWrite<DataType::TpShort>(chunk);
        break;
      case DataType::TpUShort:
        return DoWrite<DataType::TpUShort>(chunk);
        break;
      case DataType::TpInt:
        return DoWrite<DataType::TpInt>(chunk);
        break;
      case DataType::TpUInt:
        return DoWrite<DataType::TpUInt>(chunk);
        break;
      case DataType::TpInt64:
        return DoWrite<DataType::TpInt64>(chunk);
        break;
      case DataType::TpFloat:
        return DoWrite<DataType::TpFloat>(chunk);
        break;
      case DataType::TpDouble:
        return DoWrite<DataType::TpDouble>(chunk);
        break;
      case DataType::TpComplex:
        return DoWrite<DataType::TpComplex>(chunk);
        break;
      case DataType::TpDComplex:
        return DoWrite<DataType::TpDComplex>(chunk);
        break;
      case DataType::TpString:
        return DoWrite<DataType::TpString>(chunk);
        break;
      default:
        return Status::NotImplemented(
          "Column ", column,
          " with data type ",
          chunk.CasaDataType());
    }
  }
};

} // namespace

Future<bool>
WriteImpl(
    const std::shared_ptr<IsolatedTableProxy> & itp,
    const std::string & column,
    const std::shared_ptr<arrow::Array> & data,
    const Selection & selection) {

  // Get the shape of the input data, given the supplied selection
  // and the given result array
  struct ShapeResult {
    std::shared_ptr<ResultShapeData> shape;
    std::shared_ptr<Selection> selection;
  };

  auto shape_fut = itp->RunAsync([
    column = column,
    selection = selection,
    data = data
  ](const TableProxy & tp) mutable -> Result<ShapeResult>  {
    ARROW_RETURN_NOT_OK(ColumnExists(tp.table(), column));
    auto table_column = TableColumn(tp.table(), column);
    ARROW_ASSIGN_OR_RAISE(
      auto shape_data,
      ResultShapeData::MakeWrite(table_column, data, selection));
    return ShapeResult{
      std::make_shared<ResultShapeData>(std::move(shape_data)),
      std::make_shared<Selection>(std::move(selection))
    };
  });

  // Partition the resulting shape into contiguous chunks
  // of data to write to disk
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

  // Write each contiguous chunk of data on disk independently
  // then create an output array
  auto write_fut = part_fut.Then([
    column = column,
    itp = itp,
    data = data
  ](const PartitionResult & result) mutable -> Future<bool> {
    auto casa_dtype = result.partition->GetDataType();
    ARROW_ASSIGN_OR_RAISE(auto casa_type_size, CasaDataTypeSize(casa_dtype));
    auto nelements = result.partition->nElements();
    auto nbytes = nelements * casa_type_size;
    ARROW_ASSIGN_OR_RAISE(auto buffer, GetResultBuffer(data, nbytes));

    // Make an async generator over the data chunks
    // Map WriteCallback over all data chunks
    auto data_chunk_gen = MakeVectorGenerator(std::move(result.partition->data_chunks_));
    auto copy_and_write_gen = MakeMappedGenerator(
      std::move(data_chunk_gen),
      WriteCallback{std::move(column), itp, buffer});

    // Collect the write results
    auto collect = CollectAsyncGenerator(std::move(copy_and_write_gen));
    return collect.Then([](const std::vector<bool> & result) -> Future<bool> {
      for(auto b: result) { if(!b) return Status::Invalid("Write failed"); }
      return true;
    }, {}, CallbackOptions{ShouldSchedule::Always, GetCpuThreadPool()});
  });

  return write_fut;
}

}  // namespace detail
}  // namespace arcae
