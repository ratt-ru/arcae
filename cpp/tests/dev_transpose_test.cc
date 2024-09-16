
#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <random>

#include <casacore/casa/Arrays/Array.h>
#include <casacore/casa/Arrays/IPosition.h>
#include <casacore/casa/BasicSL/Complexfwd.h>
#include <casacore/ms/MeasurementSets/MeasurementSet.h>
#include <casacore/tables/DataMan/TiledColumnStMan.h>
#include <casacore/tables/Tables.h>
#include <casacore/tables/Tables/ColumnDesc.h>
#include <casacore/tables/Tables/SetupNewTab.h>
#include <casacore/tables/Tables/TableColumn.h>
#include <casacore/tables/Tables/TableLock.h>
#include <casacore/tables/Tables/TableProxy.h>

#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/async_generator.h>
#include <arrow/util/future.h>
#include <arrow/util/logging.h>
#include <arrow/util/thread_pool.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "arcae/data_partition.h"
#include "arcae/isolated_table_proxy.h"
#include "arcae/result_shape.h"
#include "arcae/selection.h"

#include <absl/strings/str_format.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <tests/test_utils.h>

using namespace std::string_literals;

using ::arrow::AsyncGenerator;
using ::arrow::CallbackOptions;
using ::arrow::Future;
using ::arrow::ShouldSchedule;
using ::arrow::internal::ThreadPool;

using ::arcae::GetArrayColumn;
using ::arcae::GetScalarColumn;
using ::arcae::detail::DataChunk;
using ::arcae::detail::DataPartition;
using ::arcae::detail::Index;
using ::arcae::detail::IndexType;
using ::arcae::detail::IsolatedTableProxy;
using ::arcae::detail::ResultShapeData;
using ::arcae::detail::Selection;
using ::arcae::detail::SelectionBuilder;

using MS = ::casacore::MeasurementSet;
using ::casacore::Array;
using ::casacore::ArrayColumnDesc;
using ::casacore::ColumnDesc;
using ::casacore::Complex;
using ::casacore::IPosition;
using ::casacore::Record;
using ::casacore::SetupNewTable;
using ::casacore::Table;
using ::casacore::TableColumn;
using ::casacore::TableDesc;
using ::casacore::TableLock;
using ::casacore::TableProxy;
using ::casacore::TiledColumnStMan;

template <>
struct arrow::IterationTraits<IPosition> {
  static IPosition End() { return {}; }
  static bool IsEnd(const IPosition& val) { return val.size() == 0; }
};

template <typename T>
struct arrow::IterationTraits<casacore::Array<T>> {
  static casacore::Array<T> End() { return {}; }
  static bool IsEnd(const casacore::Array<T>& val) { return val.size() == 0; }
};

template <>
struct arrow::IterationTraits<Complex> {
  static Complex End() { return {-1, -1}; }
  static bool IsEnd(const Complex& val) { return val == Complex{-1, -1}; }
};

namespace {

static constexpr std::size_t knrow = 1000000;
static constexpr std::size_t knchan = 16;
static constexpr std::size_t kncorr = 4;

static constexpr std::size_t knrowtile = 100;

absl::Duration read_shape_duration;
absl::Duration partition_duration;
absl::Duration read_duration;
absl::Duration transpose_duration;

std::int64_t bytes_read = 0;

std::mt19937 kRng = std::mt19937{};

class DevTransposeTest : public ::testing::Test {
 protected:
  std::string table_name_;

  void SetUp() override {
    auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    table_name_ = std::string(test_info->name() + "-"s + arcae::hexuuid(4) + ".table"s);

    auto tile_shape = IPosition({kncorr, knchan, knrowtile});
    auto data_shape = IPosition({kncorr, knchan});

    auto table_desc = TableDesc(MS::requiredTableDesc());
    auto data_column_desc =
        ArrayColumnDesc<Complex>("MODEL_DATA", data_shape, ColumnDesc::FixedShape);
    table_desc.addColumn(data_column_desc);
    auto storage_manager = TiledColumnStMan("TiledModelData", tile_shape);
    auto setup_new_table = SetupNewTable(table_name_, table_desc, Table::New);
    setup_new_table.bindColumn("MODEL_DATA", storage_manager);
    auto ms = MS(setup_new_table, knrow);
    auto data = GetArrayColumn<Complex>(ms, MS::MODEL_DATA);
    data.putColumn(Array<Complex>(IPosition({kncorr, knchan, knrow}), {1, 2}));
  }

  arrow::Result<std::shared_ptr<IsolatedTableProxy>> OpenTable() {
    return IsolatedTableProxy::Make([name = table_name_]() {
      auto lock = TableLock(TableLock::LockOption::AutoLocking);
      auto lockoptions = Record();
      lockoptions.define("option", "nolock");
      lockoptions.define("internal", lock.interval());
      lockoptions.define("maxwait", casacore::Int(lock.maxWait()));
      return std::make_shared<TableProxy>(name, lockoptions, Table::Old);
    });
  }

  void TearDown() override { std::filesystem::remove_all(table_name_); }
};

TEST_F(DevTransposeTest, Basic) {
  auto column = std::string("MODEL_DATA");
  auto cpu_pool = arrow::internal::GetCpuThreadPool();
  ASSERT_OK_AND_ASSIGN(auto itp, OpenTable());
  ASSERT_OK_AND_ASSIGN(auto data_shape,
                       itp->RunSync([column = column](const TableProxy& tp) {
                         auto data = TableColumn(tp.table(), column);
                         auto shape = data.shapeColumn();
                         shape.append(IPosition({ssize_t(data.nrow())}));
                         return shape;
                       }));
  EXPECT_EQ(data_shape, IPosition({kncorr, knchan, knrow}));

  // Build random (but contiguous if ordered) selections on each axis
  auto builder = SelectionBuilder().Order('F');
  for (std::size_t dim = 0; dim < data_shape.size(); ++dim) {
    Index dim_index(data_shape[dim], 0);
    std::iota(std::begin(dim_index), std::end(dim_index), 0);
    // Shuffle the row dimension
    if (dim == data_shape.size() - 1)
      std::shuffle(std::begin(dim_index), std::end(dim_index), kRng);
    builder.Add(std::move(dim_index));
  }
  auto selection = builder.Build();

  auto shape_fut = itp->RunAsync([&, column = column](const TableProxy& tp) {
    std::shared_ptr<void> time_it(nullptr, [start = absl::Now()](...) {
      read_shape_duration += absl::Now() - start;
    });
    auto data = TableColumn(tp.table(), column);
    return ResultShapeData::MakeRead(data, selection);
  });

  auto part_fut = shape_fut.Then(
      [&selection](const ResultShapeData& result) mutable {
        std::shared_ptr<void> time_it(nullptr, [start = absl::Now()](...) {
          partition_duration += absl::Now() - start;
        });
        return DataPartition::Make(selection, result);
      },
      {}, CallbackOptions{ShouldSchedule::Always, cpu_pool});

  auto copy_fut = part_fut.Then(
      [itp = itp, column = column, cpu_pool = cpu_pool](const DataPartition& result) {
        auto data_chunk_gen = arrow::MakeVectorGenerator(std::move(result.data_chunks_));
        auto get_data_gen = arrow::MakeMappedGenerator(
            std::move(data_chunk_gen),
            [itp = itp, column = column, cpu_pool = cpu_pool](
                const DataChunk& chunk) -> arrow::Future<Array<Complex>> {
              if (chunk.nDim() == 0)
                return arrow::Status::Invalid("Zero dimension chunk");
              constexpr std::size_t kMaxDims = 10;
              if (chunk.nDim() >= kMaxDims) {
                return arrow::Status::Invalid("Chunk has dimension > 10 ", kMaxDims);
              }

              auto read_fut = Future<casacore::Array<Complex>>::Make();
              if (chunk.nDim() == 1) {
                read_fut = itp->RunAsync(
                    [row_slicer = chunk.RowSlicer(), column = column](
                        const TableProxy& tp) -> Future<casacore::Array<Complex>> {
                      std::shared_ptr<void> time_it(nullptr, [start = absl::Now()](...) {
                        read_duration += absl::Now() - start;
                      });
                      auto col = GetArrayColumn<Complex>(tp.table(), column);
                      return col.getColumnRange(row_slicer);
                    });
              } else {
                read_fut = itp->RunAsync(
                    [row_slicer = chunk.RowSlicer(),
                     section_slicer = chunk.SectionSlicer(), column = column](
                        const TableProxy& tp) -> Future<casacore::Array<Complex>> {
                      std::shared_ptr<void> time_it(nullptr, [start = absl::Now()](...) {
                        read_duration += absl::Now() - start;
                      });
                      bytes_read += (row_slicer.length().product() *
                                     section_slicer.length().product()) *
                                    sizeof(Complex);
                      auto col = GetArrayColumn<Complex>(tp.table(), column);
                      return col.getColumnRange(row_slicer, section_slicer);
                    });
              }

              return read_fut.Then(
                  [chunk = chunk](
                      const casacore::Array<Complex>& data) mutable -> Array<Complex> {
                    using CT = Complex;
                    std::shared_ptr<void> time_it(nullptr, [start = absl::Now()](...) {
                      transpose_duration += absl::Now() - start;
                    });
                    std::ptrdiff_t ndim = chunk.nDim();
                    std::ptrdiff_t last_dim = ndim - 1;
                    auto spans = chunk.DimensionSpans();
                    auto min_mem = chunk.MinMemIndex();
                    auto chunk_strides = chunk.ChunkStrides();
                    auto buffer_strides = chunk.BufferStrides();
                    const CT* in_ptr = data.data();
                    casacore::Array<Complex> result(data.shape());
                    ;
                    CT* out_ptr = result.data();
                    auto pos = chunk.ScratchPositions();
                    for (std::size_t i = 0; i < pos.size(); ++i) pos[i] = 0;

                    while (true) {  // Iterate over the spans in memory, copying data
                      std::size_t i = 0, o = 0;
                      for (std::ptrdiff_t d = 0; d < ndim; ++d) {
                        i += pos[d] * chunk_strides[d];
                        o += (spans[d].mem[pos[d]] - min_mem[d]) * buffer_strides[d];
                      }
                      // Moves degrade to copies for simple (i.e. numeric) types
                      // but it should make casacore::String more efficient by avoiding
                      // copies
                      out_ptr[o] = std::move(in_ptr[i]);
                      for (std::ptrdiff_t d = 0; d < ndim;
                           ++d) {  // Iterate in FORTRAN order
                        if (++pos[d] < spans[d].mem.size())
                          break;     // Iteration doesn't reach dim end
                        pos[d] = 0;  // OtherwBasic iteration worksise reset, next dim
                        if (d == last_dim)
                          return result;  // The last dim is reset, we're done
                      }
                    }
                  },
                  {}, CallbackOptions{ShouldSchedule::Always, cpu_pool});
            });

        return arrow::CollectAsyncGenerator(get_data_gen);
      },
      {}, CallbackOptions{ShouldSchedule::Always, cpu_pool});

  EXPECT_EQ(copy_fut.result()->size(), 1);
  EXPECT_EQ(copy_fut.result().ValueOrDie()[0].shape(), data_shape);

  absl::Duration sort_duration;

  {
    std::vector<IndexType> indices(knrow, 0);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), kRng);
    auto start = absl::Now();
    std::sort(indices.begin(), indices.end());
    sort_duration = absl::Now() - start;
  }

  ARROW_LOG(INFO) << "Extract shape data in " << read_shape_duration;
  ARROW_LOG(INFO) << "Partition shape in " << partition_duration;
  ARROW_LOG(INFO) << "  Requires a sort of " << knrow << " indices in " << sort_duration;
  ARROW_LOG(INFO) << "Read " << float(bytes_read) / (1024. * 1024.) << "MB of data in "
                  << read_duration;
  ARROW_LOG(INFO) << "Transposed data in " << transpose_duration;

  ARROW_LOG(INFO) << "Total "
                  << read_shape_duration + partition_duration + read_duration +
                         transpose_duration;
}

}  // namespace
