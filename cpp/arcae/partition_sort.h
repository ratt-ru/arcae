#ifndef ARCAE_GROUP_SORT_H
#define ARCAE_GROUP_SORT_H

#include <cstdint>
#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

namespace arcae {

// Structure for storing data to sort the rows of a Measurement Set
// partition by a number of grouping columns (e.g. DATA_DESC_ID, FIELD_ID),
// as well as TIME, ANTENNA1 and ANTENNA2.
//
// The sorting functionality could be provided by an arrow Table.
// However, arrow's Table merging functionality is not publicly exposed,
// so PartitionSortData is used to provide a structure that can be
// used in a k-way merge.
struct PartitionSortData {
  using GroupsType = std::vector<std::shared_ptr<arrow::Int32Array>>;
  GroupsType groups_;
  std::shared_ptr<arrow::DoubleArray> time_;
  std::shared_ptr<arrow::Int32Array> ant1_;
  std::shared_ptr<arrow::Int32Array> ant2_;
  std::shared_ptr<arrow::DoubleArray> interval_;
  std::shared_ptr<arrow::Int64Array> rows_;

  inline std::int32_t group(std::size_t group, std::size_t row) const {
    return groups_[group]->raw_values()[row];
  }
  inline double time(std::size_t row) const { return time_->raw_values()[row]; }
  inline double interval(std::size_t row) const { return interval_->raw_values()[row]; }
  inline std::int32_t ant1(std::size_t row) const { return ant1_->raw_values()[row]; }
  inline std::int32_t ant2(std::size_t row) const { return ant2_->raw_values()[row]; }
  inline std::int64_t rows(std::size_t row) const { return rows_->raw_values()[row]; }

  // Create the PartitionSortata from grouping and sorting arrays
  static arrow::Result<std::shared_ptr<PartitionSortData>> Make(
      const std::vector<std::shared_ptr<arrow::Array>>& groups,
      const std::shared_ptr<arrow::Array>& time,
      const std::shared_ptr<arrow::Array>& ant1,
      const std::shared_ptr<arrow::Array>& ant2,
      const std::shared_ptr<arrow::Array>& interval,
      const std::shared_ptr<arrow::Array>& rows);

  // Convert to an Arrow Table
  std::shared_ptr<arrow::Table> ToTable() const;

  // Number of group columns
  std::size_t nGroups() const { return groups_.size(); }

  // Number of rows in the group
  std::int64_t nRows() const { return rows_->length(); }

  // Sort the Partition in the following order (ascending)
  // 1. Each GROUP column
  // 2. TIME
  // 3. ANTENNA1
  // 4. ANTENNA2
  arrow::Result<std::shared_ptr<PartitionSortData>> Sort() const;
};

// Do a k-way merge of the given partitions
// which should have been sorted by a called to PartitionSortData::Sort()
arrow::Result<std::shared_ptr<PartitionSortData>> MergePartitions(
    const std::vector<std::shared_ptr<PartitionSortData>>& partitions);

}  // namespace arcae

#endif  // ARCAE_GROUP_SORT_H
