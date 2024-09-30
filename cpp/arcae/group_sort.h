#ifndef ARCAE_GROUP_SORT_H
#define ARCAE_GROUP_SORT_H

#include <cstdint>
#include <memory>
#include <vector>

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>

namespace arcae {

struct GroupSortData {
  using GroupsType = std::vector<std::shared_ptr<arrow::Int32Array>>;
  GroupsType groups_;
  std::shared_ptr<arrow::DoubleArray> time_;
  std::shared_ptr<arrow::Int32Array> ant1_;
  std::shared_ptr<arrow::Int32Array> ant2_;
  std::shared_ptr<arrow::Int64Array> rows_;

  inline std::int32_t group(std::size_t group, std::size_t row) const {
    return groups_[group]->raw_values()[row];
  }
  inline double time(std::size_t row) const { return time_->raw_values()[row]; }
  inline std::int32_t ant1(std::size_t row) const { return ant1_->raw_values()[row]; }
  inline std::int32_t ant2(std::size_t row) const { return ant2_->raw_values()[row]; }
  inline std::int64_t rows(std::size_t row) const { return rows_->raw_values()[row]; }

  // Create the GroupSortData from grouping and sorting arrays
  static arrow::Result<std::shared_ptr<GroupSortData>> Make(
      const std::vector<std::shared_ptr<arrow::Array>>& groups,
      const std::shared_ptr<arrow::Array>& time,
      const std::shared_ptr<arrow::Array>& ant1,
      const std::shared_ptr<arrow::Array>& ant2,
      const std::shared_ptr<arrow::Array>& rows);

  // Convert to an Arrow Table
  std::shared_ptr<arrow::Table> ToTable() const;

  // Number of group columns
  std::size_t nGroups() const { return groups_.size(); }

  // Number of rows in the group
  std::int64_t nRows() const { return rows_->length(); }

  // Sort the Group
  arrow::Result<std::shared_ptr<GroupSortData>> Sort() const;
};

arrow::Result<std::shared_ptr<GroupSortData>> MergeGroups(
    const std::vector<std::shared_ptr<GroupSortData>>& group_data);

}  // namespace arcae

#endif  // ARCAE_GROUP_SORT_H
