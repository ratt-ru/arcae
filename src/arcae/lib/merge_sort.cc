#include <cstdint>
#include <numeric>
#include <queue>
#include <vector>

#include <Python.h>

#include <numpy/arrayobject.h>
#include <numpy/ndarraytypes.h>
#include <numpy/npy_common.h>

namespace arcae {

// Expand this with more types as needed
#define VISIT_SORTABLE_TYPES(VISIT)          \
  VISIT(NPY_INT32, std::int32_t)             \
  VISIT(NPY_INT64, std::int64_t)             \
  VISIT(NPY_FLOAT32, float)                  \
  VISIT(NPY_FLOAT64, double)


struct PartitionData {
  std::vector<void *> data_;
  std::vector<int> dtypes_;
  int nrow_;

  std::size_t num_arrays() const { return data_.size(); }
};

static int PartitionMerge(const std::vector<std::vector<PyArrayObject*>> & array_partitions,
                std::vector<PyArrayObject*> * merged_arrays) {
  merged_arrays->clear();

  if(array_partitions.size() == 0 || array_partitions[0].size() == 0) {
    return 0;
  }

  std::vector<PartitionData> partitions(array_partitions.size());

  // Construct the PartitionData structures
  for (std::size_t g = 0; g < array_partitions.size(); ++g) {
    const auto & arrays = array_partitions[g];
    std::vector<void *> data(arrays.size());
    std::vector<int> dtypes(arrays.size());
    int nrow = -1;

    for (int i = 0; i < arrays.size(); ++i) {
      PyArrayObject * array = arrays[i];
      int ndims = PyArray_NDIM(array);
      npy_intp * shape = PyArray_SHAPE(array);
      dtypes[i] = PyArray_TYPE(array);
      data[i] = PyArray_DATA(array);

      if (ndims != 1) {
        PyErr_SetString(PyExc_ValueError, "Array must be 1-dimensional");
        return -1;
      }

      if (i == 0) {
        nrow = shape[0];
      } else if (nrow != shape[0]) {
        PyErr_SetString(PyExc_ValueError, "Array lengths do not match");
        return -1;
      }
    }
    partitions[g] = PartitionData{std::move(data), std::move(dtypes), nrow};
  }

  // Sanity check partition data
  for (std::size_t g = 1; g < partitions.size(); ++g) {
    if (partitions[g].data_.size() != partitions[0].data_.size()) {
      PyErr_SetString(PyExc_ValueError, "Partitions must have the same number of arrays");
      return -1;
    }

    for (std::size_t a = 0; a < partitions[g].data_.size(); ++a) {
      if (partitions[g].dtypes_[a] != partitions[0].dtypes_[a]) {
        PyErr_SetString(PyExc_ValueError, "Array dtypes must match");
        return -1;
      }

      #define VISIT(NPY_TYPE, C_TYPE) \
        case NPY_TYPE:                \
          break;

      switch (partitions[g].dtypes_[a]) {
        VISIT_SORTABLE_TYPES(VISIT)
        default:
          PyErr_SetString(PyExc_ValueError, "Unsupported array type");
          return -1;
      }

      #undef VISIT
    }
  }

  // Allocate output arrays
  std::size_t narrays = partitions[0].num_arrays();
  merged_arrays->resize(narrays);
  std::vector<void *> out_data(narrays);
  int output_nrow = std::accumulate(std::begin(partitions), std::end(partitions), 0,
                    [](auto i, auto p) { return i + p.nrow_; });

  for (std::size_t a = 0; a < narrays; ++a) {
    auto dtype = partitions[0].dtypes_[a];
    npy_intp out_shape[1] = {output_nrow};
    PyArrayObject * out_array = (PyArrayObject *) PyArray_SimpleNew(1, out_shape, dtype);
    if (out_array == nullptr) {
      PyErr_SetString(PyExc_ValueError, "Could not allocate output array");
      return {};
    }
    (*merged_arrays)[a] = out_array;
    out_data[a] = PyArray_DATA(out_array);
  }

  // Drop the global interpreter lock while merging
  Py_BEGIN_ALLOW_THREADS

  // Priority queue element, pointing at a partition
  // and a row within that partition
  struct MergeData {
    std::size_t row;
    PartitionData * partition;

    bool operator<(const MergeData & rhs) const {
      for (std::size_t a = 0; a < rhs.partition->num_arrays(); ++a) {
        #define VISIT(NPY_TYPE, C_TYPE)                                          \
          case NPY_TYPE: {                                                       \
            auto lhs_data = reinterpret_cast<C_TYPE *>(partition->data_[a]);     \
            auto rhs_data = reinterpret_cast<C_TYPE *>(rhs.partition->data_[a]); \
            auto lhs_value = lhs_data[row];                                      \
            auto rhs_value = rhs_data[rhs.row];                                  \
            if (lhs_value != rhs_value) return lhs_value > rhs_value;            \
            break;                                                               \
          }

        switch(partition->dtypes_[a]) {
          VISIT_SORTABLE_TYPES(VISIT)
        }

        #undef VISIT
      }
      return false;
    }
  };

  // Create and initialize a priority queue
  std::priority_queue<MergeData> queue;
  for (std::size_t p = 0; p < partitions.size(); ++p) {
    queue.push(MergeData{0, &partitions[p]});
  }

  // Perform the k-way merge
  for (std::size_t row = 0; !queue.empty(); ++row) {
    auto [prow, top_partition] = queue.top();
    queue.pop();

    for (std::size_t a = 0; a < narrays; ++a) {
      #define VISIT(NPY_TYPE, C_TYPE)                                    \
        case NPY_TYPE: {                                                 \
          auto out = reinterpret_cast<C_TYPE *>(out_data[a]);            \
          auto in = reinterpret_cast<C_TYPE *>(top_partition->data_[a]); \
          out[row] = in[prow];                                           \
          break;                                                         \
        }

      switch(partitions[0].dtypes_[a]) {
        VISIT_SORTABLE_TYPES(VISIT)
      }

      #undef VISIT
    }

    if (prow + 1 < top_partition->nrow_) {
      queue.push(MergeData{prow + 1, top_partition});
    }
  }

  // Release the global interpreter lock
  Py_END_ALLOW_THREADS

  return 0;
}

}  // namespace arcae
