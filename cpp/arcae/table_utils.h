#ifndef ARCAE_TABLE_UTILS_H
#define ARCAE_TABLE_UTILS_H

#include <string>

#include <arrow/api.h>

#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables/TableProxy.h>

namespace arcae {
namespace detail {

// Returns OK if the ColumnExists otherwise returns an error Status
arrow::Status ColumnExists(const casacore::TableProxy& tp, const std::string& column);

// Returns true if this is a primite CASA type
bool IsPrimitiveType(casacore::DataType data_type);

// Returns true if the table was opened readonly and was re-opened in readwrite mode
// otherwise returns false
bool MaybeReopenRW(casacore::TableProxy& tp);

template <typename F>
class Finalizer {
 public:
  Finalizer(F&& f) : finalizer_(std::forward<F>(f)), enabled_(true) {};
  ~Finalizer() {
    if (enabled_) finalizer_();
  };
  void disable() { enabled_ = false; }

 private:
  F finalizer_;
  bool enabled_;
};

template <typename F>
Finalizer<F> finally(F&& f) {
  return Finalizer<F>(std::forward<F>(f));
}

}  // namespace detail
}  // namespace arcae

#endif  // ARCAE_TABLE_UTILS_H
