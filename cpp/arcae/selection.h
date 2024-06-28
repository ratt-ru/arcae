#ifndef ARCAE_SELECTION_H
#define ARCAE_SELECTION_H

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <type_traits>
#include <vector>

#include <absl/types/span.h>

#include <arrow/status.h>
#include <arrow/result.h>

namespace arcae {
namespace detail {

// The type of each index value
using IndexType = std::int64_t;
// A vector of indices
using Index = std::vector<IndexType>;
// A span over a 1D contiguous sequence of Indexes
using IndexSpan = absl::Span<const IndexType>;

// Helper class for use with static_assert
template <class...> constexpr std::false_type always_false{};

// Forward Declaration
struct SelectionBuilder;

// Class describing a FORTRAN-ordered selection
// over multiple dimensions
class Selection {
public:
  // Rule of 5
  Selection() = default;
  Selection(const Selection & rhs) = default;
  Selection(Selection && rhs) = default;
  Selection & operator=(const Selection & rhs) = default;
  Selection & operator=(Selection && rhs) = default;

  // Number of dimensions in the selection
  std::size_t Size() const { return spans_.size(); }

  // Number of stored indices in the selection
  std::size_t nIndices() const { return indices_.size(); }

  // Returns true if a selection exists at
  // the specified dimension
  bool IsValid(std::size_t dim) const {
    return dim < Size() && !spans_[dim].empty();
  }

  // Return true if
  explicit inline operator bool() const { return Size() > 0; }

  // Return the selection indices for a specified dimension
  const IndexSpan & operator[](std::size_t dim) const {
    assert(dim < Size());
    return spans_[dim];
  }

  // Return true if a valid row span exists
  bool HasRowSpan() const { return Size() > 0 && !spans_[Size() - 1].empty(); }

  // Return the selection indices for the row dimension
  const IndexSpan & GetRowSpan() const {
    assert(Size() > 0);
    return spans_[Size() - 1];
  }

  // Return the Span referenced by the C-ORDERED index
  arrow::Result<IndexSpan> CSpan(std::size_t cdim) const {
    return CSpan(cdim, Size());
  }

  // Return the Span referenced by the C-ORDERED index,
  // given the total number of dimensions
  arrow::Result<IndexSpan> CSpan(std::size_t cdim, std::size_t ndim) const {
    auto fdim = std::ptrdiff_t(cdim) +
                std::ptrdiff_t(Size()) -
                std::ptrdiff_t(ndim);
    if(fdim >= 0 && fdim < std::ptrdiff_t(Size())) return spans_[fdim];
    return arrow::Status::IndexError("Selection doesn't exist");
  }

private:
  friend SelectionBuilder;

  Selection(std::vector<Index> && indices, std::vector<IndexSpan> && spans) :
    indices_(std::move(indices)), spans_(std::move(spans)) {}

  std::vector<Index> indices_;
  std::vector<IndexSpan> spans_;
};

// Defines whether the SelectionBuilder describes
// a C ordered or FORTRAN ordered selection.
enum SelectionOrder { C_ORDER, F_ORDER };

// Builds a Selection object
struct SelectionBuilder {
  std::vector<Index> indices;
  std::vector<IndexSpan> spans;
  SelectionOrder order = SelectionOrder::C_ORDER;

  // Build the selection
  Selection Build() {
    if(order == SelectionOrder::C_ORDER) {
      std::reverse(std::begin(spans), std::end(spans));
    }
    return Selection{std::move(indices), std::move(spans)};
  }

  // ProcessArgs base case
  SelectionBuilder & ProcessArgs() { return *this; }

  // Process head of the argument pack
  template <typename T, typename... Args>
  SelectionBuilder & ProcessArgs(T && head, Args && ... args) {
    Add(std::forward<T>(head));
    return ProcessArgs(std::forward<Args>(args)...);
  }

  // Create a Selection from an Argument Pack
  template <typename... Args>
  static Selection FromArgs(Args && ... args) {
    SelectionBuilder builder;
    builder.ProcessArgs(std::forward<Args>(args)...);
    return builder.Build();
  }

  // Create a Selection from a series of initializer_list<T>
  template <typename T>
  static Selection FromInit(std::initializer_list<std::initializer_list<T>> init_list) {
    SelectionBuilder builder;
    for(const auto & il: init_list) builder.Add(il);
    return builder.Build();
  }

  // Sets the ordering of the resultant selection to C or FORTRAN ordered
  SelectionBuilder & Order(char order) {
    this->order = order == 'F' ? SelectionOrder::F_ORDER : SelectionOrder::C_ORDER;
    return *this;
  }

  // Base case, which reminds the developer of the vector<T> expectation
  template <typename T>
  SelectionBuilder & Add(T ids) {
    static_assert(always_false<T>, "ids must be a vector of integral types");
    return *this;
  }

  // Create a span over an existing index vector
  // The vector will not be copied into the Selection
  // if the vector type is an IndexType
  template <typename T>
  SelectionBuilder & Add(const std::vector<T> & ids) {
    if constexpr(std::is_same_v<T, IndexType>) {
      spans.emplace_back(IndexSpan(ids));
    } else if constexpr(std::is_integral_v<T>) {
      indices.emplace_back(Index(std::begin(ids), std::end(ids)));
      spans.emplace_back(IndexSpan(indices.back()));
    } else {
      static_assert(always_false<T>, "ids must contain integral types");
    }
    return *this;
  }

  // Creates a span over the supplied index vector,
  // which is included in the encapsulated Selection indices
  template <typename T>
  SelectionBuilder & Add(std::vector<T> && ids) {
    if constexpr(std::is_same_v<T, IndexType>) {
      indices.emplace_back(std::move(ids));
      spans.emplace_back(IndexSpan(indices.back()));
    } else if constexpr(std::is_integral_v<T>) {
      indices.emplace_back(Index(std::begin(ids), std::end(ids)));
      spans.emplace_back(IndexSpan(indices.back()));
    } else {
      static_assert(always_false<T>, "ids must contain integral types");
    }
    return *this;
  }

  // Inludes a copy of the initializer_list in the
  // encapsulated Selection indices and then creates
  // a span over the copy
  template <typename T>
  SelectionBuilder & Add(const std::initializer_list<T> & ids) {
    if constexpr(std::is_same_v<T, IndexType> || std::is_integral_v<T>) {
      return Add(Index(std::begin(ids), std::end(ids)));
    } else {
      static_assert(always_false<T>, "ids must contain integral types");
    }
    return *this;
  }
};

} // namespace detail
} // namespace arcae

#endif // #define ARCAE_SELECTION_H