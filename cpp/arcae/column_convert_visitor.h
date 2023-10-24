#ifndef ARCAE_COLUMN_CONVERT_VISITOR_H
#define ARCAE_COLUMN_CONVERT_VISITOR_H

#include <algorithm>
#include <unordered_set>

#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "arcae/casa_visitors.h"
#include "arcae/complex_type.h"
#include "arcae/service_locator.h"
#include "arcae/utility.h"

namespace arcae {

class ColumnConvertVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    const casacore::TableColumn & column_;
    const casacore::ColumnDesc & column_desc_;
    casacore::uInt startrow_;
    casacore::uInt endrow_;
    casacore::uInt nrow_;
    std::shared_ptr<arrow::Array> array_;
    arrow::MemoryPool * pool_;

public:
    explicit ColumnConvertVisitor(
        const casacore::TableColumn & column,
        casacore::uInt startrow,
        casacore::uInt nrow,
        arrow::MemoryPool * pool=arrow::default_memory_pool());
    virtual ~ColumnConvertVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

private:

    static arrow::Status ValidateArray(const std::shared_ptr<arrow::Array> & array);

    inline casacore::uInt local_row(casacore::uInt row)
        { assert(row >= startrow_); return row - startrow_; }

    inline arrow::Status FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        if(arrow_dtype == arrow::utf8()) { return arrow::Status::OK(); }
        return arrow::Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
    }

    arrow::Result<std::shared_ptr<arrow::Array>> MakeArrowPrimitiveArray(
            const std::shared_ptr<arrow::Buffer> & buffer,
            casacore::uInt nelements,
            const std::shared_ptr<arrow::DataType> & arrow_dtype);

    template <typename T>
    arrow::Status ConvertScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ScalarColumn<T>(column_);
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            try {
                auto strings = column.getColumnRange(casacore::Slice(startrow_, nrow_));
                for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(s)); }
            } catch(std::exception & e) {
                return arrow::Status::Invalid("ConvertScalarColumn ", column_desc_.name(), " ", e.what());
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Vector
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nrow_*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_vector = casacore::Vector<T>(casacore::IPosition(1, nrow_), buf_ptr, casacore::SHARE);

            // Dump column data into Arrow Buffer
            try {
                column.getColumnRange(casacore::Slice(startrow_, nrow_), casa_vector);
            } catch(std::exception & e) {
                return arrow::Status::Invalid("ConvertScalarColumn ", column_desc_.name(), " ", e.what());
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nrow_, arrow_dtype));
        }

        return ValidateArray(array_);
    }

    template <typename T>
    arrow::Status ConvertFixedArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype,
                                   const casacore::IPosition & shape) {
        auto column = casacore::ArrayColumn<T>(column_);
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
           ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
           arrow::StringBuilder builder;

            for(casacore::uInt row=startrow_; row < endrow_; ++row) {
                auto strings = column.get(row);
                for(auto & string: strings)
                    { ARROW_RETURN_NOT_OK(builder.Append(string)); }
            }
            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto array_shape = shape;
            array_shape.append(casacore::IPosition(1, nrow_));
            auto nelements = array_shape.product();
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_array = casacore::Array<T>(array_shape, buf_ptr, casacore::SHARE);

            // Dump column data into Arrow Buffer
            try {
                column.getColumnRange(casacore::Slice(startrow_, nrow_), casa_array);
            } catch(std::exception & e) {
                return arrow::Status::Invalid("MakeFixedArrayBuffer ", column_desc_.name(), " ", e.what());
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        // Fortran ordering
        for(auto dim_size: shape) {
            ARROW_ASSIGN_OR_RAISE(array_, arrow::FixedSizeListArray::FromArrays(array_, dim_size));
        }

        return ValidateArray(array_);
    }

    template <typename T>
    arrow::Status ConvertVariableArrayColumn(
            const std::shared_ptr<arrow::DataType> & arrow_dtype,
            const std::vector<casacore::IPosition> & shapes,
            std::shared_ptr<arrow::Buffer> & nulls,
            int64_t null_counts,
            casacore::uInt ndim) {

        auto column = casacore::ArrayColumn<T>(column_);
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(casacore::uInt row=startrow_; row < endrow_; ++row) {
                if(arrow::bit_util::GetBit(nulls->data(), local_row(row))) {
                    auto strings = column.get(row);
                    for(auto & string: strings) { ARROW_RETURN_NOT_OK(builder.Append(string)); }
                }
            }
            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            auto nelements = std::accumulate(
                    shapes.begin(), shapes.end(), casacore::uInt(0),
                    [](auto i, const auto & s) { return i + s.product(); });
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            casacore::uInt offset = 0;

            // Dump column data into Arrow Buffer
            for(casacore::uInt row=startrow_; row < endrow_; ++row) {
                auto lrow = local_row(row);
                auto product = shapes[lrow].product();
                if(arrow::bit_util::GetBit(nulls->data(), lrow) && product > 0) {
                    auto casa_array = casacore::Array<T>(shapes[lrow], buf_ptr + offset, casacore::SHARE);
                    try {
                        column.get(row, casa_array);
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("ConvertVariableArrayColumn",
                                                      column_desc_.name(), " ", e.what());
                    }

                    offset += product;
                }
            }

            if(offset != nelements) {
                return arrow::Status::Invalid("offsets != nelements "
                                       "during conversion of variably shaped column ",
                                       column_desc_.name());
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        // NOTE(sjperkins)
        // See https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
        // At this stage we have a flat array of values that we wish to arrange
        // into a nested structure. It needs to be built from the fastest changing
        // dimension towards the slowest changing dimension. This is accomplished by
        // creating offsets for each nested layer. The dimension size at each layer
        // must be repeated by the product of the previous elements in the shape.

        // See this worked case
        //    Here, we use casacore's FORTRAN ordering for shapes
        //    repeats = [reduce(mul, s[d+1:], 1) for s, d in enumerate(shapes_)]
        //    dim_sizes = [s[d] for d, s in enumerate(shapes)]

        //    shapes = [(2, 5, 10), (4, 3, 10)]
        //    products = [(50, 10, 1), (30, 10, 1)]
        //    For d == 0
        //    repeats = [50, 30]
        //    dim_sizes = [2, 4]
        //    offsets = [0] + cumsum(50*[2] + 30*[4])
        //    For d == 1
        //    repeats = [10, 10]
        //    dim_sizes = [5, 3]
        //    offsets = [0] + cumsum(10*[5] + 10+[3])
        //    For d == 2
        //    repeats = [1, 1]
        //    dim_sizes = [10, 10]
        //    offsets = [0] + cumsum((1*[10] + 1*[10]))



        // Compute the products
        auto products = std::vector<casacore::IPosition>(nrow_, casacore::IPosition(ndim, 1));

        for(casacore::uInt row=startrow_; row < endrow_; ++row) {
            auto lrow = local_row(row);
            const auto & shape = shapes[lrow];
            auto & product = products[lrow];

            for(int d=ndim - 2; d >= 0; --d) {
                product[d] *= product[d + 1] * shape[d + 1];
            }
        }

        for(int d=0; d < ndim; ++d) {
            // Precompute size of offsets for offset buffer allocation
            unsigned int noffsets = std::accumulate(products.begin(), products.end(), 1,
                [d](auto i, const auto & p) { return i + p[d]; });

            arrow::Int32Builder builder(pool_);
            unsigned int running_offset = 0;
            unsigned int o = 1;
            ARROW_RETURN_NOT_OK(builder.Reserve(noffsets));
            ARROW_RETURN_NOT_OK(builder.Append(running_offset));

            for(casacore::uInt row=startrow_; row < endrow_; ++row) {
                auto repeats = products[local_row(row)][d];
                auto dim_size = shapes[local_row(row)][d];

                for(auto r=0; r < repeats; ++r, ++o) {
                    running_offset += dim_size;
                    ARROW_RETURN_NOT_OK(builder.Append(running_offset));
                }
            }

            if(o != noffsets) { return arrow::Status::Invalid("o != noffsets"); }

            ARROW_ASSIGN_OR_RAISE(auto offsets, builder.Finish());
            ARROW_ASSIGN_OR_RAISE(array_, arrow::ListArray::FromArrays(*offsets, *array_));
            // NOTE(sjperkins): Perhaps remove this for performance
            ARROW_RETURN_NOT_OK(ValidateArray(array_));
        }

        if(auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(array_)) {
            // NOTE(sjperkins)
            // Directly adding nulls to the underlying list_array->data()
            // doesn't seem to work, recreate the array with nulls and null_count
            ARROW_ASSIGN_OR_RAISE(array_, arrow::ListArray::FromArrays(
                *list_array->offsets(),
                *list_array->values(),
                pool_,
                nulls,
                null_counts));

            return ValidateArray(array_);
        } else {
            return arrow::Status::Invalid("Unable to cast final array to arrow::ListArray");
        }
    }

    template <typename T>
    arrow::Status CheckByteWidths(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        // Check that the arrow type byte widths match up with the casacore byte widths
        if(auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype)) {
            auto vdt = complex_dtype->value_type();
            if(vdt->byte_width() == -1 || 2*vdt->byte_width() != sizeof(T)) {
                return arrow::Status::Invalid(
                    "2 x byte width of complex value type",
                    vdt->ToString(), " (",
                    2*vdt->byte_width(),
                    ") != sizeof(T) (", sizeof(T), ")");
            }
        } else if(arrow_dtype == arrow::utf8()) {
            return arrow::Status::OK();
        } else if(arrow_dtype->byte_width() == -1 || arrow_dtype->byte_width() != sizeof(T)) {
            return arrow::Status::Invalid(
                arrow_dtype->ToString(), " byte width (",
                arrow_dtype->byte_width(),
                ") != sizeof(T) (", sizeof(T), ")");
        }

        return arrow::Status::OK();
    }

    template <typename T>
    arrow::Status ConvertColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        ARROW_RETURN_NOT_OK(CheckByteWidths<T>(arrow_dtype));

        if(nrow_ == 0) {
            return arrow::Status::Invalid("Zero-length row request");
        }

        if(column_desc_.isScalar()) {  // ndim == 0
            return ConvertScalarColumn<T>(arrow_dtype);
        }

        auto & config = ServiceLocator::configuration();
        auto convert_method = config.GetDefault("casa.convert.strategy", "fixed");
        auto list_convert = convert_method.find("list") == 0;

        if(!list_convert && column_desc_.isFixedShape()) {
            return ConvertFixedArrayColumn<T>(arrow_dtype, column_desc_.shape());
        }

        // Variably shaped, read in shapes and null values
        auto shapes = std::vector<casacore::IPosition>(nrow_, casacore::IPosition());
        auto shapes_set = std::unordered_set<casacore::IPosition>{};
        auto column = casacore::ArrayColumn<T>(column_);
        int64_t null_counts = 0;
        ARROW_ASSIGN_OR_RAISE(auto nulls, arrow::AllocateBitmap(nrow_, pool_));

        for(casacore::uInt row = startrow_; row < endrow_; ++row) {
            auto lrow = local_row(row);
            auto is_defined = column.isDefined(row);
            arrow::bit_util::SetBitTo(nulls->mutable_data(), lrow, is_defined);
            null_counts += is_defined ? 0 : 1;

            if(is_defined) {
                shapes[lrow] = column.shape(row);
                shapes_set.insert(shapes[lrow]);
            }
        }

        auto [shapes_equal, ndim_equal] = [&]() -> std::tuple<bool, bool> {
            bool ndim_equal = true;
            bool shapes_equal = true;

            if(shapes_set.size() <= 1) {
                return {shapes_equal, ndim_equal};
            }

            for(auto it = std::begin(shapes_set); it != std::end(shapes_set); ++it) {
                if(it->size() != std::begin(shapes_set)->size()) {
                    ndim_equal = ndim_equal && false;
                    shapes_equal = shapes_equal && false;
                    continue;
                }

                if(*it != *std::begin(shapes_set)) {
                    shapes_equal = shapes_equal && false;
                }
            }

            return {shapes_equal, ndim_equal};
        }();

        if(!ndim_equal) {
            return arrow::Status::NotImplemented(
                column_desc_.name(), " has unconstrained dimensionality");
        }

        // No nulls and all shapes are equal, we can represent this with a
        // fixed shape list
        if(!list_convert && null_counts == 0 && shapes_equal) {
            return ConvertFixedArrayColumn<T>(arrow_dtype, shapes[0]);
        }

        return ConvertVariableArrayColumn<T>(
                arrow_dtype,
                shapes,
                nulls,
                null_counts,
                shapes[0].size());
    }
};

} // namespace arcae

#endif
