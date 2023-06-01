#ifndef CASA_ARROW_COLUMN_CONVERT_VISITOR_H
#define CASA_ARROW_COLUMN_CONVERT_VISITOR_H

#include <algorithm>

#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "casa_visitors.h"
#include "complex_type.h"

using ::arrow::Buffer;
using ::arrow::DataType;
using ::arrow::Status;

using ::casacore::IPosition;
using ::casacore::Slice;

class ColumnConvertVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<IPosition>;

public:
    const casacore::TableColumn & column;
    casacore::uInt startrow;
    casacore::uInt nrow;
    casacore::uInt endrow;
    const casacore::ColumnDesc & column_desc;
    std::shared_ptr<arrow::Array> array;
    arrow::MemoryPool * pool;

public:
    explicit ColumnConvertVisitor(
        const casacore::TableColumn & column,
        casacore::uInt startrow,
        casacore::uInt nrow,
        arrow::MemoryPool * pool=arrow::default_memory_pool());
    virtual ~ColumnConvertVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

private:
    inline casacore::uInt local_row(casacore::uInt row)
        { return row - startrow; }

    inline Status FailIfNotUTF8(const std::shared_ptr<DataType> & arrow_dtype) {
        if(arrow_dtype == arrow::utf8()) { return Status::OK(); }
        return Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
    }

    std::shared_ptr<arrow::Array> MakeArrowPrimitiveArray(
            const std::shared_ptr<Buffer> & buffer,
            casacore::uInt nelements,
            const std::shared_ptr<DataType> & arrow_dtype) {

        if(auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype)) {
            auto child_array = arrow::PrimitiveArray(complex_dtype->value_type(),
                                                     2*nelements, buffer, nullptr, 0, 0);
            // NOTE(sjperkins)
            // Check the FixedSizeListAray layout documents
            // https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout
            // A single empty buffer {nullptr} must be provided otherwise this segfaults
            auto array_data = arrow::ArrayData::Make(
                complex_dtype, nelements, {nullptr}, {child_array.data()},
                0, 0);

            return complex_dtype->MakeArray(std::move(array_data));

        } else {
            return std::make_shared<arrow::PrimitiveArray>(
                arrow_dtype, nelements, buffer, nullptr, 0, 0);
        }
    }

    template <typename T>
    Status ConvertScalarColumn(const std::shared_ptr<DataType> & arrow_dtype) {
        auto column = casacore::ScalarColumn<T>(this->column);

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            try {
                auto strings = column.getColumnRange(Slice(startrow, nrow));
                for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(s)); }
            } catch(std::exception & e) {
                return Status::Invalid("ConvertScalarColumn ", column_desc.name(), " ", e.what());
            }

            ARROW_ASSIGN_OR_RAISE(this->array, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Vector
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nrow*sizeof(T), pool));
            auto buffer = std::shared_ptr<Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_vector = casacore::Vector<T>(IPosition(1, nrow), buf_ptr, casacore::SHARE);

            // Dump column data into Arrow Buffer
            try {
                column.getColumnRange(Slice(startrow, nrow), casa_vector);
            } catch(std::exception & e) {
                return Status::Invalid("ConvertScalarColumn ", column_desc.name(), " ", e.what());
            }

            this->array = MakeArrowPrimitiveArray(buffer, nrow, arrow_dtype);
        }

        return this->array->ValidateFull();
    }

    template <typename T>
    Status ConvertFixedArrayColumn(const std::shared_ptr<DataType> & arrow_dtype,
                                   const IPosition & shape) {
        auto column = casacore::ArrayColumn<T>(this->column);

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
           ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
           arrow::StringBuilder builder;

            for(casacore::uInt row=startrow; row < endrow; ++row) {
                auto strings = column.get(row);
                for(auto & string: strings)
                    { ARROW_RETURN_NOT_OK(builder.Append(string)); }
            }
            ARROW_ASSIGN_OR_RAISE(this->array, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto array_shape = shape;
            array_shape.append(IPosition(1, nrow));
            auto nelements = array_shape.product();
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool));
            auto buffer = std::shared_ptr<Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_array = casacore::Array<T>(array_shape, buf_ptr, casacore::SHARE);

            // Dump column data into Arrow Buffer
            try {
                column.getColumnRange(Slice(startrow, nrow), casa_array);
            } catch(std::exception & e) {
                return Status::Invalid("MakeFixedArrayBuffer ", column_desc.name(), " ", e.what());
            }

            this->array = MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype);
        }

        // Fortran ordering
        for(auto dim_size: shape) {
            ARROW_ASSIGN_OR_RAISE(this->array, arrow::FixedSizeListArray::FromArrays(this->array, dim_size));
        }

        return this->array->ValidateFull();
    }

    template <typename T>
    Status ConvertVariableArrayColumn(
            const std::shared_ptr<DataType> & arrow_dtype,
            const std::vector<IPosition> & shapes,
            std::shared_ptr<Buffer> & nulls,
            int64_t null_counts) {

        auto column = casacore::ArrayColumn<T>(this->column);
        auto nelements = std::accumulate(
                shapes.begin(), shapes.end(), casacore::uInt(0),
                [](auto i, const auto & s) { return i + s.product(); });

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(casacore::uInt row=startrow; row < endrow; ++row) {
                if(arrow::bit_util::GetBit(nulls->data(), local_row(row))) {
                    auto strings = column.get(row);
                    for(auto & string: strings) { ARROW_RETURN_NOT_OK(builder.Append(string)); }
                }
            }
            ARROW_ASSIGN_OR_RAISE(this->array, builder.Finish());
        } else {
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool));
            auto buffer = std::shared_ptr<Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            casacore::uInt offset = 0;

            // Dump column data into Arrow Buffer
            for(casacore::uInt row=startrow; row < endrow; ++row) {
                auto lrow = local_row(row);
                auto product = shapes[lrow].product();
                if(arrow::bit_util::GetBit(nulls->data(), lrow) && product > 0) {
                    auto casa_array = casacore::Array<T>(shapes[lrow], buf_ptr + offset, casacore::SHARE);
                    try {
                        column.get(row, casa_array);
                    } catch(std::exception & e) {
                        return Status::Invalid("ConvertVariableArrayColumn",
                                                      column_desc.name(), " ", e.what());
                    }

                    offset += product;
                }
            }

            if(offset != nelements) {
                return Status::Invalid("offsets != nelements "
                                       "during conversion of variably shaped column ",
                                       column_desc.name());
            }

            this->array = MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype);
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


        auto ndim = column_desc.ndim();

        // Compute the products
        auto products = std::vector<IPosition>(nrow, IPosition(column_desc.ndim(), 1));

        for(casacore::uInt row=startrow; row < endrow; ++row) {
            auto lrow = local_row(row);
            const auto & shape = shapes[lrow];
            auto & product = products[lrow];

            for(ssize_t d=ndim - 2; d >= 0; --d) {
                product[d] *= product[d + 1] * shape[d + 1];
            }
        }

        for(int d=0; d < ndim; ++d) {
            // Precompute size of offsets for offset buffer allocation
            unsigned int noffsets = std::accumulate(products.begin(), products.end(), 1,
                [d](auto i, const auto & p) { return i + p[d]; });

            ARROW_ASSIGN_OR_RAISE(
                auto offset_buffer,
                arrow::AllocateBuffer(noffsets*sizeof(noffsets), pool));

            int32_t * optr = reinterpret_cast<int32_t *>(offset_buffer->mutable_data());
            unsigned int running_offset = 0;
            unsigned int o = 0;
            optr[o++] = running_offset;

            for(casacore::uInt row=startrow; row < endrow; ++row) {
                auto repeats = products[local_row(row)][d];
                auto dim_size = shapes[local_row(row)][d];

                for(auto r=0; r < repeats; ++r) {
                    running_offset += dim_size;
                    optr[o++] = running_offset;
                }
            }

            if(o != noffsets) {
                return Status::Invalid("o != noffsets "
                                       "during conversion of variably shaped column ",
                                       column_desc.name());
            }

            auto offsets = std::make_shared<arrow::PrimitiveArray>(
                arrow::int32(), noffsets, std::move(offset_buffer));
            ARROW_ASSIGN_OR_RAISE(this->array, arrow::ListArray::FromArrays(*offsets, *this->array));
            // NOTE(sjperkins): Perhaps remove this for performance
            ARROW_RETURN_NOT_OK(this->array->ValidateFull());
        }

        if(auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(this->array)) {
            // NOTE(sjperkins)
            // Directly adding nulls to the underlying list_array->data()
            // doesn't seem to work, recreate the array with nulls and null_count
            ARROW_ASSIGN_OR_RAISE(this->array, arrow::ListArray::FromArrays(
                *list_array->offsets(),
                *list_array->values(),
                pool,
                nulls,
                null_counts));

            return this->array->ValidateFull();
        } else {
            return Status::Invalid("Unable to cast final array to arrow::ListArray");
        }
    }

    template <typename T>
    Status CheckByteWidths(const std::shared_ptr<DataType> & arrow_dtype) {
        // Check that the arrow type byte widths match up with the casacore byte widths
        if(auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype)) {
            auto vdt = complex_dtype->value_type();
            if(vdt->byte_width() == -1 || 2*vdt->byte_width() != sizeof(T)) {
                return Status::Invalid(
                    "2 x byte width of complex value type",
                    vdt->ToString(), " (",
                    2*vdt->byte_width(),
                    ") != sizeof(T) (", sizeof(T), ")");
            }
        } else if(arrow_dtype == arrow::utf8()) {
            return Status::OK();
        } else if(arrow_dtype->byte_width() == -1 || arrow_dtype->byte_width() != sizeof(T)) {
            return Status::Invalid(
                arrow_dtype->ToString(), " byte width (",
                arrow_dtype->byte_width(),
                ") != sizeof(T) (", sizeof(T), ")");
        }

        return Status::OK();
    }

    template <typename T>
    Status ConvertColumn(const std::shared_ptr<DataType> & arrow_dtype) {
        ARROW_RETURN_NOT_OK(CheckByteWidths<T>(arrow_dtype));

        if(column_desc.ndim() == -1) {
            return Status::NotImplemented(
                column_desc.name(), " has unconstrained dimensionality");
        }

        if(column_desc.isScalar()) {  // ndim == 0
            return ConvertScalarColumn<T>(arrow_dtype);
        }

        assert(column_desc.ndim() >= 1);

        if(column_desc.isFixedShape()) {
            return ConvertFixedArrayColumn<T>(arrow_dtype, column_desc.shape());
        }

        // Variably shaped, read in shapes and null values
        auto shapes = std::vector<IPosition>(nrow, IPosition(column_desc.ndim(), 0));
        auto column = casacore::ArrayColumn<T>(this->column);
        bool shapes_equal = true;
        int64_t null_counts = 0;
        ARROW_ASSIGN_OR_RAISE(auto nulls, arrow::AllocateBitmap(nrow, pool));

        for(casacore::uInt row = startrow; row < endrow; ++row) {
            auto lrow = local_row(row);
            auto is_defined = column.isDefined(row);
            arrow::bit_util::SetBitTo(nulls->mutable_data(), lrow, is_defined);
            null_counts += is_defined ? 0 : 1;
            if(is_defined)
                { shapes[lrow] = column.shape(row); }
            if(shapes_equal)
                { shapes_equal = (shapes[lrow] == shapes[0]); }
        }

        // No nulls and all shapes are equal, we can represent this with a
        // fixed shape list
        if(null_counts == 0 && shapes_equal) {
            return ConvertFixedArrayColumn<T>(arrow_dtype, shapes[0]);
        }

        return ConvertVariableArrayColumn<T>(arrow_dtype, shapes, nulls, null_counts);
    }
};


#endif
