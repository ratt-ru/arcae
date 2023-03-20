#include <algorithm>

#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "casa_visitors.h"
#include "complex_type.h"


class ColumnConvertVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    const casacore::TableColumn & column;
    casacore::uInt startrow;
    casacore::uInt nrow;
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
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

private:
    template <typename ColumnType, typename DT>
    arrow::Result<std::tuple<
        std::shared_ptr<arrow::Array>,
        std::unique_ptr<ShapeVectorType>>>
    MakeArrowStringArray(ColumnType & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        if(arrow_dtype != arrow::utf8()) {
            return arrow::Status::Invalid(
                arrow_dtype->ToString(),
                "incompatible with casacore::String");
        }

        // Handle string cases with Arrow StringBuilders
        arrow::StringBuilder builder;
        int64_t nelements = 0;
        std::unique_ptr<ShapeVectorType> shapes;

        if constexpr(std::is_same<ColumnType, casacore::ScalarColumn<DT>>::value) {
            try {
                auto strings = column.getColumnRange(casacore::Slice(startrow, nrow));

                for(auto & s: strings) {
                    builder.Append(s);
                    nelements += 1;
                }
            } catch(std::exception & e) {
                return arrow::Status::Invalid("MakeArrowStringArray ", column_desc.name(), " ", e.what());
            }
        } else if constexpr(std::is_same<ColumnType, casacore::ArrayColumn<DT>>::value) {
            if(!column_desc.isFixedShape()) {
                auto default_shape = casacore::IPosition(column_desc.ndim(), 0);
                shapes = std::make_unique<ShapeVectorType>(nrow, std::move(default_shape));
            }

            for(casacore::uInt row=startrow; row < nrow; ++row) {
                if(column.isDefined(row)) {
                    auto array = column.get(row);

                    if(!column_desc.isFixedShape()) {
                        (*shapes)[row - startrow] = array.shape();
                    }

                    for(auto & string: array) {
                        builder.Append(string);
                        nelements += 1;
                    }
                }
            }
        } else {
            return arrow::Status::Invalid("Unknown column type for ", column_desc.name());
        }

        ARROW_ASSIGN_OR_RAISE(auto array, builder.Finish());
        return std::make_tuple(std::move(array), std::move(shapes));
    }

    template <typename DT>
    arrow::Result<std::tuple<std::shared_ptr<arrow::Buffer>, int64_t>>
    MakeVectorBuffer(casacore::ScalarColumn<DT> & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nrow*sizeof(DT), pool));
        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));

        // Wrap Arrow Buffer in casacore Vector
        auto casa_vector = casacore::Vector<DT>(
            casacore::IPosition(1, nrow),
            reinterpret_cast<DT *>(buffer->mutable_data()),
            casacore::SHARE);

        // Dump column data into Arrow
        try {
            column.getColumnRange(casacore::Slice(startrow, nrow, 1), casa_vector);
        } catch(std::exception & e) {
            return arrow::Status::Invalid("MakeVectorBuffer ", column_desc.name(), " ", e.what());
        }

        return std::make_tuple(std::move(buffer), nrow);
    }

    template <typename DT>
    arrow::Result<std::tuple<std::shared_ptr<arrow::Buffer>, int64_t>>
    MakeFixedArrayBuffer(casacore::ArrayColumn<DT> & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        // Fixed shape Array
        auto shape = column_desc.shape();
        shape.append(casacore::IPosition(1, nrow));
        int64_t nelements = shape.product();

        // Allocate an Arrow Buffer from default Memory Pool
        ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(DT), pool));
        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));


        auto array = casacore::Array<DT>(
            shape,
            reinterpret_cast<DT *>(buffer->mutable_data()),
            casacore::SHARE);

        // Dump column data into Arrow Buffer
        try {
            column.getColumnRange(casacore::Slice(startrow, nrow, 1), array);
        } catch(std::exception & e) {
            return arrow::Status::Invalid("MakeFixedArrayBuffer ", column_desc.name(), " ", e.what());
        }

        return std::make_tuple(std::move(buffer), nelements);
    }

    template <typename DT>
    arrow::Result<std::tuple<
        std::shared_ptr<arrow::Buffer>,
        int64_t,
        std::unique_ptr<ShapeVectorType>>>
    MakeVariableArrayBuffer(casacore::ArrayColumn<DT> & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        int64_t nelements = 0;

        // Variably shaped. Two passes
        // First, determine the number of elements in the Array
        auto shapes = std::make_unique<ShapeVectorType>(
            nrow,
            casacore::IPosition(column_desc.ndim(), 0));

        for(casacore::uInt row=startrow; row < nrow; ++row) {
            if(column.isDefined(row)) {
                (*shapes)[row - startrow] = column.shape(row);
                nelements += (*shapes)[row - startrow].product();
            }
        }

        // Allocate an Arrow Buffer from default Memory Pool
        ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(DT), pool));
        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
        auto * buffer_ptr = reinterpret_cast<DT *>(buffer->mutable_data());
        casacore::uInt offset = 0;

        // Secondly dump data into the buffer
        for(casacore::uInt row=startrow; row < nrow; ++row) {
            auto product = (*shapes)[row - startrow].product();
            if(column.isDefined(row) && product > 0) {
                auto array = casacore::Array<DT>(
                    (*shapes)[row - startrow],
                    buffer_ptr + offset, casacore::SHARE);
                try {
                   column.get(row, array);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("MakeVariableArrayBuffer ", column_desc.name(), " ", e.what());
                }
                offset += product;
            }
        }

        return std::make_tuple(std::move(buffer), nelements, std::move(shapes));
    }

    template <typename ColumnType, typename DT>
    arrow::Result<std::tuple<
        std::shared_ptr<arrow::Array>,
        std::unique_ptr<ShapeVectorType>>>
    MakeArrowPrimitiveArray(ColumnType & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        std::unique_ptr<ShapeVectorType> shapes;
        int64_t nelements = 0;

        // Construct a buffer
        std::shared_ptr<arrow::Buffer> buffer;

        if constexpr(std::is_same<ColumnType, casacore::ScalarColumn<DT>>::value) {
            ARROW_ASSIGN_OR_RAISE(std::tie(buffer, nelements), (MakeVectorBuffer<DT>(column, arrow_dtype)));
        } else if constexpr(std::is_same<ColumnType, casacore::ArrayColumn<DT>>::value) {
            if(column_desc.isFixedShape()) {
                ARROW_ASSIGN_OR_RAISE(std::tie(buffer, nelements), (MakeFixedArrayBuffer<DT>(column, arrow_dtype)));
            } else {
                ARROW_ASSIGN_OR_RAISE(std::tie(buffer, nelements, shapes), (MakeVariableArrayBuffer<DT>(column, arrow_dtype)));
            }
        } else {
            return arrow::Status::Invalid("Unknown column type for ", column_desc.name());
        }

        auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype);

        // At this point we have a buffer of nelements
        if(complex_dtype) {
            // Complex values are merely FixedListArrays of pairs float/double

            // Array of floats/doubles
            auto child_array = std::make_shared<arrow::PrimitiveArray>(
                complex_dtype->value_type(), 2*nelements, buffer,
                nullptr, 0, 0);

            // NOTE(sjperkins)
            // Check the FixedSizeListAray layout documents
            // https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout
            // A single empty buffer {nullptr} must be provided otherwise this segfaults
            auto array_data = arrow::ArrayData::Make(
                complex_dtype, nelements, {nullptr}, {child_array->data()},
                0, 0);

            array = complex_dtype->MakeArray(array_data);
        } else {
            array = std::make_shared<arrow::PrimitiveArray>(arrow_dtype, nelements, buffer, nullptr, 0, 0);
        }

        return std::make_tuple(std::move(array), std::move(shapes));
    }

    using CreateReturnType = std::tuple<
        std::shared_ptr<arrow::Array>,
        std::unique_ptr<ShapeVectorType>,
        std::shared_ptr<arrow::Buffer>,
        int64_t>;

    template <typename ColumnType, typename DT>
    arrow::Result<CreateReturnType>
    MakeArrowArrayNew(ColumnType & column, const std::shared_ptr<arrow::DataType> & arrow_dtype)
    {
        std::shared_ptr<arrow::Array> array;
        std::unique_ptr<ShapeVectorType> shapes;
        std::unique_ptr<ShapeVectorType> products;

        // Now create the flattened array of values for this column
        if constexpr(std::is_same<DT, casacore::String>::value) {
            ARROW_ASSIGN_OR_RAISE(std::tie(array, shapes), (MakeArrowStringArray<ColumnType, DT>(column, arrow_dtype)));
        } else {
            ARROW_ASSIGN_OR_RAISE(std::tie(array, shapes), (MakeArrowPrimitiveArray<ColumnType, DT>(column, arrow_dtype)));
        }

        // Determine nulls and null counts at the row level
        int64_t null_counts = 0;
        ARROW_ASSIGN_OR_RAISE(auto nulls, arrow::AllocateBitmap(nrow, pool));

        for(casacore::uInt row=startrow; row < nrow; ++row) {
            auto is_defined = column.isDefined(row);
            arrow::bit_util::SetBitTo(nulls->mutable_data(), row, is_defined);
            null_counts += is_defined ? 0 : 1;
        }

        // If we have shapes at this point,
        // calculate the cumulative product for each dimension
        if(shapes) {
            // Sanity checks
            if(shapes->size() != nrow) {
                return arrow::Status::Invalid("shapes.size() != nrow");
            }
        }

        return std::make_tuple(std::move(array), std::move(shapes),
                               std::move(nulls), null_counts);
    }

    template <typename T>
    arrow::Status ConvertScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        using CT = casacore::ScalarColumn<T>;
        auto column = CT(this->column);
        ARROW_ASSIGN_OR_RAISE(auto result, (MakeArrowArrayNew<CT, T>(column, arrow_dtype)));
        this->array = std::get<0>(result);
        // Indicate success/failure
        return this->array->Validate();
    }

    template <typename T>
    arrow::Status ConvertFixedArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        using CT = casacore::ArrayColumn<T>;
        auto column = CT(this->column);
        ARROW_ASSIGN_OR_RAISE(auto result, (MakeArrowArrayNew<CT, T>(column, arrow_dtype)));
        this->array = std::get<0>(result);

        // Fortran ordering
        for(auto dim_size: column_desc.shape()) {
            ARROW_ASSIGN_OR_RAISE(this->array, arrow::FixedSizeListArray::FromArrays(this->array, dim_size));
        }

        return this->array->Validate();
    }

    template <typename T>
    arrow::Status ConvertVariableArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        std::shared_ptr<arrow::Array> values;
        std::unique_ptr<ShapeVectorType> shape_ptr;
        std::shared_ptr<arrow::Buffer> nulls;
        int64_t null_counts;

        using CT = casacore::ArrayColumn<T>;
        auto column = CT(this->column);
        auto ndim = column_desc.ndim();

        ARROW_ASSIGN_OR_RAISE(
            std::tie(values, shape_ptr, nulls, null_counts),
            (MakeArrowArrayNew<CT, T>(column, arrow_dtype)));

        if(!shape_ptr) {
            return arrow::Status::Invalid("shapes not provided");
        }

        ShapeVectorType & shapes = *shape_ptr;

        // NOTE(sjperkins)
        // See https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
        // At this stage we have a flat array of values that we wish to arrange
        // into a nested structure. It needs to be built from the fastest changing
        // dimension towards the slowest changing dimension. This is accomplished by
        // creating offsets for each nested layer. The dimension size at each layer
        // must be repeated by the product of the previous elements in the shape.

        // See this worked case
        //    Here, we use casacore's FORTRAN ordering for shapes
        //    repeats = [reduce(mul, s[d+1:], 1) for s, d in enumerate(shapes_]
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
        auto products = ShapeVectorType(nrow, casacore::IPosition(column_desc.ndim(), 1));

        for(casacore::uInt row=startrow; row < nrow; ++row) {
            const auto & shape = shapes[row - startrow];
            auto & product = products[row - startrow];

            for(ssize_t d=column_desc.ndim() - 2; d >= 0; --d) {
                product[d] *= product[d + 1] * shape[d + 1];
            }
        }

        for(int d=0; d < ndim; ++d) {
            // Precompute size of offsets for buffer allocation
            int32_t noffsets = std::accumulate(products.begin(), products.end(), 1,
                [d](auto i, const auto & v) { return i + v[d]; });

            ARROW_ASSIGN_OR_RAISE(
                auto offset_buffer,
                arrow::AllocateBuffer(noffsets*sizeof(noffsets), pool));

            int32_t * optr = reinterpret_cast<int32_t *>(offset_buffer->mutable_data());
            int32_t running_offset = 0;
            int32_t o = 0;
            optr[o++] = running_offset;

            for(casacore::uInt row=startrow; row < nrow; ++row) {
                auto repeats = products[row - startrow][d];
                auto dim_size = shapes[row - startrow][d];

                for(auto r=0; r < repeats; ++r) {
                    running_offset += dim_size;
                    optr[o++] = running_offset;
                }
            }

            assert(o == noffsets);
            assert(optr[o - 1] == nrow);
            auto offsets = std::make_shared<arrow::PrimitiveArray>(
                arrow::int32(), noffsets, std::move(offset_buffer));
            ARROW_ASSIGN_OR_RAISE(values, arrow::ListArray::FromArrays(*offsets, *values));
            // NOTE(sjperkins): Perhaps remove this for performance
            ARROW_RETURN_NOT_OK(values->Validate());
        }

        if(auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(values)) {
            // NOTE(sjperkins)
            // Directly adding nulls to the underlying list_array->data()
            // doesn't seem to work, recreate the array with nulls and null_count
            ARROW_ASSIGN_OR_RAISE(this->array, arrow::ListArray::FromArrays(
                *list_array->offsets(),
                *list_array->values(),
                pool,
                nulls,
                null_counts));

            return this->array->Validate();
        } else {
            return arrow::Status::Invalid("Unable to cast final array to arrow::ListArray");
        }
    }

    template <typename T>
    arrow::Status ConvertArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        if(column_desc.ndim() < 1) {
            return arrow::Status::Invalid(
                "Array column dimensionality (",
                column_desc.ndim(), ") < 1");
        }

        if(column_desc.isFixedShape()) {
            return ConvertFixedArrayColumn<T>(arrow_dtype);
        } else {
            return ConvertVariableArrayColumn<T>(arrow_dtype);
        }
    }

    template <typename T>
    arrow::Status CheckByteWidths(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        // Check that the arrow type byte widths match up with the casacore byte widths
        auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype);

        if(complex_dtype) {
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

        if(column_desc.ndim() == -1) {
            return arrow::Status::NotImplemented(
                column_desc.name(), " has unconstrained dimensionality");
        }

        if(column_desc.isScalar()) {
            return ConvertScalarColumn<T>(arrow_dtype);
        } else if(column_desc.isArray()) {
            return ConvertArrayColumn<T>(arrow_dtype);
        }

        return arrow::Status::Invalid(
            "Conversion of ", column_desc.name(), " failed.");
    }
};
