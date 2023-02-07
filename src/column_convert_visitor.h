#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "casa_visitors.h"
#include "complex_type.h"



enum ColumnType {
    ArrayColumn,
    ScalarColumn,
    VariableColumn,
};

template <ColumnType CT, typename T> arrow::Status
MakeArrowArray(const std::shared_ptr<arrow::DataType> & arrow_dtype)
{
    if constexpr(std::is_same<T, casacore::String>::value) {
        if(arrow_dtype != arrow::utf8()) {
            return arrow::Status::Invalid(
                arrow_dtype->ToString(),
                "incompatible with casacore::String");
        }
    }

    if constexpr(CT == ArrayColumn) {
        return arrow::Status::OK();
    } else if constexpr(CT == ScalarColumn) {
        return arrow::Status::OK();
    } else if constexpr(CT == VariableColumn) {
        return arrow::Status::OK();
    } else {
        return arrow::Status::Invalid("blah");
    }
}


class ColumnConvertVisitor : public CasaTypeVisitor {
public:
    const casacore::TableColumn & column;
    const casacore::ColumnDesc & column_desc;
    std::shared_ptr<arrow::Array> array;
    arrow::MemoryPool * pool;


public:
    explicit ColumnConvertVisitor(
        const casacore::TableColumn & column,
        arrow::MemoryPool * pool=arrow::default_memory_pool());
    virtual ~ColumnConvertVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

private:
    template <typename T>
    arrow::Result<std::shared_ptr<arrow::Array>> MakeArrowArray(
        std::shared_ptr<arrow::Buffer> buffer,
        const std::shared_ptr<arrow::DataType> & dtype,
        int64_t length)
    {
        auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(dtype);

        if(complex_dtype) {
            // Array of floats/doubles
            auto child_array = std::make_shared<arrow::PrimitiveArray>(
                complex_dtype->value_type(), 2*length, buffer,
                nullptr, 0, 0);

            // NOTE(sjperkins)
            // Check the FixedSizeListAray layout documents
            // https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout
            // A single empty buffer {nullptr} must be provided otherwise this segfaults
            auto array_data = arrow::ArrayData::Make(
                complex_dtype, length, {nullptr}, {child_array->data()},
                0, 0);

            return complex_dtype->MakeArray(array_data);
        } else if(dtype == arrow::utf8()) {
            return nullptr;
        } else {
            return std::make_shared<arrow::PrimitiveArray>(
                dtype, length, buffer, nullptr, 0, 0);
        }
    }

    template <typename T>
    arrow::Status ConvertScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto scalar_column = casacore::ScalarColumn<T>(this->column);
        auto nrows = scalar_column.nrow();
        auto length = nrows;

        auto allocation = arrow::AllocateBuffer(nrows*sizeof(T), pool);
        ARROW_RETURN_NOT_OK(allocation);

        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation.ValueOrDie()));

        // Wrap Arrow Buffer in casacore Vector
        auto casa_vector = casacore::Vector<T>(
            casacore::IPosition(1, length),
            reinterpret_cast<T *>(buffer->mutable_data()),
            casacore::SHARE);

        // Dump column data into Arrow Buffer
        scalar_column.getColumn(casa_vector);

        ARROW_ASSIGN_OR_RAISE(this->array, MakeArrowArray<T>(buffer, arrow_dtype, length));

        // Indicate success/failure
        return this->array->Validate();
    }

    template <typename T>
    arrow::Status ConvertFixedArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto array_column = casacore::ArrayColumn<T>(this->column);
        auto nrows = array_column.nrow();
        auto shape = column_desc.shape();
        auto length = shape.product()*nrows;

        // Allocate an Arrow Buffer from default Memory Pool
        auto allocation = arrow::AllocateBuffer(length*sizeof(T), pool);
        ARROW_RETURN_NOT_OK(allocation);
        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation.ValueOrDie()));

        shape.append(casacore::IPosition(1, nrows));
        // Wrap Arrow Buffer in casacore Vector
        auto casa_array = casacore::Array<T>(
            shape,
            reinterpret_cast<T *>(buffer->mutable_data()),
            casacore::SHARE);

        // Dump column data into Arrow Buffer
        array_column.getColumn(casa_array);

        ARROW_ASSIGN_OR_RAISE(auto base_array, MakeArrowArray<T>(buffer, arrow_dtype, length));

        this->array = arrow::FixedSizeListArray::FromArrays(base_array, shape[0]).ValueOrDie();

        for(std::size_t i=1; i<shape.size()-1; ++i) {
            this->array = arrow::FixedSizeListArray::FromArrays(this->array, shape[i]).ValueOrDie();
        }

        return this->array->Validate();
    }

    template <typename T>
    arrow::Status ConvertVariableArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto array_column = casacore::ArrayColumn<T>(this->column);
        auto nrows = array_column.nrow();
        auto ndim = column_desc.ndim();

        ARROW_ASSIGN_OR_RAISE(auto nulls, arrow::AllocateBitmap(nrows, pool));
        std::vector<casacore::IPosition> shapes(nrows, casacore::IPosition(ndim, 0));
        std::vector<casacore::IPosition> products(nrows, casacore::IPosition(ndim, 0));
        int64_t nelements = 0;
        int64_t null_count = 0;

        // Iterate over each table row to determine
        // (1) null bitmap
        // (2) offsets between number of elements in each row
        for(casacore::uInt row=0; row < nrows; ++row) {
            bool is_defined = array_column.isDefined(row);
            arrow::bit_util::SetBitTo(nulls->mutable_data(), row, is_defined);

            if(is_defined) {
                auto casa_shape = array_column.shape(row);

                // CASA stores shapes in Fortran ordering,
                // invert this for sanities sake
                for(auto d=0; d<ndim; ++d) {
                    shapes[row][ndim - d - 1] = casa_shape[d];
                    products[row][ndim - d - 1] = casa_shape[d];
                }

                for(auto d=1; d<ndim; ++d) {
                    products[row][d] *= products[row][d - 1];
                }

                nelements += products[row][ndim - 1];

            } else {
                shapes[row] = casacore::IPosition(ndim, 0);
                null_count += 1;
            }
        }

        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nelements*sizeof(T), pool));
        auto * buffer_ptr = reinterpret_cast<T *>(buffer->mutable_data());
        casacore::uInt element = 0;

        // Read data into the values buffer
        for(casacore::uInt row=0; row < nrows; ++row) {
            if(!arrow::bit_util::GetBit(nulls->data(), row)) {
                continue;
            }

            if(products[row][ndim - 1] == 0) {
                continue;
            }

            // Create a casa Array that wraps the buffer at this
            // row location and read data into it.
            auto casa_array = casacore::Array<T>(
                array_column.shape(row),
                buffer_ptr + element,
                casacore::SHARE);

            array_column.get(row, casa_array);
            element += products[row][ndim - 1];
        }

        // Create a flat array of values
        ARROW_ASSIGN_OR_RAISE(
            std::shared_ptr<arrow::Array> values,
            MakeArrowArray<T>(std::move(buffer), arrow_dtype, nelements));

        // NOTE(sjperkins)
        // See https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
        // At this stage we have a flat array of values that we wish to arrange
        // into a nested structure. It needs to be built from the fastest changing
        // dimension towards the slowest changing dimension. This is accomplished by
        // creating offsets for each nested layer. The dimension size at each layer
        // must be repeated by the product of the previous elements in the shape.

        // See this worked case
        // for d in reversed(range(ndim))
        //    repeats = [reduce(mul, s[:d], 1) for s in shapes]
        //    dim_sizes = [s[d] for s in shapes]

        //    shapes = [(10, 5, 2), (10, 3, 4)]
        //    For d==2
        //    repeats = [50, 30]
        //    dim_sizes = [2, 4]
        //    offsets = [0] + cumsum(50*[2] + 30*[4])
        //    For d==1
        //    repeats = [10, 10]
        //    dim_sizes = [5, 3]
        //    offsets = [0] + cumsum(10*[5] + 10*[3])
        //    For d==0
        //    repeats = [1, 1]
        //    dim_sizes = [10, 10]
        //    offsets = [0] + cumsum(1*[10] + 1*[10])
        for(int d=ndim-1; d >= 0; --d) {
            arrow::Int32Builder builder;
            int32_t running_offset = 0;
            ARROW_RETURN_NOT_OK(builder.Append(running_offset));

            for(auto row=0; row < nrows; ++row) {
                auto repeats = d >= 1 ? products[row][d - 1] : 1;
                auto dim_size = shapes[row][d];

                for(auto r=0; r < repeats; ++r) {
                    running_offset += dim_size;
                    ARROW_RETURN_NOT_OK(builder.Append(running_offset));
                }
            }

            std::shared_ptr<arrow::Array> offsets;
            ARROW_RETURN_NOT_OK(builder.Finish(&offsets));
            ARROW_ASSIGN_OR_RAISE(values, arrow::ListArray::FromArrays(*offsets, *values));
            // NOTE(sjperkins): Perhaps remove this for performance
            ARROW_RETURN_NOT_OK(values->Validate());
        }

        auto list_array = std::dynamic_pointer_cast<arrow::ListArray>(values);

        if(list_array == nullptr) {
            return arrow::Status::Invalid("Unable to cast final array to arrow::ListArray");
        }

        // NOTE(sjperkins)
        // Directly adding nulls to the underlying list_array->data()
        // doesn't seem to work, recreate the array with nulls and null_count
        ARROW_ASSIGN_OR_RAISE(this->array, arrow::ListArray::FromArrays(
            *list_array->offsets(),
            *list_array->values(),
            pool,
            nulls,
            null_count));

        return this->array->Validate();
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
