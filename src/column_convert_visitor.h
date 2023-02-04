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

public:
    explicit ColumnConvertVisitor(const casacore::TableColumn & column);
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

        // Allocate an Arrow Buffer from default Memory Pool
        auto allocation = arrow::AllocateBuffer(nrows*sizeof(T), nullptr);
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
        auto allocation = arrow::AllocateBuffer(length*sizeof(T), nullptr);
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

        ARROW_ASSIGN_OR_RAISE(auto offset_buffer, arrow::AllocateBuffer((nrows + 1)*sizeof(int32_t), nullptr));
        ARROW_ASSIGN_OR_RAISE(auto nulls, arrow::AllocateBitmap(nrows, nullptr));
        int32_t * offset_ptr = reinterpret_cast<int32_t *>(offset_buffer->mutable_data());
        offset_ptr[0] = 0;
        std::vector<casacore::IPosition> shapes;
        casacore::uInt nelements = 0;


        // Iterate over each table row to determine
        // (1) null bitmap
        // (2) shape of each row
        for(casacore::uInt row=0; row < nrows; ++row) {
            bool is_defined = array_column.isDefined(row);
            arrow::bit_util::SetBitTo(nulls->mutable_data(), row, is_defined);

            if(is_defined) {
                auto column_shape = array_column.shape(row);
                shapes.push_back(column_shape);
                nelements += column_shape.product();
            } else {
                shapes.push_back(casacore::IPosition(column_desc.ndim(), 0));
            }

            offset_ptr[row + 1] = nelements;
        }

        auto offsets = std::make_shared<arrow::Int32Array>(nrows + 1, std::move(offset_buffer));
        ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateBuffer(nelements*sizeof(T), nullptr));
        auto * buffer_ptr = reinterpret_cast<T *>(buffer->mutable_data());
        casacore::uInt element = 0;

        for(casacore::uInt row=0; row < nrows; ++row) {
            const auto & shape = shapes[row];
            auto shape_product = shape.product();

            if(shape_product == 0) {
                continue;
            }

            auto casa_array = casacore::Array<T>(
                shape,
                buffer_ptr + element,
                casacore::SHARE);

            array_column.get(row, casa_array);
            element += shape_product;
        }

        ARROW_ASSIGN_OR_RAISE(auto values, MakeArrowArray<T>(std::move(buffer), arrow_dtype, nelements));
        ARROW_ASSIGN_OR_RAISE(this->array, arrow::ListArray::FromArrays(*offsets, *values, nullptr, nulls));
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
