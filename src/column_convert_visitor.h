#include <casacore/tables/Tables.h>

#include "casa_visitors.h"
#include "complex_type.h"

class ColumnConvertVisitor : public CasaTypeVisitor {
public:
    const casacore::TableColumn & column;
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
        std::shared_ptr<arrow::Buffer> & buffer,
        const std::shared_ptr<arrow::DataType> & dtype,
        int64_t length)
    {
        auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(dtype);

        if(complex_dtype) {
            // Array of floats
            auto child_array = std::make_shared<arrow::PrimitiveArray>(
                complex_dtype->value_type(), 2*length, buffer,
                nullptr, 0, 0);

            auto array_data = arrow::ArrayData::Make(
                complex_dtype, length, {nullptr}, {child_array->data()},
                0, 0);

            return complex_dtype->MakeArray(array_data);
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

        if(!allocation.ok()) {
            return allocation.status();
        }

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
    arrow::Status ConvertArrayColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto array_column = casacore::ArrayColumn<T>(this->column);
        auto column_desc = this->column.columnDesc();

        if(!column_desc.ndim() >= 1) {
            return arrow::Status::Invalid(
                "Array ", column_desc.name(),
                " has dimensionality of ", column_desc.ndim()
            );
        }

        if(!column_desc.isFixedShape()) {
            return arrow::Status::Invalid(
                "Array ", column_desc.name(),
                " is not a FixedShape"
            );
        }

        auto nrows = array_column.nrow();
        auto shape = column_desc.shape();
        auto length = shape.product()*nrows;

        // Allocate an Arrow Buffer from default Memory Pool
        auto allocation = arrow::AllocateBuffer(length*sizeof(T), nullptr);

        if(!allocation.ok()) {
            return allocation.status();
        }

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

        const auto & column_desc = this->column.columnDesc();

        if(column_desc.isScalar()) {
            return this->ConvertScalarColumn<T>(arrow_dtype);
        }

        if(!column_desc.isFixedShape()) {
            return arrow::Status::NotImplemented("Variably shaped ", column_desc.name());
        }

        if(column_desc.isArray()) {
            return this->ConvertArrayColumn<T>(arrow_dtype);
        }

        return arrow::Status::Invalid(
            "Conversion of ", column_desc.name(), " failed.");

    }
};
