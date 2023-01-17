#include <casacore/tables/Tables.h>

#include "casa_visitors.h"

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
    arrow::Status ConvertScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto scalar_column = casacore::ScalarColumn<T>(this->column);
        auto nrows = scalar_column.nrow();

        // Allocate an Arrow Buffer from default Memory Pool
        auto allocation = arrow::AllocateBuffer(nrows*sizeof(T), nullptr);

        if(!allocation.ok()) {
            return allocation.status();
        }

        auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation.ValueOrDie()));

        // Wrap Arrow Buffer in casacore Vector
        auto casa_vector = casacore::Vector<T>(
            casacore::IPosition(1, nrows),
            reinterpret_cast<T *>(buffer->mutable_data()),
            casacore::SHARE);

        // Dump column data into Arrow Buffer
        scalar_column.getColumn(casa_vector);

        // Create a Primitive Arrow Array
        this->array = std::make_shared<arrow::PrimitiveArray>(
            arrow_dtype,
            nrows,
            buffer,
            nullptr,
            0, 0);

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

        // Create basic array
        auto base_array = std::make_shared<arrow::PrimitiveArray>(
            arrow_dtype,
            length,
            buffer,
            nullptr,
            0, 0);

        this->array = arrow::FixedSizeListArray::FromArrays(base_array, shape[0]).ValueOrDie();

        for(std::size_t i=1; i<shape.size()-1; ++i) {
            this->array = arrow::FixedSizeListArray::FromArrays(this->array, shape[i]).ValueOrDie();
        }

        return this->array->Validate();
    }

    template <typename T>
    arrow::Status ConvertColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        const auto & column_desc = this->column.columnDesc();

        if(arrow_dtype->byte_width() == -1 || arrow_dtype->byte_width() != sizeof(T)) {
            return arrow::Status::Invalid(
                arrow_dtype->ToString(), " byte width (",
                arrow_dtype->byte_width(),
                ") != sizeof(T) (", sizeof(T), ")");
        }

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
