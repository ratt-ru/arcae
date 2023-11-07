#ifndef ARCAE_NEW_CONVERT_VISITOR_H
#define ARCAE_NEW_CONVERT_VISITOR_H

#include <algorithm>
#include <unordered_set>

#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "arcae/casa_visitors.h"
#include "arcae/column_mapper.h"
#include "arcae/complex_type.h"
#include "arcae/service_locator.h"
#include "arcae/utility.h"

namespace arcae {

class NewConvertVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    const casacore::TableColumn & column_;
    const casacore::ColumnDesc & column_desc_;
    const ColumnMapping<casacore::rownr_t> & column_map_;
    std::shared_ptr<arrow::Array> array_;
    arrow::MemoryPool * pool_;

public:
    explicit NewConvertVisitor(
        const casacore::TableColumn & column,
        const ColumnMapping<casacore::rownr_t> & column_map,
        arrow::MemoryPool * pool=arrow::default_memory_pool()) :
            column_(column),
            column_desc_(column_.columnDesc()),
            column_map_(column_map),
            array_(),
            pool_(pool) {};
    virtual ~NewConvertVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

    template <typename T>
    arrow::Status ConvertScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ScalarColumn<T>(column_);
        column.setMaximumCacheSize(1);

        if(column_map_.nDim() != 1) {
            return arrow::Status::Invalid(
                "Number of dimensions in Column Map does not match "
                "that of column ", column_desc_.name());
        }

        if constexpr(std::is_same<T, casacore::String>::value) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(*it);
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(s)); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertScalarColumn ", column_desc_.name(), " ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Vector
            auto nelements = column_map_.nElements();
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_vector = casacore::Vector<T>(casacore::IPosition(1, nelements), buf_ptr, casacore::SHARE);

            assert(column_map_.IsSimple());

            for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                // Dump column data into Arrow Buffer
                try {
                    std::cout << "Getting " << *it << std::endl;
                    column.getColumnRange(*it, casa_vector);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertScalarColumn ", column_desc_.name(), " ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        return ValidateArray(array_);
    }


private:

    static arrow::Status ValidateArray(const std::shared_ptr<arrow::Array> & array);

    inline arrow::Status FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        if(arrow_dtype == arrow::utf8()) { return arrow::Status::OK(); }
        return arrow::Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
    }

    arrow::Result<std::shared_ptr<arrow::Array>> MakeArrowPrimitiveArray(
            const std::shared_ptr<arrow::Buffer> & buffer,
            casacore::uInt nelements,
            const std::shared_ptr<arrow::DataType> & arrow_dtype);

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

        if(column_desc_.isScalar()) {
            return ConvertScalarColumn<T>(arrow_dtype);
        }

        return arrow::Status::Invalid("Unable to convert column ", column_desc_.name());
        
    };
};

} // namespace arcae

#endif // ARCAE_NEW_CONVERT_VISITOR_H
