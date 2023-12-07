#ifndef ARCAE_NEW_CONVERT_VISITOR_H
#define ARCAE_NEW_CONVERT_VISITOR_H

#include <algorithm>
#include <cstddef>
#include <functional>
#include <unordered_set>

#include <casacore/tables/Tables.h>

#include <arrow/util/logging.h>  // IWYU pragma: keep

#include "arcae/casa_visitors.h"
#include "arcae/column_mapper.h"
#include "arcae/complex_type.h"
#include "arcae/service_locator.h"
#include "arcae/utility.h"
#include "arrow/result.h"

namespace arcae {

class NewConvertVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    std::reference_wrapper<const casacore::TableColumn> column_;
    const ColumnMapping & column_map_;
    std::shared_ptr<arrow::Array> array_;
    arrow::MemoryPool * pool_;

public:
    explicit NewConvertVisitor(
        const casacore::TableColumn & column,
        const ColumnMapping & column_map,
        arrow::MemoryPool * pool=arrow::default_memory_pool()) :
            column_(column),
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

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            return arrow::Status::NotImplemented("ConvertScalarColumn<casacore::String>");
            arrow::StringBuilder builder;

            for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(s)); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertScalarColumn ",
                                                  column_.get().columnDesc().name(),
                                                  ": ", e.what());
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
            auto casa_ptr = casa_vector.data();

            if(column_map_.IsSimple()) {
                try {
                    // Dump column data straight into the Arrow Buffer
                    auto it = column_map_.RangeBegin();
                    column.getColumnRange(it.GetRowSlicer(), casa_vector);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertScalarColumn ",
                                                  column_.get().columnDesc().name(),
                                                  ": ", e.what());
                }
            } else {
                for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                    // Copy sections of data into the Arrow Buffer
                    try {
                        auto chunk = column.getColumnRange(it.GetRowSlicer());
                        auto chunk_ptr = chunk.data();

                        for(auto [i, mit] = std::tuple{int{0}, it.MapBegin()}; mit != it.MapEnd(); ++mit, ++i) {
                            auto out = mit.ToBufferOffset();
                            casa_ptr[out] = chunk_ptr[i];
                        }
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("ConvertScalarColumn ",
                                                      column_.get().columnDesc().name(),
                                                      ": ", e.what());
                    }
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        return ValidateArray(array_);
    }

    template <typename T>
    arrow::Status ConvertFixedColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ArrayColumn<T>(column_);
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            return arrow::Status::Invalid("ConvertedFixedColumn<casacore::String>");
            arrow::StringBuilder builder;

            for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(s)); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertFixedColumn ",
                                                  column_.get().columnDesc().name(),
                                                  ": ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto nelements = column_map_.nElements();
            ARROW_ASSIGN_OR_RAISE(auto shape, column_map_.GetOutputShape());
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto carray = casacore::Array<T>(shape, buf_ptr, casacore::SHARE);
            auto casa_ptr = carray.data();

            if(column_map_.IsSimple()) {
                try {
                    // Dump column data straight into the Arrow Buffer
                    column.getColumnRange(column_map_.RangeBegin().GetRowSlicer(),
                                          column_map_.RangeBegin().GetSectionSlicer(),
                                          carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ConvertFixedColumn ",
                                                  column_.get().columnDesc().name(),
                                                  ": ", e.what());
                }
            } else {
                for(auto it = column_map_.RangeBegin(); it != column_map_.RangeEnd(); ++it) {
                    // Copy sections of data into the Arrow Buffer
                    try {
                        auto chunk = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                        auto chunk_ptr = chunk.data();

                        for(auto [i, mit] = std::tuple{int{0}, it.MapBegin()}; mit != it.MapEnd(); ++mit, ++i) {
                            auto out = mit.ToBufferOffset();
                            casa_ptr[out] = chunk_ptr[i];
                        }
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("ConvertFixedColumn ",
                                                      column_.get().columnDesc().name(),
                                                      ": ", e.what());
                    }
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
        auto column_desc = column_.get().columnDesc();


        if(column_desc.isScalar()) {
            return ConvertScalarColumn<T>(arrow_dtype);
        }

        if(column_desc.isFixedShape()) {
            return ConvertFixedColumn<T>(arrow_dtype);
        }

        return arrow::Status::Invalid("Unable to convert column ",
                                      column_desc.name());

    };
};

} // namespace arcae

#endif // ARCAE_NEW_CONVERT_VISITOR_H
