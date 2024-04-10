#ifndef ARCAE_COLUMN_READ_VISITOR_H
#define ARCAE_COLUMN_READ_VISITOR_H

#include <cstddef>
#include <functional>
#include <memory>

#include <arrow/array/array_nested.h>
#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/status.h>
#include <arrow/result.h>

#include <casacore/tables/Tables.h>

#include "arcae/array_util.h"
#include "arcae/base_column_map.h"
#include "arcae/casa_visitors.h"
#include "arcae/column_read_map.h"
#include "arcae/complex_type.h"
#include "arcae/configuration.h"
#include "arcae/service_locator.h"

namespace arcae {

class ColumnReadVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    std::reference_wrapper<const ColumnReadMap> map_;
    std::shared_ptr<arrow::Array> array_;
    arrow::MemoryPool * pool_;

public:
    explicit ColumnReadVisitor(
        const ColumnReadMap & column_map,
        arrow::MemoryPool * pool=arrow::default_memory_pool()) :
            map_(std::cref(column_map)),
            pool_(pool) {};
    virtual ~ColumnReadVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

    arrow::Status Visit();


    const casacore::TableColumn & GetTableColumn() const {
        return map_.get().column_.get();
    }

    template <typename T>
    arrow::Result<std::shared_ptr<arrow::Buffer>>
    GetResultBufferOrAllocate(std::size_t nelements) const {
        auto GetBuffer = [&]() -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
            // Get the data buffer if a result array has been provided on the map
            if(auto result = map_.get().result_; result) {
                // TODO(sjperkins). Move most of these checks into the ColumnReadMap
                auto shape_result = map_.get().GetOutputShape();
                if(!shape_result.ok()) {
                    return arrow::Status::NotImplemented(
                        "Reading to supplied result arrays "
                        "from variably shaped selections");
                }

                auto selection_shape = shape_result.ValueOrDie();
                ARROW_ASSIGN_OR_RAISE(auto result_props,
                                      GetArrayProperties(GetTableColumn(), result));

                if(!result_props.shape.has_value()) {
                    return arrow::Status::NotImplemented(
                        "Reading to variably shaped result arrays "
                        "for fixed shaped selections");
                }

                auto result_shape = result_props.shape.value();

                if(result_shape != selection_shape) {
                    return arrow::Status::Invalid(
                        "Selection shape ", selection_shape,
                        " does not match result array shape ", result_shape);
                }

                auto array_data = result->data();

                while(true) {
                    switch(array_data->type->id()) {
                        case arrow::Type::LARGE_LIST:
                        case arrow::Type::LIST:
                        case arrow::Type::FIXED_SIZE_LIST:
                            array_data = array_data->child_data[0];
                            break;
                        case arrow::Type::BOOL:
                        case arrow::Type::UINT8:
                        case arrow::Type::UINT16:
                        case arrow::Type::UINT32:
                        case arrow::Type::UINT64:
                        case arrow::Type::INT8:
                        case arrow::Type::INT16:
                        case arrow::Type::INT32:
                        case arrow::Type::INT64:
                        case arrow::Type::FLOAT:
                        case arrow::Type::DOUBLE:
                        case arrow::Type::STRING:
                        {
                            if(array_data->buffers.size() == 0) {
                                return arrow::Status::Invalid("Result array does not contain a buffer");
                            }

                            if(auto buffer = array_data->buffers[array_data->buffers.size() - 1]; buffer) {
                                return buffer;
                            }

                            return arrow::Status::Invalid("Result array does not contain a buffer");
                        }
                        default:
                            return arrow::Status::TypeError(
                                "Unable to obtain array root buffer "
                                "for types ", array_data->type);
                    }
                }

                return arrow::Status::Invalid("Result array does not contain a buffer ", result->ToString());
            } else {
                // Allocate a result buffer
                ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
                return std::shared_ptr<arrow::Buffer>(std::move(allocation));
            }
        };

        ARROW_ASSIGN_OR_RAISE(auto buffer, GetBuffer());
        auto span = buffer->template span_as<T>();
        ARROW_RETURN_NOT_OK(CheckElements(span.size(), nelements));
        return buffer;
    }

    template <typename T>
    arrow::Status
    ReadScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ScalarColumn<T>(GetTableColumn());
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(std::move(s))); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadScalarColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Vector
            auto nelements = map_.get().nElements();
            ARROW_ASSIGN_OR_RAISE(auto buffer, GetResultBufferOrAllocate<T>(nelements));
            auto casa_vector = casacore::Vector<T>(casacore::IPosition(1, nelements),
                                                   buffer->template mutable_data_as<T>(),
                                                   casacore::SHARE);
            auto casa_ptr = casa_vector.data();

            if(map_.get().IsSimple()) {
                try {
                    // Dump column data straight into the Arrow Buffer
                    auto it = map_.get().RangeBegin();
                    column.getColumnRange(it.GetRowSlicer(), casa_vector);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadScalarColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            } else {
                for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                    // Copy sections of data into the Arrow Buffer
                    try {
                        auto chunk = column.getColumnRange(it.GetRowSlicer());
                        auto chunk_ptr = chunk.data();

                        for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                            casa_ptr[mit.GlobalOffset()] = chunk_ptr[mit.ChunkOffset()];
                        }
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("ReadScalarColumn ",
                                                      GetTableColumn().columnDesc().name(),
                                                      ": ", e.what());
                    }
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        return ValidateArray(array_);
    }

    template <typename T>
    arrow::Status
    ReadFixedColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ArrayColumn<T>(GetTableColumn());
        column.setMaximumCacheSize(1);
        ARROW_ASSIGN_OR_RAISE(auto shape, map_.get().GetOutputShape());

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(std::move(s))); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadFixedColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto nelements = map_.get().nElements();
            ARROW_ASSIGN_OR_RAISE(auto buffer, GetResultBufferOrAllocate<T>(nelements));
            auto carray = casacore::Array<T>(shape,
                                             buffer->template mutable_data_as<T>(),
                                             casacore::SHARE);
            auto casa_ptr = carray.data();

            if(map_.get().IsSimple()) {
                try {
                    // Dump column data straight into the Arrow Buffer
                    column.getColumnRange(map_.get().RangeBegin().GetRowSlicer(),
                                          map_.get().RangeBegin().GetSectionSlicer(),
                                          carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadFixedColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            } else {
                for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                    // Copy sections of data into the Arrow Buffer
                    try {
                        auto chunk = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                        auto chunk_ptr = chunk.data();

                        for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                            casa_ptr[mit.GlobalOffset()] = chunk_ptr[mit.ChunkOffset()];
                        }
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("ReadFixedColumn ",
                                                      GetTableColumn().columnDesc().name(),
                                                      ": ", e.what());
                    }
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }

        // Fortran ordering, but without the row dimension
        for(std::size_t dim=0; dim < shape.size() - 1; ++dim) {
            ARROW_ASSIGN_OR_RAISE(array_, arrow::FixedSizeListArray::FromArrays(array_, shape[dim]));
        }


        return ValidateArray(array_);
    }



    template <typename T>
    arrow::Status
    ReadVariableColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        auto column = casacore::ArrayColumn<T>(GetTableColumn());
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(arrow_dtype));
            arrow::StringBuilder builder;

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(std::move(s))); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadVariableColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, builder.Finish());
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto nelements = map_.get().nElements();
            ARROW_ASSIGN_OR_RAISE(auto buffer, GetResultBufferOrAllocate<T>(nelements));
            auto * buf_ptr = buffer->template mutable_data_as<T>();

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                // Copy sections of data into the Arrow Buffer
                try {
                    auto chunk = column.getColumnRange(it.GetRowSlicer(), it.GetSectionSlicer());
                    auto chunk_ptr = chunk.data();

                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        buf_ptr[mit.GlobalOffset()] = chunk_ptr[mit.ChunkOffset()];
                    }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("ReadVariableColumn ",
                                                    GetTableColumn().columnDesc().name(),
                                                    ": ", e.what());
                }
            }

            ARROW_ASSIGN_OR_RAISE(array_, MakeArrowPrimitiveArray(buffer, nelements, arrow_dtype));
        }


        // Fortran ordering
        ARROW_ASSIGN_OR_RAISE(const auto & offsets, map_.get().GetOffsets());

        for(auto & offset: offsets) {
            ARROW_ASSIGN_OR_RAISE(array_, arrow::ListArray::FromArrays(*offset, *array_));
        }

        return ValidateArray(array_);
    }


private:
    arrow::Status FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
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
        const auto & column_desc = GetTableColumn().columnDesc();

        if(column_desc.isScalar()) {
            return ReadScalarColumn<T>(arrow_dtype);
        }

        auto & config = ServiceLocator::configuration();
        auto convert_method = config.GetDefault("casa.convert.strategy", "fixed");
        auto list_convert = convert_method.find("list") == 0;

        if(!list_convert && (column_desc.isFixedShape() || map_.get().IsFixedShape())) {
            return ReadFixedColumn<T>(arrow_dtype);
        }

        return ReadVariableColumn<T>(arrow_dtype);
    }
};

} // namespace arcae

#endif // ARCAE_COLUMN_READ_VISITOR_H
