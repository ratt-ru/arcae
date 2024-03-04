#ifndef ARCAE_COLUMN_READ_VISITOR_H
#define ARCAE_COLUMN_READ_VISITOR_H

#include <functional>
#include <memory>

#include <arrow/array/array_nested.h>
#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/result.h>

#include <casacore/tables/Tables.h>

#include "arcae/array_util.h"
#include "arcae/casa_visitors.h"
#include "arcae/column_read_map.h"
#include "arcae/complex_type.h"

namespace arcae {

class ColumnReadVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    std::reference_wrapper<const ColumnReadMap> map_;
    arrow::MemoryPool * pool_;
    std::shared_ptr<arrow::Array> array_;

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
    arrow::Status ReadScalarColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {

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
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto casa_vector = casacore::Vector<T>(casacore::IPosition(1, nelements), buf_ptr, casacore::SHARE);
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
    arrow::Status ReadFixedColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
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
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            auto carray = casacore::Array<T>(shape, buf_ptr, casacore::SHARE);
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
        for(auto dim=0; dim < shape.size() - 1; ++dim) {
            ARROW_ASSIGN_OR_RAISE(array_, arrow::FixedSizeListArray::FromArrays(array_, shape[dim]));
        }


        return ValidateArray(array_);
    }



    template <typename T>
    arrow::Status ReadVariableColumn(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
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
            ARROW_ASSIGN_OR_RAISE(auto allocation, arrow::AllocateBuffer(nelements*sizeof(T), pool_));
            auto buffer = std::shared_ptr<arrow::Buffer>(std::move(allocation));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());

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

        if(column_desc.isFixedShape() || map_.get().IsFixedShape()) {
            return ReadFixedColumn<T>(arrow_dtype);
        }

        return ReadVariableColumn<T>(arrow_dtype);
    };
};

} // namespace arcae

#endif // ARCAE_COLUMN_READ_VISITOR_H
