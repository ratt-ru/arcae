#ifndef COLUMN_WRITE_VISITOR_H
#define COLUMN_WRITE_VISITOR_H

#include <functional>
#include <memory>

#include <arrow/array/array_nested.h>
#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/result.h>

#include <casacore/tables/Tables.h>
#include <type_traits>

#include "arcae/array_util.h"
#include "arcae/casa_visitors.h"
#include "arcae/column_mapper.h"
#include "arcae/complex_type.h"
#include "arrow/array/array_primitive.h"
#include "arrow/buffer.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_traits.h"

namespace arcae {

class ColumnWriteVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    std::reference_wrapper<const ColumnMapping> map_;
    arrow::MemoryPool * pool_;
    std::shared_ptr<arrow::Array> array_;

public:
    explicit ColumnWriteVisitor(
        const ColumnMapping & column_map,
        const std::shared_ptr<arrow::Array> & array,
        arrow::MemoryPool * pool=arrow::default_memory_pool()) :
            map_(std::cref(column_map)),
            array_(array),
            pool_(pool) {};
    virtual ~ColumnWriteVisitor() = default;

#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

    const casacore::TableColumn & GetTableColumn() const {
        return map_.get().column_.get();
    }

    template <typename T>
    arrow::Status WriteScalarColumn() {
        auto column = casacore::ScalarColumn<T>(GetTableColumn());
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same_v<T, casacore::String>) {
            // Handle string cases with Arrow StringBuilders
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(array_->type()));
            arrow::StringBuilder builder;

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                try {
                    auto strings = column.getColumnRange(it.GetRowSlicer());
                    for(auto & s: strings) { ARROW_RETURN_NOT_OK(builder.Append(std::move(s))); }
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteScalarColumn ",
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
                    return arrow::Status::Invalid("WriteScalarColumn ",
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
                        return arrow::Status::Invalid("WriteScalarColumn ",
                                                      GetTableColumn().columnDesc().name(),
                                                      ": ", e.what());
                    }
                }
            }

        }

        return arrow::Status::OK();
    }

    template <typename T>
    arrow::Status WriteFixedColumn() {
        auto column = casacore::ArrayColumn<T>(GetTableColumn());
        assert(array_->type()->id() == arrow::Type::FIXED_SIZE_LIST);
        column.setMaximumCacheSize(1);
        ARROW_ASSIGN_OR_RAISE(auto shape, map_.get().GetOutputShape());

        if constexpr(std::is_same_v<T, casacore::String>) {
            auto GetFlatArray = [&]() -> arrow::Result<std::shared_ptr<arrow::StringArray>> {
                for(auto fsl = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(array_);;) {
                    ARROW_ASSIGN_OR_RAISE(auto next, fsl->Flatten());
                    if(fsl = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(next); !fsl) {
                        auto result = std::dynamic_pointer_cast<arrow::StringArray>(next);
                        assert(result && "Unable to cast values of FixedSizeListArray to StringArray");
                        return result;
                    }
                }
            };

            ARROW_ASSIGN_OR_RAISE(auto flat_strings, GetFlatArray());
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(flat_strings->type()));

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                assert(it.GetShape().product() == flat_strings->length());
                // Copy sections of data from the Arrow Buffer and write
                try {
                    auto carray = casacore::Array<casacore::String>(it.GetShape());
                    auto * chunk_ptr = carray.data();
                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        auto sv = flat_strings->GetView(mit.GlobalOffset());
                        chunk_ptr[mit.ChunkOffset()] = casacore::String(std::begin(sv), std::end(sv));
                    }
                    column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteFixedColumn ",
                                                    GetTableColumn().columnDesc().name(),
                                                    ": ", e.what());
                }
            }
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto nelements = map_.get().nElements();
            auto GetValueBuffer = [&]() -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
                for(auto fsl=std::dynamic_pointer_cast<arrow::FixedSizeListArray>(array_);;) {
                    if(auto next = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(fsl->values()); next) {
                        fsl = next;
                    } else {
                        auto result = fsl->values()->data()->buffers[1];
                        assert(result && "Unable to obtain value buffer of FixedSizeListArray");
                        return result;
                    }
                }
            };

            ARROW_ASSIGN_OR_RAISE(auto buffer, GetValueBuffer());
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());

            if(map_.get().IsSimple()) {
                auto carray = casacore::Array<T>(shape, buf_ptr, casacore::SHARE);

                try {
                    // Dump column data straight into the Arrow Buffer
                    column.putColumnRange(map_.get().RangeBegin().GetRowSlicer(),
                                          map_.get().RangeBegin().GetSectionSlicer(),
                                          carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteFixedColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            } else {
                for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                    // Copy sections of data from the Arrow Buffer and write
                    try {
                        auto carray = casacore::Array<T>(it.GetShape());
                        auto chunk_ptr = carray.data();
                        for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                            chunk_ptr[mit.ChunkOffset()] = buf_ptr[mit.GlobalOffset()];
                        }
                        column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                    } catch(std::exception & e) {
                        return arrow::Status::Invalid("WriteFixedColumn ",
                                                      GetTableColumn().columnDesc().name(),
                                                      ": ", e.what());
                    }
                }
            }
        }

        return arrow::Status::OK();
    }


    template <typename T>
    arrow::Status WriteVariableColumn() {
        auto column = casacore::ArrayColumn<T>(GetTableColumn());
        assert(arrow::is_var_length_list(*array_->type()));
        column.setMaximumCacheSize(1);

        if constexpr(std::is_same_v<T, casacore::String>) {
            auto GetFlatArray = [&]() -> arrow::Result<std::shared_ptr<arrow::StringArray>> {
                for(auto la = std::dynamic_pointer_cast<arrow::ListArray>(array_);;) {
                    ARROW_ASSIGN_OR_RAISE(auto next, la->Flatten());
                    if(la = std::dynamic_pointer_cast<arrow::ListArray>(next); !la) {
                        auto result = std::dynamic_pointer_cast<arrow::StringArray>(next);
                        assert(result && "Unable to cast values of ListArray to StringArray");
                        return result;
                    }
                }
            };

            ARROW_ASSIGN_OR_RAISE(auto flat_strings, GetFlatArray());
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(flat_strings->type()));

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                // Copy sections of data from the Arrow Buffer and write
                try {
                    auto carray = casacore::Array<casacore::String>(it.GetShape());
                    auto * chunk_ptr = carray.data();
                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        auto sv = flat_strings->GetView(mit.GlobalOffset());
                        chunk_ptr[mit.ChunkOffset()] = casacore::String(std::begin(sv), std::end(sv));
                    }
                    column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteFixedColumn ",
                                                    GetTableColumn().columnDesc().name(),
                                                    ": ", e.what());
                }
            }
        } else {
            // Wrap Arrow Buffer in casacore Array
            auto nelements = map_.get().nElements();
            auto GetValueBuffer = [&]() -> arrow::Result<std::shared_ptr<arrow::Buffer>> {
                for(auto la=std::dynamic_pointer_cast<arrow::ListArray>(array_);;) {
                    if(auto next = std::dynamic_pointer_cast<arrow::ListArray>(la->values()); next) {
                        la = next;
                    } else {
                        auto result = la->values()->data()->buffers[1];
                        assert(result && "Unable to obtain value buffer of ListArray");
                        return result;
                    }
                }
            };

            ARROW_ASSIGN_OR_RAISE(auto buffer, GetValueBuffer());
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());
            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                // Copy sections of data from the Arrow Buffer and write
                try {
                    auto carray = casacore::Array<T>(it.GetShape());
                    auto chunk_ptr = carray.data();
                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        chunk_ptr[mit.ChunkOffset()] = buf_ptr[mit.GlobalOffset()];
                    }
                    column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteFixedColumn ",
                                                    GetTableColumn().columnDesc().name(),
                                                    ": ", e.what());
                }
            }
        }

        return arrow::Status::OK();
    }


private:
    inline arrow::Status FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) {
        if(arrow_dtype == arrow::utf8()) { return arrow::Status::OK(); }
        return arrow::Status::Invalid(arrow_dtype->ToString(), " incompatible with casacore::String");
    }

    template <typename T>
    arrow::Status CheckByteWidths() {
        // Check that the arrow type byte widths match up with the casacore byte widths
        // if(auto complex_dtype = std::dynamic_pointer_cast<ComplexType>(arrow_dtype)) {
        //     auto vdt = complex_dtype->value_type();
        //     if(vdt->byte_width() == -1 || 2*vdt->byte_width() != sizeof(T)) {
        //         return arrow::Status::Invalid(
        //             "2 x byte width of complex value type",
        //             vdt->ToString(), " (",
        //             2*vdt->byte_width(),
        //             ") != sizeof(T) (", sizeof(T), ")");
        //     }
        // } else if(arrow_dtype == arrow::utf8()) {
        //     return arrow::Status::OK();
        // } else if(arrow_dtype->byte_width() == -1 || arrow_dtype->byte_width() != sizeof(T)) {
        //     return arrow::Status::Invalid(
        //         arrow_dtype->ToString(), " byte width (",
        //         arrow_dtype->byte_width(),
        //         ") != sizeof(T) (", sizeof(T), ")");
        // }

        return arrow::Status::OK();
    }

    template <typename T>
    arrow::Status ConvertColumn() {
        ARROW_RETURN_NOT_OK(CheckByteWidths<T>());


        if(arrow::is_numeric(*array_->type())) {
            return WriteScalarColumn<T>();
        } else if(array_->type()->id() == arrow::Type::FIXED_SIZE_LIST) {
            return WriteFixedColumn<T>();
        } else if(arrow::is_var_length_list(*array_->type())) {
            return WriteVariableColumn<T>();
        }

        return arrow::Status::Invalid("Unhandled arrow type");
    };
};

} // namespace arcae

#endif // COLUMN_WRITE_VISITOR_H
