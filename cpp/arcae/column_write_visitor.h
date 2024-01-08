#ifndef COLUMN_WRITE_VISITOR_H
#define COLUMN_WRITE_VISITOR_H

#include <functional>
#include <memory>

#include <arrow/array/array_nested.h>
#include <arrow/util/logging.h>  // IWYU pragma: keep
#include <arrow/result.h>

#include <casacore/tables/Tables.h>

#include "arcae/casa_visitors.h"
#include "arcae/column_read_map.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"

namespace arcae {

class ColumnWriteVisitor : public CasaTypeVisitor {
public:
    using ShapeVectorType = std::vector<casacore::IPosition>;

public:
    std::reference_wrapper<const ColumnReadMap> map_;
    arrow::MemoryPool * pool_;
    std::shared_ptr<arrow::Array> array_;

public:
    explicit ColumnWriteVisitor(
        const ColumnReadMap & column_map,
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

    // Convert the
    template <typename T>
    arrow::Status WriteColumn() {
        if(map_.get().nDim() == 1) {
            return WriteScalarColumn<T>();
        } else if(map_.get().IsFixedShape()) {
            return WriteFixedColumn<T>();
        } else {
            return WriteVariableColumn<T>();
        }

    };

private:
    const casacore::TableColumn & GetTableColumn() const {
        return map_.get().column_.get();
    }


    arrow::Result<std::shared_ptr<arrow::Array>> GetFlatArray(bool nulls=false) const;
    arrow::Status CheckElements(std::size_t map_size, std::size_t data_size) const;
    arrow::Status FailIfNotUTF8(const std::shared_ptr<arrow::DataType> & arrow_dtype) const;

    template <typename T>
    arrow::Status WriteScalarColumn() {
        auto column = casacore::ScalarColumn<T>(GetTableColumn());
        column.setMaximumCacheSize(1);
        ARROW_ASSIGN_OR_RAISE(auto flat_array, GetFlatArray());
        auto nelements = map_.get().nElements();

        if constexpr(std::is_same_v<T, casacore::String>) {
            auto flat_strings = std::dynamic_pointer_cast<arrow::StringArray>(flat_array);
            assert(flat_strings != nullptr && "Unable to cast array to StringArray");
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(flat_strings->type()));
            ARROW_RETURN_NOT_OK(CheckElements(flat_strings->length(), nelements));

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                // Copy sections of data from the Arrow Buffer and write
                try {
                    auto carray = casacore::Array<casacore::String>(it.GetShape());
                    auto * chunk_ptr = carray.data();
                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        auto sv = flat_strings->GetView(mit.GlobalOffset());
                        chunk_ptr[mit.ChunkOffset()] = casacore::String(std::begin(sv), std::end(sv));
                    }
                    column.putColumnRange(it.GetRowSlicer(), carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteScalarColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }
        } else {
            // Get Arrow Array buffer
            auto buffer = flat_array->data()->buffers[1];
            ARROW_ASSIGN_OR_RAISE(auto shape, map_.get().GetOutputShape());
            ARROW_RETURN_NOT_OK(CheckElements(buffer->size() / sizeof(T), nelements));
            auto * buf_ptr = reinterpret_cast<T *>(buffer->mutable_data());

            if(map_.get().IsSimple()) {
                auto carray = casacore::Array<T>(shape, buf_ptr, casacore::SHARE);

                try {
                    // Dump column data straight from the Arrow Buffer
                    column.putColumnRange(map_.get().RangeBegin().GetRowSlicer(),
                                          carray);
                } catch(std::exception & e) {
                    return arrow::Status::Invalid("WriteScalarColumn ",
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
                        column.putColumnRange(it.GetRowSlicer(), carray);
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
        column.setMaximumCacheSize(1);
        ARROW_ASSIGN_OR_RAISE(auto shape, map_.get().GetOutputShape());
        ARROW_ASSIGN_OR_RAISE(auto flat_array, GetFlatArray());
        auto nelements = map_.get().nElements();

        if constexpr(std::is_same_v<T, casacore::String>) {
            auto flat_strings = std::dynamic_pointer_cast<arrow::StringArray>(flat_array);
            assert(flat_strings != nullptr && "Unable to cast array to StringArray");
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(flat_strings->type()));
            ARROW_RETURN_NOT_OK(CheckElements(flat_strings->length(), nelements));

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
            // Get Arrow Array buffer
            auto buffer = flat_array->data()->buffers[1];
            ARROW_RETURN_NOT_OK(CheckElements(buffer->size() / sizeof(T), nelements));
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
        column.setMaximumCacheSize(1);
        ARROW_ASSIGN_OR_RAISE(auto flat_array, GetFlatArray());
        auto nelements = map_.get().nElements();

        if constexpr(std::is_same_v<T, casacore::String>) {
            auto flat_strings = std::dynamic_pointer_cast<arrow::StringArray>(flat_array);
            assert(flat_strings != nullptr && "Unable to cast array to StringArray");
            ARROW_RETURN_NOT_OK(FailIfNotUTF8(flat_strings->type()));
            ARROW_RETURN_NOT_OK(CheckElements(flat_strings->length(), nelements));

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
                    return arrow::Status::Invalid("WriteVariableColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }
        } else {
            // Get Arrow Array buffer
            auto buffer = flat_array->data()->buffers[1];
            ARROW_RETURN_NOT_OK(CheckElements(buffer->size() / sizeof(T), nelements));
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
                    return arrow::Status::Invalid("WriteVariableColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }
        }

        return arrow::Status::OK();
    }
};

} // namespace arcae

#endif // COLUMN_WRITE_VISITOR_H
