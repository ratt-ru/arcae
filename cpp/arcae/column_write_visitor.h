#ifndef COLUMN_WRITE_VISITOR_H
#define COLUMN_WRITE_VISITOR_H

#include <functional>
#include <map>
#include <memory>
#include <type_traits>

#include <arrow/array.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <casacore/casa/aipstype.h>
#include <casacore/casa/aipsxtype.h>
#include <casacore/casa/BasicSL/String.h>
#include <casacore/casa/Exceptions/Error.h>
#include <casacore/casa/Utilities/DataType.h>
#include <casacore/tables/Tables.h>

#include "arcae/casa_visitors.h"
#include "arcae/column_write_map.h"

namespace arcae {

class ColumnWriteVisitor : public CasaTypeVisitor {
private:
    // Map of casacore datatypes to arrow datatypes
    static const std::map<casacore::DataType, std::shared_ptr<arrow::DataType>> arrow_type_map_;

public:
    std::reference_wrapper<const ColumnWriteMap> map_;
    arrow::MemoryPool * pool_;

public:
    explicit ColumnWriteVisitor(
        const ColumnWriteMap & column_map,
        arrow::MemoryPool * pool=arrow::default_memory_pool()) :
            map_(std::cref(column_map)),
            pool_(pool) {};
    virtual ~ColumnWriteVisitor() = default;


#define VISIT(CASA_TYPE) \
    virtual arrow::Status Visit##CASA_TYPE() override;

    VISIT_CASA_TYPES(VISIT)
#undef VISIT

    arrow::Status Visit();
    arrow::Result<std::shared_ptr<arrow::Array>> MaybeCastFlatArray(const std::shared_ptr<arrow::Array> & data);


    // Write arrow array to the CASA column
    template <typename T>
    arrow::Status WriteColumn() {
        try {
            if(map_.get().nDim() == 1) {
                return WriteScalarColumn<T>();
            } else if(map_.get().IsFixedShape()) {
                return WriteFixedColumn<T>();
            } else {
                return WriteVariableColumn<T>();
            }
        } catch(casacore::AipsError & e) {
            return arrow::Status::Invalid("WriteColumn ",
                                          GetTableColumn().columnDesc().name(),
                                            ": ", e.what());
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
        ARROW_ASSIGN_OR_RAISE(flat_array, MaybeCastFlatArray(flat_array));
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
                } catch(casacore::AipsError & e) {
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
            auto span = buffer->span_as<T>();

            if(map_.get().IsSimple()) {
                auto carray = casacore::Array<T>(shape, std::remove_const_t<T *>(span.data()), casacore::SHARE);

                try {
                    // Dump column data straight from the Arrow Buffer
                    column.putColumnRange(map_.get().RangeBegin().GetRowSlicer(),
                                          carray);
                } catch(casacore::AipsError & e) {
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
                            chunk_ptr[mit.ChunkOffset()] = span[mit.GlobalOffset()];
                        }
                        column.putColumnRange(it.GetRowSlicer(), carray);
                    } catch(casacore::AipsError & e) {
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
        ARROW_ASSIGN_OR_RAISE(flat_array, MaybeCastFlatArray(flat_array));
        auto nelements = map_.get().nElements();

        if(shape.product() != nelements) {
            return arrow::Status::Invalid("Shape ", shape, " elements ", shape.nelements(),
                                            " doesn't match map elements ", nelements);
        }


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
                } catch(casacore::AipsError & e) {
                    return arrow::Status::Invalid("WriteFixedColumn ",
                                                    GetTableColumn().columnDesc().name(),
                                                    ": ", e.what());
                }
            }
        } else {
            // Get Arrow Array buffer
            std::shared_ptr<arrow::Buffer> buffer = flat_array->data()->buffers[1];
            auto span = buffer->span_as<T>();
            ARROW_RETURN_NOT_OK(CheckElements(buffer->size() / sizeof(T), nelements));
            ARROW_RETURN_NOT_OK(CheckElements(span.size(), nelements));

            if(map_.get().IsSimple()) {
                auto carray = casacore::Array<T>(shape, std::remove_const_t<T *>(span.data()), casacore::SHARE);

                try {
                    // Dump column data straight into the Arrow Buffer
                    column.putColumnRange(map_.get().RangeBegin().GetRowSlicer(),
                                          map_.get().RangeBegin().GetSectionSlicer(),
                                          carray);
                } catch(casacore::AipsError & e) {
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
                            chunk_ptr[mit.ChunkOffset()] = span[mit.GlobalOffset()];
                        }
                        column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                    } catch(casacore::AipsError & e) {
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
        ARROW_ASSIGN_OR_RAISE(flat_array, MaybeCastFlatArray(flat_array));
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
                } catch(casacore::AipsError & e) {
                    return arrow::Status::Invalid("WriteVariableColumn ",
                                                  GetTableColumn().columnDesc().name(),
                                                  ": ", e.what());
                }
            }
        } else {
            // Get Arrow Array buffer
            auto buffer = flat_array->data()->buffers[1];
            ARROW_RETURN_NOT_OK(CheckElements(flat_array->length(), nelements));
            auto span = buffer->span_as<T>();

            for(auto it = map_.get().RangeBegin(); it != map_.get().RangeEnd(); ++it) {
                // Copy sections of data from the Arrow Buffer and write
                try {
                    auto carray = casacore::Array<T>(it.GetShape());
                    auto chunk_ptr = carray.data();
                    for(auto mit = it.MapBegin(); mit != it.MapEnd(); ++mit) {
                        chunk_ptr[mit.ChunkOffset()] = span[mit.GlobalOffset()];
                    }
                    column.putColumnRange(it.GetRowSlicer(), it.GetSectionSlicer(), carray);
                } catch(casacore::AipsError & e) {
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
