#include "arcae/array_util.h"
#include "arcae/configuration.h"
#include "arcae/service_locator.h"

#include <memory>

#include <arrow/api.h>
#include <arrow/util/logging.h>

using ::arrow::Array;
using ::arrow::Result;
using ::arrow::Status;

namespace arcae {

Status ValidateArray(const std::shared_ptr<Array> & array) {
    auto & config = ServiceLocator::configuration();
    auto validation_level = config.GetDefault("validation-level", "basic");

    if(validation_level == "basic") {
        return array->Validate();
    } else if(validation_level == "full") {
        return array->ValidateFull();
    } else if(validation_level == "none") {
        return Status::OK();
    } else {
        ARROW_LOG(WARNING) << "Invalid validation-level=" << validation_level
                            << ". No array construction validation will be performed";
        return Status::OK();
    }
}



Result<std::shared_ptr<Array>>
GetFlatArray(std::shared_ptr<Array> data, bool nulls) {
    if(!nulls && data->null_count() > 0) {
        return arrow::Status::Invalid("Null values were encountered "
                                      "during array flattening.");
    }

    while(true) {
        switch(data->type_id()) {
            case arrow::Type::LARGE_LIST:
            {
                auto lla = std::dynamic_pointer_cast<arrow::LargeListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, lla->Flatten());
                break;
            }
            case arrow::Type::LIST:
            {
                auto la = std::dynamic_pointer_cast<arrow::ListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, la->Flatten());
                break;
            }
            case arrow::Type::FIXED_SIZE_LIST:
            {
                auto fsl = std::dynamic_pointer_cast<arrow::FixedSizeListArray>(data);
                ARROW_ASSIGN_OR_RAISE(data, fsl->Flatten());
                break;
            }
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
                return data;
            default:
                return arrow::Status::TypeError(
                    "Flattening of type ", data->type(),
                    " is not supported");
        }
    }
}

} // namespace arcae