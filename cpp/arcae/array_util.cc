#include "arcae/array_util.h"
#include "arcae/configuration.h"
#include "arcae/service_locator.h"

#include <memory>

#include <arrow/api.h>
#include <arrow/util/logging.h>

using ::arrow::Array;
using ::arrow::Status;

namespace arcae {

Status ValidateArray(const std::shared_ptr<arrow::Array> & array) {
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


} // namespace arcae