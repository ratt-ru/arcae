#ifndef ARCAE_TABLE_FACTORY_H
#define ARCAE_TABLE_FACTORY_H

#include <memory>
#include <string>

#include <arrow/result.h>

#include "safe_table_proxy.h"

namespace arcae {

arrow::Result<std::shared_ptr<SafeTableProxy>> open_table(const std::string & filename);

} // namespace arcae


#endif // ARCAE_TABLE_FACTORY_H
