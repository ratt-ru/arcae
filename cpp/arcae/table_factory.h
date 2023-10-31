#ifndef ARCAE_TABLE_FACTORY_H
#define ARCAE_TABLE_FACTORY_H

#include <memory>
#include <string>

#include <arrow/result.h>

#include "arcae/safe_table_proxy.h"

namespace arcae {

arrow::Result<std::shared_ptr<SafeTableProxy>> OpenTable(
                                const std::string & filename,
                                bool readonly=true);
arrow::Result<std::shared_ptr<SafeTableProxy>> DefaultMS(
                                const std::string & name,
                                const std::string & subtable="MAIN",
                                const std::string & json_table_desc="{}",
                                const std::string & json_dminfo="{}");
arrow::Result<std::shared_ptr<SafeTableProxy>> Taql(
                                const std::string & taql);


} // namespace arcae


#endif // ARCAE_TABLE_FACTORY_H
