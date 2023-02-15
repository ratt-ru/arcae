#include "safe_table_proxy.h"

arrow::Result<std::shared_ptr<arrow::Table>> open_table(const std::string & filename) {
    ARROW_ASSIGN_OR_RAISE(auto test_proxy, SafeTableProxy::Make(filename));
    ARROW_ASSIGN_OR_RAISE(auto nrow, test_proxy->nrow());
    return test_proxy->read_table(0, nrow);
}
