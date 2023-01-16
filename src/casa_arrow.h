#include <arrow/api.h>

arrow::Result<std::shared_ptr<arrow::Table>> open_table(const std::string & filename);
