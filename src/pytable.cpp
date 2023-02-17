#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <arrow/python/pyarrow.h>

#include "safe_table_proxy.h"
#include "type_converters.h"

namespace py = pybind11;


template<typename T> struct unwrap_result {};
template<typename T> struct unwrap_result<arrow::Result<T>> { using type = T ;};
template<typename T> using unwrap_result_t = typename unwrap_result<T>::type;

template <class F, class... Args>
unwrap_result_t<std::invoke_result_t<F, Args...>>
result_wrapper(F && f, Args &&... args)
{
    py::gil_scoped_release();
    auto result = std::invoke(std::forward<F>(f), std::forward<Args>(args)...);

    if(!result.ok()) {
        throw py::value_error(result.status().ToString());
    }

    return result.ValueOrDie();
}

PYBIND11_MODULE(_pytable, m)
{
    arrow::py::import_pyarrow();

    pybind11::class_<SafeTableProxy, std::shared_ptr<SafeTableProxy>>(m, "Table")
    .def(py::init([](std::string filename)
        { return result_wrapper(&SafeTableProxy::Make, filename); }))
    .def("nrow", [](const SafeTableProxy & table)
        { return result_wrapper(&SafeTableProxy::nrow, table); })
    .def("columns", [](const SafeTableProxy & table)
        { return result_wrapper(&SafeTableProxy::columns, table); })
    .def("ncolumns", [](const SafeTableProxy & table)
        { return result_wrapper(&SafeTableProxy::ncolumns, table); })
    .def("read_table", [](const SafeTableProxy & table)
        { return result_wrapper([&]() { return table.read_table(); }); })
    .def("read_table", [](const SafeTableProxy & table, casacore::uInt startrow, casacore::uInt nrow)
        { return result_wrapper([&]() { return table.read_table(startrow, nrow); }); })
    .def("close", [](SafeTableProxy & table)
        { return result_wrapper(&SafeTableProxy::close, table); });
}
