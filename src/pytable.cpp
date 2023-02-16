#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <arrow/python/pyarrow.h>

#include "safe_table_proxy.h"
#include "type_converters.h"

namespace py = pybind11;

PYBIND11_MODULE(pytable, m)
{
    arrow::py::import_pyarrow();

    pybind11::class_<SafeTableProxy, std::shared_ptr<SafeTableProxy>>(m, "SafeTableProxy")
    .def(py::init([](std::string filename)
        { py::gil_scoped_release rgil; return SafeTableProxy::Make(filename).ValueOrDie(); }))
    .def("nrow", [](const SafeTableProxy & table)
        { py::gil_scoped_release rgil; return table.nrow().ValueOrDie(); })
    .def("columns", [](const SafeTableProxy & table)
        { py::gil_scoped_release rgil; return table.columns().ValueOrDie(); })
    .def("ncolumns", [](const SafeTableProxy & table)
        { py::gil_scoped_release rgil; return table.ncolumns().ValueOrDie(); })
    .def("read_table", [](const SafeTableProxy & table)
        { py::gil_scoped_release rgil; return table.read_table().ValueOrDie(); })
    .def("read_table", [](const SafeTableProxy & table, casacore::uInt startrow, casacore::uInt nrow)
        { py::gil_scoped_release rgil; return table.read_table(startrow, nrow).ValueOrDie(); })
    .def("close", [](SafeTableProxy & table)
        { py::gil_scoped_release rgil; table.close().ValueOrDie(); });
}
