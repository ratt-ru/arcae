#include <mutex>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <arrow/python/pyarrow.h>

#include "type_converters.h"
#include "casa_arrow.h"

PYBIND11_MODULE(pytable, m)
{
    arrow::py::import_pyarrow();

    m.def("open_table", [](std::string filename) {
        pybind11::gil_scoped_release release;
        auto result = open_table(filename);

        if(!result.ok()) {
            throw pybind11::value_error(result.status().ToString());
        }

        return result.ValueOrDie();
    });
}
