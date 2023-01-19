#pragma once

#include <Python.h>
#include <pybind11/pybind11.h>

#include <arrow/api.h>

#include "complex_type.h"

namespace PYBIND11_NAMESPACE {
namespace detail {

template <typename AT>
struct arrow_array_caster {
public:
    PYBIND11_TYPE_CASTER(std::shared_ptr<AT>, _("pyarrow::Array"));

    bool load(handle src, bool) {
        PyObject * object = src.ptr();

        if(!arrow::py::is_array(object)) {
            return false;
        }

        auto result = arrow::py::unwrap_array(object);
        if(!result.ok()) {
            return false;
        }

        value = std::static_pointer_cast<AT>(result.ValueOrDie());
        return true;
    }

    static handle cast(std::shared_ptr<AT> src, return_value_policy /* policy */, handle /* parent */) {
        return arrow::py::wrap_array(src);
    }
};


template <typename DT>
struct arrow_datatype_caster {
    PYBIND11_TYPE_CASTER(std::shared_ptr<DT>, _("std::shared_ptr<DataType>"));

    bool load(handle src, bool) {
        PyObject * object = src.ptr();

        if(!arrow::py::is_data_type(object)) {
            return false;
        }

        auto result = arrow::py::unwrap_data_type(object);
        if(!result.ok()) {
            return false;
        }

        value = std::dynamic_pointer_cast<DT>(result.ValueOrDie());
        return value != nullptr;
    }

    static handle cast(std::shared_ptr<DT> src, return_value_policy /* policy */, handle /* parent */) {
        return arrow::py::wrap_data_type(src);
    }
};

template <> struct type_caster<std::shared_ptr<ComplexDoubleType>> : arrow_datatype_caster<ComplexDoubleType> {};
template <> struct type_caster<std::shared_ptr<ComplexFloatType>> : arrow_datatype_caster<ComplexFloatType> {};

template <> struct type_caster<std::shared_ptr<ComplexDoubleArray>> : arrow_array_caster<ComplexDoubleArray> {};
template <> struct type_caster<std::shared_ptr<ComplexFloatArray>> : arrow_array_caster<ComplexFloatArray> {};


template <> struct type_caster<std::shared_ptr<arrow::Table>> {
public:
    PYBIND11_TYPE_CASTER(std::shared_ptr<arrow::Table>, _("std::shared_ptr<arrow::Table>"));

    bool load(handle src, bool) {
        PyObject * object = src.ptr();

        if(!arrow::py::is_table(object)) {
            return false;
        }

        auto result = arrow::py::unwrap_table(object);
        if(!result.ok()) {
            return false;
        }

        value = std::dynamic_pointer_cast<arrow::Table>(result.ValueOrDie());
        return true;
    }

    static handle cast(std::shared_ptr<arrow::Table> src, return_value_policy /* policy */, handle /* parent */) {
        return arrow::py::wrap_table(src);
    }
};

}

}
