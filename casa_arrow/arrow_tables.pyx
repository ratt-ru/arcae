# distutils: language = c++
# cython: language_level = 3

from collections import Iterable
from collections.abc import MutableMapping
import cython
from cython.operator cimport dereference as deref

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.memory cimport dynamic_pointer_cast, shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

import pyarrow as pa
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

from pyarrow.lib cimport (
    pyarrow_wrap_table,
    pyarrow_wrap_data_type,
    pyarrow_wrap_table,
    pyarrow_unwrap_data_type,
    DataType,
    BaseExtensionType,
    ExtensionType,
    ExtensionArray)

from pyarrow.lib import (tobytes, frombytes)

from casa_arrow.casa_table cimport (CCasaTable,
                                    CConfiguration,
                                    CComplexType,
                                    CComplexDoubleArray,
                                    CComplexFloatArray,
                                    CComplexDoubleType,
                                    CComplexFloatType,
                                    CServiceLocator,
                                    complex64,
                                    complex128,
                                    UINT_MAX)


cdef class ComplexType(BaseExtensionType):
    cdef void init(self, const shared_ptr[CDataType]& type) except *:
        BaseExtensionType.init(self, type)

    def value_type(self):
        cdef:
            shared_ptr[CComplexType] complex_type
            shared_ptr[CDataType] value_type

        with nogil:
            complex_type = dynamic_pointer_cast[CComplexType, CDataType](self.sp_type)

            if not <bint>complex_type:
                raise ValueError("Unable to downcast CDataType to CComplexType")

            value_type = deref(complex_type).value_type()

        return pyarrow_wrap_data_type(value_type)

cdef class ComplexDoubleArray(ExtensionArray):
    pass

cdef class ComplexDoubleType(ComplexType):
    def __init__(self):
        ComplexType.init(self, complex128())

    def __arrow_ext_class__(self):
        return ComplexDoubleArray

cdef class ComplexFloatType(ComplexType):
    def __init__(self):
        ComplexType.init(self, complex64())

    def __arrow_ext_class__(self):
        return ComplexFloatArray


cdef class ComplexFloatArray(ExtensionArray):
    pass

# Create a Cython extension type around the CCasaTable C++ instance
cdef class Table:
    cdef shared_ptr[CCasaTable] c_table

    def __init__(self, filename):
        cdef string cfilename = tobytes(filename)

        with nogil:
            self.c_table = GetResultValue(CCasaTable.Make(cfilename))

    def to_arrow(self, unsigned int startrow=0, unsigned int nrow=UINT_MAX, columns: list[str] | str = None):
        cdef:
            shared_ptr[CTable] ctable
            vector[string] cpp_columns

        if isinstance(columns, str):
            columns = [columns]

        if columns:
            cpp_columns = [tobytes(c) for c in columns]

        with nogil:
            ctable = GetResultValue(deref(self.c_table).to_arrow(startrow, nrow, cpp_columns))

        return pyarrow_wrap_table(ctable)

    def nrow(self):
        return GetResultValue(self.c_table.get().nrow())

    def ncolumns(self):
        return GetResultValue(self.c_table.get().ncolumns())

    def close(self):
        return GetResultValue(self.c_table.get().close())

    def columns(self):
        return [frombytes(s) for s in GetResultValue(self.c_table.get().columns())]

    def partition(self, columns, sort_columns=None):
        cdef vector[shared_ptr[CCasaTable]] vector_result

        if isinstance(sort_columns, str):
            sort_columns = [sort_columns]

        if isinstance(columns, str):
            columns = [columns]

        if (not isinstance(columns, Iterable) or
            not all(isinstance(s, str) for s in columns)):
                raise TypeError(f"type(columns) {columns} must be a str "
                                f"or a list of str")

        cpp_columns: vector[string] = [tobytes(c) for c in columns]
        cpp_sort_columns: vector[string] = [] if sort_columns is None else [tobytes(c) for c in sort_columns]
        result = []

        with nogil:
            vector_result = GetResultValue(self.c_table.get().partition(cpp_columns, cpp_sort_columns))

        for v in vector_result:
            table: Table = Table.__new__(Table)
            table.c_table = v
            result.append(table)

        return result


class Configuration(MutableMapping):
    def __getitem__(self, key: str):
        c_key: string = tobytes(key)

        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()
            result: CResult[string] = config.Get(c_key)

        if result.ok():
            return frombytes(GetResultValue(result))

        raise KeyError(key)

    def __setitem__(self, key: str, item: str):
        c_key: string = tobytes(key)
        c_item: string = tobytes(item)

        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()
            config.Set(c_key, c_item)

    def __delitem__(self, key: str):
        c_key: string = tobytes(key)

        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()
            result: CResult[bool] = config.Delete(c_key)

        if not result.ok():
            raise KeyError(key)

    def __iter__(self):
        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()
            keys: vector[string] = config.GetKeys()

        return iter([frombytes(k) for k in keys])

    def __len__(self):
        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()

        return config.Size()
