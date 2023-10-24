# distutils: language = c++
# cython: language_level = 3

from collections.abc import Iterable, MutableMapping
import cython
import json
from typing import Optional

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.memory cimport dynamic_pointer_cast, shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

import numpy as np
import pyarrow as pa
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

from pyarrow.lib cimport (
    pyarrow_wrap_table,
    pyarrow_wrap_array,
    pyarrow_wrap_data_type,
    pyarrow_wrap_table,
    pyarrow_unwrap_data_type,
    DataType,
    BaseExtensionType,
    ExtensionType,
    ExtensionArray)

from pyarrow.lib import (tobytes, frombytes)

from arcae.arrow_tables cimport (CCasaTable,
                                 CConfiguration,
                                 CComplexType,
                                 CMSDescriptor,
                                 CComplexDoubleArray,
                                 CComplexFloatArray,
                                 CComplexDoubleType,
                                 CComplexFloatType,
                                 CServiceLocator,
                                 COpenTable,
                                 CDefaultMS,
                                 CTaql,
                                 complex64,
                                 complex128,
                                 UINT_MAX)

def ms_descriptor(table: str, complete: bool = False) -> dict:
    table_desc = GetResultValue(CMSDescriptor(tobytes(table), complete))
    return json.loads(frombytes(table_desc))


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

            value_type = complex_type.get().value_type()

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

    def __init__(self):
        raise TypeError("This class cannot be instantiated directly.")

    def __enter__(self):
        return self

    def __exit__(self, etype, evalue, etraceback):
        self.close()

    @staticmethod
    def from_taql(taql: str) -> Table:
      cdef Table table = Table.__new__(Table)
      table.c_table = GetResultValue(CTaql(tobytes(taql)))
      return table

    @staticmethod
    def from_filename(filename: str) -> Table:
        cdef Table table = Table.__new__(Table)
        table.c_table = GetResultValue(COpenTable(tobytes(filename)))
        return table

    @staticmethod
    def ms_from_descriptor(
        filename: str,
        subtable: str = "MAIN",
        table_desc: Optional[dict] = None,
        dminfo: Optional[dict] = None
    ) -> Table:
        cdef Table table = Table.__new__(Table)
        json_table_desc = json.dumps(table_desc) if table_desc else "{}"
        json_dminfo = json.dumps(dminfo) if dminfo else "{}"
        table.c_table = GetResultValue(CDefaultMS(tobytes(filename),
                                                   tobytes(subtable),
                                                   tobytes(json_table_desc),
                                                   tobytes(json_dminfo)))
        return table

    def to_arrow(self, unsigned int startrow=0, unsigned int nrow=UINT_MAX, columns: list[str] | str = None):
        cdef:
            vector[string] cpp_columns
            shared_ptr[CArray] carray

        if isinstance(columns, str):
            columns = [columns]

        if columns:
            cpp_columns = [tobytes(c) for c in columns]

        ctable = GetResultValue(self.c_table.get().ToArrow(startrow, nrow, cpp_columns))

        return pyarrow_wrap_table(ctable)

    def getcol(self, column: str, unsigned int startrow=0, unsigned int nrow=UINT_MAX):
        cdef:
            shared_ptr[CArray] carray
            string cpp_column = tobytes(column)

        carray = GetResultValue(self.c_table.get().GetColumn(cpp_column, startrow, nrow))

        py_column = pyarrow_wrap_array(carray)

        if isinstance(py_column, (pa.ListArray, pa.LargeListArray)):
            raise TypeError(f"Can't convert variably shaped column {column} to numpy array")

        if isinstance(py_column, pa.NumericArray):
            return py_column.to_numpy(zero_copy_only=True)

        if isinstance(py_column, pa.StringArray):
            return py_column.to_numpy(zero_copy_only=False)

        if isinstance(py_column, pa.FixedSizeListArray):
            shape = [len(py_column)]
            nested_column = py_column
            zero_copy_only = True

            while True:
                if pa.types.is_primitive(nested_column.type):
                    break
                elif isinstance(nested_column, pa.StringArray):
                    zero_copy_only = False
                    break
                elif isinstance(nested_column, pa.FixedSizeListArray):
                    shape.append(nested_column.type.list_size)
                    nested_column = nested_column.flatten()
                else:
                    raise TypeError(f"Encountered invalid type {nested_column.type} "
                                    f"when converting column {column} from a "
                                    f"nested FixedSizeListArray to a numpy array. "
                                    f"Only FixedSizeListArrays or PrimitiveArrays "
                                    f"are supported")

            nested_column = nested_column.to_numpy(zero_copy_only=zero_copy_only)
            array = nested_column.reshape(tuple(shape))

            # Convert to complex if necessary
            if "COMPLEX" in self.getcoldesc(column)["valueType"].upper():
                complex_dtype = np.result_type(array.dtype, np.complex64)
                array = array.view(complex_dtype)[..., 0]

            return array

        raise TypeError(f"Unhandled column type {py_column.type}")


    def nrow(self):
        return GetResultValue(self.c_table.get().nRow())

    def ncolumns(self):
        return GetResultValue(self.c_table.get().nColumns())

    def close(self):
        return GetResultValue(self.c_table.get().Close())

    def columns(self):
        return [frombytes(s) for s in GetResultValue(self.c_table.get().Columns())]

    def getcoldesc(self, column: str):
        col_desc = GetResultValue(self.c_table.get().GetColumnDescriptor(tobytes(column)))
        return json.loads(frombytes(col_desc)).popitem()[1]

    def tabledesc(self):
        table_desc = GetResultValue(self.c_table.get().GetTableDescriptor())
        return json.loads(frombytes(table_desc)).popitem()[1]

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

        vector_result = GetResultValue(self.c_table.get().Partition(cpp_columns, cpp_sort_columns))

        for v in vector_result:
            table: Table = Table.__new__(Table)
            table.c_table = v
            result.append(table)

        return result

    def addrows(self, nrows: int):
        return  GetResultValue(self.c_table.get().AddRows(nrows))

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
