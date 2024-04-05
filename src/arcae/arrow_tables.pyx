# distutils: language = c++
# cython: language_level = 3

from collections.abc import Iterable, MutableMapping
import ctypes
import cython
import json
from typing import Optional, Union

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.memory cimport dynamic_pointer_cast, shared_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

import numpy as np
import pyarrow as pa

cimport numpy as cnp

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

from pyarrow.lib cimport (
    pyarrow_wrap_table,
    pyarrow_wrap_array,
    pyarrow_wrap_data_type,
    pyarrow_wrap_table,
    pyarrow_unwrap_array,
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
                                 ColumnSelection,
                                 RowId,
                                 RowIds,
                                 Span,
                                 UINT_MAX)


DimIndex = Union[slice, list, np.ndarray]
FullIndex = Union[list[DimIndex], tuple[DimIndex]]

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


cdef class SelectionObj:
    """
    Helper object for generating a ColumnSelection

    Needed because vectors may need to be stored so
    that they stay live when referenced by span objects
    """
    cdef vector[vector[RowId]] vec_store
    cdef ColumnSelection selection

    def __init__(self, index: FullIndex = None):
        cdef RowId[::1] dim_memview
        cdef RowId i

        if index is None:
            pass
        elif isinstance(index, (tuple, list)):
            for d, dim_index in enumerate(index):
                if dim_index is None:
                    self.selection.push_back(RowIds())
                elif isinstance(dim_index, slice):
                    # Convert a slice object into a vector, and then a span of RowId
                    if dim_index.step is not None and dim_index.step != 1:
                        raise ValueError(f"slice step {dim_index.step} is not 1")

                    start: RowId = dim_index.start
                    stop: RowId = dim_index.stop

                    with nogil:
                        vec_index = vector[RowId](stop - start, 0)

                        for i in range(stop - start):
                            vec_index[i] = start + i

                        span = RowIds(&vec_index[0], vec_index.size())
                        self.vec_store.push_back(move(vec_index))
                        self.selection.push_back(move(span))
                elif isinstance(dim_index, list):
                    dim_memview = np.asarray(dim_index, np.int64)
                    span = RowIds(&dim_memview[0], dim_memview.shape[0])
                    self.selection.push_back(move(span))
                elif isinstance(dim_index, np.ndarray):
                    if dim_index.ndim != 1:
                        raise ValueError(f"Multi-dimensional ndarray received as index "
                                            f"in dimension {d}")
                    # Cast to uint64 if necessary
                    dim_memview = np.require(dim_index, dtype=np.int64, requirements=["C"])
                    span = RowIds(&dim_memview[0], dim_memview.shape[0])
                    self.selection.push_back(move(span))
                else:
                    raise TypeError(f"Invalid index type {type(dim_index)} "
                                    f"for dimension {d}")
        else:
            raise TypeError(f"index must be a tuple of "
                            f"(None, slices or numpy arrays), or None")



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
    def from_filename(filename: str, readonly=True) -> Table:
        cdef Table table = Table.__new__(Table)
        table.c_table = GetResultValue(COpenTable(tobytes(filename), readonly))
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

    def to_arrow(self, index: FullIndex = None, columns: list[str] | str = None):
        cdef:
            vector[string] cpp_columns
            shared_ptr[CArray] carray
            SelectionObj selobj = SelectionObj(index)

        if isinstance(columns, str):
            columns = [columns]

        if columns:
            cpp_columns = [tobytes(c) for c in columns]

        ctable = GetResultValue(self.c_table.get().ToArrow(selobj.selection, cpp_columns))

        return pyarrow_wrap_table(ctable)

    def getcol(self, column: str, index: FullIndex = None) -> np.ndarray:
        cdef string cpp_column = tobytes(column)
        cdef SelectionObj selobj = SelectionObj(index)

        carray = GetResultValue(self.c_table.get().GetColumn(cpp_column, selobj.selection))
        py_column = pyarrow_wrap_array(carray)
        return self._arrow_to_numpy(column, py_column)


    def putcol(self, column: str, data: np.ndarray, index: FullIndex = None):
        cdef string cpp_column = tobytes(column)
        cdef SelectionObj selobj = SelectionObj(index)
        cdef shared_ptr[CArray] carray = pyarrow_unwrap_array(self._numpy_to_arrow(data))
        return GetResultValue(self.c_table.get().PutColumn(cpp_column, selobj.selection, carray))

    def _numpy_to_arrow(self, data: np.ndarray) -> pa.array:
        """ Covert numpy array into a nested FixedSizeListArrays """
        shape = data.shape
        np_array = data.ravel()

        # Add an extra dimension of 2 elements and cast to the real dtype
        if issubclass(np_array.dtype.type, np.complexfloating):
            shape += (2,)
            np_array = np_array.view(np_array.real.dtype)

        # Convert to pyarrow array
        pa_array = pa.array(np_array)

        # Nested by secondary dimensions
        for dim in reversed(shape[1:]):
            pa_array = pa.FixedSizeListArray.from_arrays(pa_array, dim)

        return pa_array


    def _arrow_to_numpy(self, column: str, array: pa.array):
        """ Attempt to convert an arrow array to a numpy array """
        if isinstance(array, (pa.ListArray, pa.LargeListArray)):
            raise TypeError(f"Can't convert variably shaped column {column} to numpy array")

        if isinstance(array, pa.NumericArray):
            return array.to_numpy(zero_copy_only=True)

        if isinstance(array, pa.StringArray):
            return array.to_numpy(zero_copy_only=False)

        if isinstance(array, pa.FixedSizeListArray):
            shape = [len(array)]
            nested_column = array
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

        raise TypeError(f"Unhandled column type {array.type}")



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
