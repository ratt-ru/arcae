# distutils: language = c++
# cython: language_level = 3

from collections.abc import MutableMapping, Sequence
import cython
import json
from typing import Any, Dict, List, Union

from libcpp cimport bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.utility cimport move
from libcpp.vector cimport vector

import numpy as np
import pyarrow as pa

from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

from pyarrow.lib cimport (
    pyarrow_wrap_table,
    pyarrow_wrap_array,
    pyarrow_wrap_table,
    pyarrow_unwrap_array)

from pyarrow.lib import (tobytes, frombytes)

from arcae.lib.arrow_tables cimport (
    CCasaTable,
    CConfiguration,
    CMSDescriptor,
    CServiceLocator,
    COpenTable,
    CDefaultMS,
    CSelection,
    CSelectionBuilder,
    CTaql,
    IndexType)


DimIndex = Union[slice, list, np.ndarray]
FullIndex = Union[list[DimIndex], tuple[DimIndex]]


def ms_descriptor(table: str, complete: bool = False) -> Dict:
    cdef string ctable = tobytes(table)

    with nogil:
        table_desc = GetResultValue(CMSDescriptor(ctable, complete))

    return json.loads(frombytes(table_desc))


cdef CSelection build_selection(index: FullIndex = None):
    cdef CSelectionBuilder builder = CSelectionBuilder().Order(b"C")

    if index is None:
        return builder.Build()
    elif not isinstance(index, (tuple, list)):
        raise TypeError(
            "index must be a tuple of "
            "(None, slices or numpy arrays), or None")

    for d, dim_index in enumerate(index):
        if dim_index is None:
            builder.AddEmpty()
        elif isinstance(dim_index, slice):
            if dim_index == slice(None):
                builder.AddEmpty()
                continue

            # Convert a slice object into a vector, and then a span of RowId
            if dim_index.step is not None and dim_index.step != 1:
                raise ValueError(f"slice step {dim_index.step} is not 1")

            start: IndexType = dim_index.start
            stop: IndexType = dim_index.stop

            with nogil:
                vec_index: Index = Index(stop - start, 0)

                i: IndexType = 0
                for i in range(stop - start):
                    vec_index[i] = start + i

                builder.Add(move(vec_index))
        elif isinstance(dim_index, list):
            vec_index: Index = dim_index
            builder.Add(move(vec_index))
        elif isinstance(dim_index, np.ndarray):
            if dim_index.ndim != 1:
                raise ValueError(
                    f"Multi-dimensional ndarray received "
                    f"as index in dimension {d}")
            if dim_index.dtype == np.int64:
                dim_array_view: IndexType [:] = dim_index
                builder.Add(IndexSpan(&dim_array_view[0], dim_array_view.shape[0]))
            else:
                vec_index: Index = dim_index
                builder.Add(move(vec_index))
        else:
            raise TypeError(f"Invalid index type {type(dim_index)} "
                            f"for dimension {d}")

    return builder.Build()

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
    def from_taql(
        taql: str,
        tables: Union[Sequence[Table], Table] | None = None
    ) -> Table:
        cdef:
            Table table = Table.__new__(Table)
            vector[shared_ptr[CCasaTable]] ctables
            string ctaql = tobytes(taql)

        if tables is not None:
            if isinstance(tables, Table):
                ctables.push_back((<Table?> tables).c_table)
            else:
                for t in tables:
                    ctables.push_back((<Table?> t).c_table)

        with nogil:
            table.c_table = GetResultValue(CTaql(ctaql, ctables))

        return table

    @staticmethod
    def from_filename(
        filename: str,
        ninstances: int = 1,
        readonly: bool = True,
        lockoptions: Union[str, dict] = "auto"
    ) -> Table:
        cdef:
            string cfilename = tobytes(filename)
            size_t cninstances = ninstances
            Table table = Table.__new__(Table)

        if isinstance(lockoptions, str):
            lockoptions = f"{{\"option\": \"{lockoptions}\"}}"
        elif isinstance(lockoptions, dict):
            lockoptions = json.dumps(lockoptions)
        else:
            raise TypeError(f"Invalid lockoptions type {lockoptions}")

        clockoptions: string = tobytes(lockoptions)

        with nogil:
            table.c_table = GetResultValue(
                COpenTable(cfilename, cninstances, readonly, clockoptions)
            )
        return table

    @staticmethod
    def ms_from_descriptor(
        filename: str,
        subtable: str = "MAIN",
        table_desc: Dict | None = None,
        dminfo: Dict | None = None
    ) -> Table:
        cdef:
            Table table = Table.__new__(Table)
            string cfilename = tobytes(filename)
            string csubtable = tobytes(subtable)
        json_table_desc = json.dumps(table_desc) if table_desc else "{}"
        json_dminfo = json.dumps(dminfo) if dminfo else "{}"

        cjson_table_desc: string = tobytes(json_table_desc)
        cjson_dm_info: string = tobytes(json_dminfo)

        with nogil:
            table.c_table = GetResultValue(CDefaultMS(cfilename,
                                                      csubtable,
                                                      cjson_table_desc,
                                                      cjson_dm_info))
        return table

    def to_arrow(
        self,
        index: FullIndex | None = None,
        columns: Union[list[str], str] = None
    ) -> pa.Table:
        cdef:
            vector[string] cpp_columns
            CSelection selection = build_selection(index)

        if isinstance(columns, str):
            columns = [columns]

        if columns:
            cpp_columns = [tobytes(c) for c in columns]

        with nogil:
            ctable = GetResultValue(self.c_table.get().ToArrow(selection, cpp_columns))

        return pyarrow_wrap_table(ctable)

    def name(self) -> str:
        """Returns the directory containing the table"""
        with nogil:
            cname = GetResultValue(self.c_table.get().Name())
        return frombytes(cname)

    def getcol(
        self,
        column: str,
        index: FullIndex | None = None,
        result: np.ndarray | None = None
    ) -> np.ndarray:
        cdef:
            string cpp_column = tobytes(column)
            CSelection selection = build_selection(index)
            shared_ptr[CArray] cpp_result

        if result is not None:
            pa_result = self._numpy_to_arrow(result)
            cpp_result = pyarrow_unwrap_array(pa_result)

        with nogil:
            carray = GetResultValue(
                self.c_table.get().GetColumn(cpp_column, selection, cpp_result)
            )

        py_column = pyarrow_wrap_array(carray)
        return self._arrow_to_numpy(column, py_column)

    def putcol(
        self,
        column: str,
        data: np.ndarray,
        index: FullIndex | None = None
    ):
        cdef:
            string cpp_column = tobytes(column)
            CSelection selection = build_selection(index)
            shared_ptr[CArray] carray = pyarrow_unwrap_array(self._numpy_to_arrow(data))

        with nogil:
            GetResultValue(self.c_table.get().PutColumn(cpp_column, carray, selection))

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

    def _arrow_to_numpy(
        self,
        column: str,
        array: pa.array
    ) -> np.ndarray:
        """ Attempt to convert an arrow array to a numpy array """
        if isinstance(array, (pa.ListArray, pa.LargeListArray)):
            raise TypeError(
                f"Can't convert variably shaped "
                f"column {column} to numpy array")

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

    def lockoptions(self) -> Dict[str, Any]:
        with nogil:
            lock_opts = GetResultValue(self.c_table.get().GetLockOptions())

        return json.loads(lock_opts)

    def nrow(self) -> int:
        with nogil:
            nrow = GetResultValue(self.c_table.get().nRows())

        return nrow

    def ncolumns(self) -> int:
        with nogil:
            ncolumns = GetResultValue(self.c_table.get().nColumns())

        return ncolumns

    def close(self):
        with nogil:
            GetResultValue(self.c_table.get().Close())

    def columns(self) -> List[str]:
        with nogil:
            columns = GetResultValue(self.c_table.get().Columns())
        return [frombytes(s) for s in columns]

    def getcoldesc(self, column: str) -> Dict[str, Any]:
        cdef string ccolumn = tobytes(column)
        with nogil:
            col_desc = GetResultValue(self.c_table.get().GetColumnDescriptor(ccolumn))
        return json.loads(frombytes(col_desc)).popitem()[1]

    def tabledesc(self) -> Dict[str, Any]:
        with nogil:
            table_desc = GetResultValue(self.c_table.get().GetTableDescriptor())
        return json.loads(frombytes(table_desc))

    def getdminfo(self, column: str | None = None) -> Dict[str, Any]:
        with nogil:
            cdminfo = GetResultValue(self.c_table.get().GetDataManagerInfo())

        dminfo = json.loads(frombytes(cdminfo))

        if column is None:
            return dminfo

        return dminfo[column]

    def addrows(self, nrows: int):
        cdef int cnrows = nrows
        with nogil:
            GetResultValue(self.c_table.get().AddRows(cnrows))

    def addcols(self, columndescs: Dict, dminfo: Dict | None):
        cdef:
            string cjson_columndescs = tobytes(json.dumps(columndescs))
            string cjson_dminfo = tobytes(json.dumps(dminfo) if dminfo else "{}")

        with nogil:
            GetResultValue(
                self.c_table.get().AddColumns(cjson_columndescs, cjson_dminfo)
            )


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

    def __len__(self) -> int:
        with nogil:
            config: cython.pointer(CConfiguration) = &CServiceLocator.configuration()

        return config.Size()
