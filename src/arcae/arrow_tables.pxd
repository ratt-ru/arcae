# distutils: language = c++
# cython: language_level = 3

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.memory import shared_ptr
from pyarrow.includes.common cimport *
from pyarrow.includes.libarrow cimport *

cdef extern from "<climits>" nogil:
    cdef unsigned int UINT_MAX

cdef extern from "<casacore/casa/aipsxtype.h>" namespace "casacore" nogil:
    ctypedef unsigned long long uInt64
    ctypedef uInt64 rownr_t

cdef extern from "<absl/types/span.h>" namespace "absl" nogil:
    cdef cppclass Span[T]:
        Span() except +
        Span(T * array, size_t length) except +
        Span(Span&) except +
        size()

cdef extern from "arcae/base_column_map.h" namespace "arcae" nogil:
    ctypedef int64_t RowId
    ctypedef Span[RowId] RowIds
    ctypedef vector[RowIds] ColumnSelection

cdef extern from "arcae/service_locator.h" namespace "arcae" nogil:
    cdef cppclass CServiceLocator" arcae::ServiceLocator":
        @staticmethod
        CConfiguration & configuration" ServiceLocator::configuration"()

cdef extern from "arcae/configuration.h" namespace "arcae" nogil:
    cdef cppclass CConfiguration" arcae::Configuration":
        void Set" Configuration::Set"(const string & key, string value)
        CResult[string] Get" Configuration::Get"(const string & key)
        CResult[bool] Delete" Configuration::Delete"(const string & key)
        vector[string] GetKeys" Configuration::GetKeys"()
        size_t Size" Configuration::Size"()

cdef extern from "arcae/descriptor.h" namespace "arcae" nogil:
    cdef CResult[string] CMSDescriptor" arcae::MSDescriptor"(const string & table, bool complete)

cdef extern from "arcae/safe_table_proxy.h" namespace "arcae" nogil:
    cdef cppclass CCasaTable" arcae::SafeTableProxy":
        @staticmethod
        CResult[bool] Close" SafeTableProxy::Close"()

        CResult[shared_ptr[CTable]] ToArrow " SafeTableProxy::ToArrow"( const ColumnSelection & selection, const vector[string] & columns)
        CResult[shared_ptr[CArray]] GetColumn " SafeTableProxy::GetColumn"(const string & column, const ColumnSelection & selection)
        CResult[bool] PutColumn " SafeTableProxy::PutColumn"(const string & column, const ColumnSelection & selection, const shared_ptr[CArray] & data)
        CResult[string] GetTableDescriptor " SafeTableProxy::GetTableDescriptor"()
        CResult[string] GetColumnDescriptor "SafeTableProxy::GetColumnDescriptor"(const string & column)
        CResult[string] GetLockOptions "SafeTableProxy::GetLockOptions"()
        CResult[bool] ReopenRW "SafeTableProxy::ReopenRW"()
        CResult[unsigned int] nRow " SafeTableProxy::nRow"()
        CResult[unsigned int] nColumns " SafeTableProxy::nColumns"()
        CResult[vector[string]] Columns " SafeTableProxy::Columns"()
        CResult[vector[shared_ptr[CCasaTable]]] Partition " SafeTableProxy::Partition"(const vector[string] & partition_columns, const vector[string] & sort_columns)
        CResult[bool] AddRows " SafeTableProxy::AddRows"(unsigned int nrows)


cdef extern from "arcae/table_factory.h" namespace "arcae" nogil:
    cdef CResult[shared_ptr[CCasaTable]] COpenTable" arcae::OpenTable"(
                                                    const string & filename,
                                                    bool readonly,
                                                    const string & json_lockoptions)
    cdef CResult[shared_ptr[CCasaTable]] CDefaultMS" arcae::DefaultMS"(
                                                    const string & name,
                                                    const string & subtable,
                                                    const string & json_table_desc,
                                                    const string & json_dminfo)
    cdef CResult[shared_ptr[CCasaTable]] CTaql" arcae::Taql"(
                                                    const string & taql,
                                                    const vector[shared_ptr[CCasaTable]] & tables)


cdef extern from "arcae/complex_type.h" namespace "arcae" nogil:
    cdef cppclass CComplexType" arcae::ComplexType"(CExtensionType):
        shared_ptr[CDataType] value_type" ComplexType::value_type"()
        string extension_name()

    cdef cppclass CComplexDoubleType" arcae::ComplexDoubleType"(CComplexType):
        CComplexDoubleType" ComplexDoubleType"()

    cdef cppclass CComplexFloatType" arcae::ComplexFloatType"(CComplexType):
        CComplexFloatType" ComplexFloatType"()

    cdef cppclass CComplexDoubleArray" arcae::ComplexDoubleArray":
        pass

    cdef cppclass CComplexFloatArray" arcae::ComplexFloatArray":
        pass

    cdef shared_ptr[CDataType] complex64()
    cdef shared_ptr[CDataType] complex128()
