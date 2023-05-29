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


cdef extern from "../src/service_locator.h" nogil:
    cdef cppclass CServiceLocator" ServiceLocator":
        @staticmethod
        CConfiguration & configuration" ServiceLocator::configuration"()

cdef extern from "../src/configuration.h" nogil:
    cdef cppclass CConfiguration" Configuration":
        void Set" Configuration::Set"(const string & key, string value)
        CResult[string] Get" Configuration::Get"(const string & key)
        CResult[bool] Delete" Configuration::Delete"(const string & key)
        vector[string] GetKeys" Configuration::GetKeys"()
        size_t Size" Configuration::Size"()


cdef extern from "../src/safe_table_proxy.h" nogil:
    cdef cppclass CCasaTable" SafeTableProxy":
        @staticmethod
        CResult[shared_ptr[CCasaTable]] Make" SafeTableProxy::Make"(const string & filename)
        CResult[bool] close" SafeTableProxy::close"()

        CResult[shared_ptr[CTable]] to_arrow " SafeTableProxy::to_arrow"(unsigned int startrow, unsigned int nrow, const vector[string] & columns)
        CResult[unsigned int] nrow " SafeTableProxy::nrow"()
        CResult[unsigned int] ncolumns " SafeTableProxy::ncolumns"()
        CResult[vector[string]] columns " SafeTableProxy::columns"()
        CResult[vector[shared_ptr[CCasaTable]]] partition " SafeTableProxy::partition"(const vector[string] & partition_columns, const vector[string] & sort_columns)

cdef extern from "../src/complex_type.h" nogil:
    cdef cppclass CComplexType" ComplexType"(CExtensionType):
        shared_ptr[CDataType] value_type" ComplexType::value_type"()
        string extension_name()

    cdef cppclass CComplexDoubleType" ComplexDoubleType"(CComplexType):
        CComplexDoubleType" ComplexDoubleType"()

    cdef cppclass CComplexFloatType" ComplexFloatType"(CComplexType):
        CComplexFloatType" ComplexFloatType"()

    cdef cppclass CComplexDoubleArray" ComplexDoubleArray":
        pass

    cdef cppclass CComplexFloatArray" ComplexFloatArray":
        pass

    shared_ptr[CDataType] complex64()
    shared_ptr[CDataType] complex128()
