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


cdef extern from "../cpp/service_locator.h" namespace "arcae" nogil:
    cdef cppclass CServiceLocator" arcae::ServiceLocator":
        @staticmethod
        CConfiguration & configuration" ServiceLocator::configuration"()

cdef extern from "../cpp/configuration.h" namespace "arcae" nogil:
    cdef cppclass CConfiguration" arcae::Configuration":
        void Set" Configuration::Set"(const string & key, string value)
        CResult[string] Get" Configuration::Get"(const string & key)
        CResult[bool] Delete" Configuration::Delete"(const string & key)
        vector[string] GetKeys" Configuration::GetKeys"()
        size_t Size" Configuration::Size"()

cdef extern from "../cpp/descriptor.h" namespace "arcae" nogil:
    cdef CResult[string] CMSDescriptor" arcae::MSDescriptor"(const string & table, bool complete)

cdef extern from "../cpp/safe_table_proxy.h" namespace "arcae" nogil:
    cdef cppclass CCasaTable" arcae::SafeTableProxy":
        @staticmethod
        CResult[bool] close" SafeTableProxy::close"()

        CResult[shared_ptr[CTable]] to_arrow " SafeTableProxy::to_arrow"(unsigned int startrow, unsigned int nrow, const vector[string] & columns)
        CResult[unsigned int] nrow " SafeTableProxy::nrow"()
        CResult[unsigned int] ncolumns " SafeTableProxy::ncolumns"()
        CResult[vector[string]] columns " SafeTableProxy::columns"()
        CResult[vector[shared_ptr[CCasaTable]]] partition " SafeTableProxy::partition"(const vector[string] & partition_columns, const vector[string] & sort_columns)

cdef extern from "../cpp/table_factory.h" namespace "arcae" nogil:
    cdef CResult[shared_ptr[CCasaTable]] copen_table" arcae::open_table"(
                                                    const string & filename)
    cdef CResult[shared_ptr[CCasaTable]] cdefault_ms" arcae::default_ms"(
                                                    const string & name,
                                                    const string & subtable,
                                                    const string & json_table_desc,
                                                    const string & json_dminfo)


cdef extern from "../cpp/complex_type.h" namespace "arcae" nogil:
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
