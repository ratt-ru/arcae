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

cdef extern from "<absl/types/span.h>" namespace "absl" nogil:
    cdef cppclass Span[T]:
        Span() except +
        Span(T * array, size_t length) except +
        Span(Span&) except +
        size()

cdef extern from "arcae/selection.h" namespace "arcae::detail" nogil:
    ctypedef int64_t IndexType
    ctypedef vector[IndexType] Index
    ctypedef Span[const IndexType] IndexSpan
    cdef cppclass CSelection" arcae::detail::Selection":
        pass

    cdef cppclass CSelectionBuilder" arcae::detail::SelectionBuilder":
        CSelectionBuilder() except +
        CSelectionBuilder & Add[T](vector[T] ids)
        CSelectionBuilder & Add(const IndexSpan & ids)
        CSelectionBuilder & AddEmpty()
        CSelectionBuilder & Order(char order)
        CSelection Build()

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

cdef extern from "arcae/new_table_proxy.h" namespace "arcae" nogil:
    cdef cppclass CCasaTable" arcae::NewTableProxy":
        @staticmethod
        CResult[bool] Close" NewTableProxy::Close"() noexcept

        CResult[shared_ptr[CTable]] ToArrow " NewTableProxy::ToArrow"(
            const CSelection & selection,
            const vector[string] & columns) noexcept
        CResult[shared_ptr[CArray]] GetColumn " NewTableProxy::GetColumn"(
            const string & column,
            const CSelection & selection,
            shared_ptr[CArray] result) noexcept
        CResult[bool] PutColumn " NewTableProxy::PutColumn"(
            const string & column,
            const shared_ptr[CArray] & data,
            const CSelection & selection) noexcept
        CResult[string] Name " NewTableProxy::Name"() noexcept
        CResult[string] GetTableDescriptor " NewTableProxy::GetTableDescriptor"() noexcept
        CResult[string] GetColumnDescriptor "NewTableProxy::GetColumnDescriptor"(const string & column) noexcept
        CResult[string] GetLockOptions "NewTableProxy::GetLockOptions"() noexcept
        CResult[unsigned int] nRows " NewTableProxy::nRows"() noexcept
        CResult[unsigned int] nColumns " NewTableProxy::nColumns"() noexcept
        CResult[vector[string]] Columns " NewTableProxy::Columns"() noexcept
        CResult[bool] AddRows " NewTableProxy::AddRows"(unsigned int nrows) noexcept


cdef extern from "arcae/table_factory.h" namespace "arcae" nogil:
    cdef CResult[shared_ptr[CCasaTable]] COpenTable" arcae::OpenTable"(
                                                    const string & filename,
                                                    bool readonly,
                                                    const string & json_lockoptions) noexcept
    cdef CResult[shared_ptr[CCasaTable]] CDefaultMS" arcae::DefaultMS"(
                                                    const string & name,
                                                    const string & subtable,
                                                    const string & json_table_desc,
                                                    const string & json_dminfo) noexcept
    cdef CResult[shared_ptr[CCasaTable]] CTaql" arcae::Taql"(
                                                    const string & taql,
                                                    const vector[shared_ptr[CCasaTable]] & tables) noexcept
