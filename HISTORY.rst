=======
History
=======

0.2.2 (2023-11-10)
------------------
* Remove WITH_SOABI workaround (:pr:`75`)
* Add test case demonstrating the feasibility of parallel writes to
  Tiled Storage Manager Columns (:pr:`69`)
* Update README.rst to show that it is possible to convert
  unconstrained columns (ndim==-1) whose rows have the same rank (:pr:`74`)
* Guard against python-casacore imports (:pr:`73`)

0.2.1 (2023-10-24)
------------------
* Table Query Language Support (:pr:`71`)
* Set skip-existing=true when uploading distributables to TestPyPI (:pr:`68`)

0.2.0 (2023-10-19)
------------------
* Support multiple table objects reading from the same underlying table in multiple threads (:pr:`67`)
* Don't hold the GIL when calling GetResultValue (:pr:`66`)
* Add support for a C++ test suite (:pr:`64`)
* Use underscore for ColumnConvertVisitor member names (:pr:`62`)
* Migrate build system to scikit-build-core (:pr:`61`)
* Upgrade to Cython 3 and pyarrow 13.0.0 (:pr:`60`)
* Introduce a more canonical C++ project structure (:pr:`57`. :pr:`59`)
* Consistently use CamelCase throughout the C++ layer (:pr:`56`)
* Support getcol, tabledesc and getcoldesc (:pr:`55`, :pr:`58`)
* Enable initial OSX support in the build process (:pr:`54`)
* Add support for adding rows to a table (:pr:`53`)
* Create and use JSON Table Descriptors and Data Managers (:pr:`51`)
* Use ccache, if available (:pr:`50`)
* Use vcpkg's internal github actions binary caching (:pr:`49`)
* Generalise the opening and creation of Tables (:pr:`48`)
* Optimise storage and passing of TableProxy objects (:pr:`46`)
* Convert SAFE_TABLE_FUNCTOR from macro to template function (:pr:`45`)
* Fix `export CIBW_TEST_SKIP` (:pr:`42`)

0.1.0 (2023-06-30)
------------------
* First release
