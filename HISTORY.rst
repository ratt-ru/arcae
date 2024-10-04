=======
History
=======

X.Y.Z (YYYY-MM-DD)
------------------
* Support C++ merging of sorted NumPy partitions (:pr:`127`)
* Deprecate Python 3.9 support (:pr:`125`)
* Upgrade to casacore 3.6.1 (:pr:`124`)
* Build against NumPy 2 (:pr:`122`)
* Add pre-commit hooks, install formatters and linters (:pr:`121`)
* Support adding columns (:pr:`120`)

0.2.4 (2024-09-05)
------------------
* Disable OpenBLAS threading (:pr:`117`)
* Move Cython source to lib directory (:pr:`116`)
* Upgrade to pyarrow 16.1.0 (:pr:`115`)
* Code formatting nits (:pr:`114`)
* Multiplex CASA Table I/O over multiple table instances (:pr:`113`)
* Handle arrays and tables with no rows (:pr:`112`)
* Drop the Global Interpreter Lock (:pr:`111`)
* Remove FFTW3 and casacore apps from the casacore build (:pr:`110``)
* Add table name method (:pr:`109`)
* Re-implement partial support for selection over unconstrained columns
  that, in practice, have the same rank (:pr:`108`)
* Unpin manylinux_2_28_x86_64 image (:pr:`107`)
* Pin cython to less than 3.0.10 (:pr:`106`)
* Use casacore::RefRows for indexing the row dimension (:pr:`105`)
* Refactor arcae to use a finer-grained execution model (:pr:`101`)
* Pin manylinux_2_28 image to manylinux_2_28_x86_64:2024.07.02-0 (:pr:`102`)
* Restrict Numpy to less than 2.0.0 (:pr:`100`)
* Avoid stripping debug information (:pr:`96`)
* Set cmake build type to RelWithDebInfo (:pr:`96`)
* Avoid creating ColumnDesc objects in inner loops (:pr:`95`)
* Support Table arguments in TAQL queries (:pr:`93`)
* Upgrade to pyarrow 16.0.0 (:pr:`92`)
* Handle slice(None) in getcol index (:pr:`91`)

0.2.3 (2024-18-04)
------------------
* Remove unused utility code (:pr:`90`)
* Upgrade to pyarrow 15.0.2 (:pr:`89`)
* Fix python 3.9 typing (:pr:`87`, :pr:`88`)
* Support table lock options (:pr:`86`)
* Support complex indexing (:pr:`65`)
* Configure dependabot (:pr:`85`)
* Enable dependabot version updates (:pr:`84`)
* Upgrade to cibuildwheel 2.17 (:pr:`83`)
* Upgrade to pyarrow 15.0.0 (:pr:`76`)
* Build linux arm64 and macos arm64/x86_64 wheels (:pr:`76`)
* Upgrade vcpkg version to include wcslib 8.2.1 (:pr:`82`)
* Export compile_commands.json by default (:pr:`81`)
* Make export application dependencies optional (:pr:`80`)
* Fix ENV access within cmake files (:pr:`79`)

0.2.2 (2023-11-10)
------------------
* Upgrade to pyarrow 14.0.1 (:pr:`77`)
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
