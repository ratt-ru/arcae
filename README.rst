C++ and Python Arrow Bindings for casacore
==========================================


Rationale
---------

* The structure of Apache Arrow Tables is highly similar to that of CASA Tables
* It's easy to convert Arrow Tables between many different languages
* Once in Apache Arrow format, it is easy to store data in modern, cloud-native disk formats such as parquet and orc.
* Converting CASA Tables to Arrow in the C++ layer avoids the GIL
* Access to non thread-safe CASA Tables is constrained to a ThreadPool containing a single thread
* It also allows us to write astrometric routines in C++, potentially side-stepping thread-safety
  and GIL issues with the CASA Measures server.


Building
--------

This guide is targeted at Ubuntu 20.04, mostly because it provides an easy
install of casacore via kernsuite.
This guide can be adapted to other OS's if you're willing to build casacore yourself.
This software should be built with the new C++11 ABI.

* Install casacore C++ libraries and headers via `kernsuite <https://kernsuite.info/installation/>`_.

  .. code-block:: bash

    $ sudo apt install casacore-dev

  Note this installs a version of casacore built with the new C++11 ABI: `-D_GLIBCXX_USE_CXX11_ABI=1`

* Create a Python 3.8 virtual environment and built the Cython extension.

  .. code-block:: bash

    $ virtualenv -p python3.8 ~/venv/carrow
    $ source ~/venv/carrow/bin/activate
    (carrow) $ pip install -U pip setuptools wheel
    (carrow) $ pip install -r requirements.txt
    (carrow) $ python setup.py build_ext --inplace

* Note that `requirements.txt` contains a custom Python 3.8 pyarrow manylinux_2_28 wheel
  built with `-D_GLIBCXX_USE_CXX11_ABI=1`
* Run the test cases

  .. code-block::

    (carrow) $ py.test -s -vvv



Usage
-----

.. code-block:: python

  import casa_arrow as ca
  import pyarrow as pa

  casa_table = ca.table("/path/to/measurementset.ms")
  arrow_table = casa_table.to_arrow()        # read entire column
  arrow_table = casa_table.to_arrow(10, 20)  # startrow, nrow
  assert isinstance(arrow_table, pa.Table)
  time = arrow_table.column("TIME").to_numpy()
  data = arrow_table.column("DATA").to_numpy()   # arrays of object arrays

Limitations
-----------

Some edge cases have not yet been implemented, but could be with some thought.

* Not yet able to handle columns with unconstrained rank (ndim == -1). Probably simplest to convert these rows to json and store as a string.
* Not yet able to handle TpRecord columns. Probably simplest to convert these rows to json and store as a string.
* Not yet able to handle TpQuantity columns. Possible to represent as a run-time parametric Arrow DataType.
* `to_numpy()` conversion of nested lists produces nested numpy arrays, instead of tensors.
  This is `possible <daskms_ext_types_>`_ but requires some changes to how
  `C++ Extension Types are exposed in Python <arrow_python_expose_cpp_ext_types_>`_.

.. _daskms_ext_types: https://github.com/ratt-ru/dask-ms/blob/1ff73ce3a60ea6479e40fc8cf440fd8d077e3d26/daskms/experimental/arrow/extension_types.py#L120-L152
.. _arrow_python_expose_cpp_ext_types: https://github.com/apache/arrow/issues/33997
