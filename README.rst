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


Build Wheel Locally
-------------------

In the user or, even better, a virtual environment:

.. code-block:: python

  $ pip install -U pip cibuildwheel
  $ bash scripts/run_cbuildwheel.sh -p 3.10

.. warning::
  Only linux wheels are currently supported.

Local Development
-----------------

In the directory containing the source, setup your development environment as follows:

.. code-block:: python

  $ pip install -U pip virtualenv
  $ virtualenv -p python3.10 /venv/arcaedev
  $ . /venv/arcaedev/bin/activate
  (arcaedev) export VCPKG_TARGET_TRIPLET=x64-linux-dynamic-cxx17-abi1-dbg
  (arcaedev) pip install -e .[test]
  (arcaedev) export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$(pwd)/vcpkg/installed/$VCPKG_TARGET_TRIPLET/lib
  (arcaedev) py.test -s -vvv --pyargs arcae

Usage
-----

Example Usage:

  .. code-block:: python

    import json
    from pprint import pprint

    import arcae
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Obtain (partial) Apache Arrow Table from a CASA Table
    casa_table = arcae.table("/path/to/measurementset.ms")
    arrow_table = casa_table.to_arrow()        # read entire table
    arrow_table = casa_table.to_arrow(10, 20)  # startrow, nrow
    assert isinstance(arrow_table, pa.Table)

    # Print JSON-encoded Table and Column keywords
    pprint(json.loads(arrow_table.schema.metadata[b"__arcae_metadata__"]))
    pprint(json.loads(arrow_table.schema.field("DATA").metadata[b"__arcae_metadata__"]))

    # Extract Arrow Table columns into numpy arrays
    time = arrow_table.column("TIME").to_numpy()
    data = arrow_table.column("DATA").to_numpy()   # currently, arrays of object arrays, overly slow and memory hungry
    df = arrow_table.to_pandas()                   # currently slow, memory hungry due to arrays of object arrays

    # Write Arrow Table to parquet file
    pq.write_table(arrow_table, "measurementset.parquet")


See the test cases for further use cases.


Exporting Measurement Sets to Arrow Parquet Datasets
----------------------------------------------------

An export script is available:

.. code-block:: bash

  $ arcae export /path/to/the.ms --nrow 50000
  $ tree output.arrow/
  output.arrow/
  ├── ANTENNA
  │   └── data0.parquet
  ├── DATA_DESCRIPTION
  │   └── data0.parquet
  ├── FEED
  │   └── data0.parquet
  ├── FIELD
  │   └── data0.parquet
  ├── MAIN
  │   └── FIELD_ID=0
  │       └── PROCESSOR_ID=0
  │           ├── DATA_DESC_ID=0
  │           │   ├── data0.parquet
  │           │   ├── data1.parquet
  │           │   ├── data2.parquet
  │           │   └── data3.parquet
  │           ├── DATA_DESC_ID=1
  │           │   ├── data0.parquet
  │           │   ├── data1.parquet
  │           │   ├── data2.parquet
  │           │   └── data3.parquet
  │           ├── DATA_DESC_ID=2
  │           │   ├── data0.parquet
  │           │   ├── data1.parquet
  │           │   ├── data2.parquet
  │           │   └── data3.parquet
  │           └── DATA_DESC_ID=3
  │               ├── data0.parquet
  │               ├── data1.parquet
  │               ├── data2.parquet
  │               └── data3.parquet
  ├── OBSERVATION
  │   └── data0.parquet


This data can be loaded into an Arrow Dataset:

.. code-block:: python

    >>> import pyarrow as pa
    >>> import pyarrow.dataset as pad
    >>> main_ds = pad.dataset("output.arrow/MAIN")
    >>> spw_ds = pad.dataset("output.arrow/SPECTRAL_WINDOW")

Limitations
-----------

Some edge cases have not yet been implemented, but could be with some thought.

* Columns with unconstrained rank (ndim == -1) whose rows, in practice, have differing dimensions.
  Unconstrained rank columns whose rows actually have the same rank are catered for.
* Not yet able to handle TpRecord columns. Probably simplest to convert these rows to json and store as a string.
* Not yet able to handle TpQuantity columns. Possible to represent as a run-time parametric Arrow DataType.
* `to_numpy()` conversion of nested lists produces nested numpy arrays, instead of tensors.
  This is `possible <daskms_ext_types_>`_ but requires some changes to how
  `C++ Extension Types are exposed in Python <arrow_python_expose_cpp_ext_types_>`_.



Etymology
---------

Noun: **arca** f (genitive **arcae**); first declension
A chest, box, coffer, safe (safe place for storing items, or anything of a similar shape)

Pronounced: `ar-ki <arcae_pronounce_>`_.


.. _daskms_ext_types: https://github.com/ratt-ru/dask-ms/blob/1ff73ce3a60ea6479e40fc8cf440fd8d077e3d26/daskms/experimental/arrow/extension_types.py#L120-L152
.. _arrow_python_expose_cpp_ext_types: https://github.com/apache/arrow/issues/33997
.. _arcae_pronounce: https://translate.google.com/?sl=la&tl=en&text=arcae%0A&op=translate
