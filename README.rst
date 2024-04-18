C++ and Python Arrow Bindings for casacore
==========================================

``arcae`` implements a limited subset of functionality from the more mature python-casacore_ package. It bypasses some existing limitations in python-casacore to provide safe, multi-threaded access to CASA formats, thereby enabling export into newer cloud native formats such as Apache Arrow and Zarr.

Rationale
---------

``casacore`` and the ``python-casacore`` Python bindings provide access to the CASA Table Data System (CTDS) and Measurement Sets created within this system. The CTDS, as of casacore 3.5.0 is subject to the following limitations:

* Access from multiple threads is unsafe.

    - https://github.com/casacore/casacore/issues/1038
    - https://github.com/casacore/casacore/issues/1163

* ``python-casacore`` doesn't drop the Global Interpreter Lock

    - https://github.com/casacore/python-casacore/pull/209

Resolving these concerns is potentially a major effort, involving invasive changes across the CTDS system.

In the time since the CTDS was developed, newer, open-source formats such as Apache Arrow and Zarr have been developed that are suitable for representing Radio Astronomy data.

* The Apache Arrow project defines a programming language portable in-memory columnar storage format.
* Translating CTDS data to Arrow is relatively simple, with some limitations mentioned below.
* It's easy to convert Arrow Tables between many different languages
* Once in Apache Arrow format, it is easy to store data in modern, cloud-native disk formats such as parquet and Zarr.
* Converting CASA Tables to Arrow in the C++ layer avoids the GIL
* Access to non thread-safe CASA Tables is constrained to a ThreadPool containing a single thread
* It also allows us to write astrometry routines in C++, potentially side-stepping thread-safety and GIL issues with the CASA Measures server.

Limitations
-----------

Arrow supports both 1D arrays and nested structures:

1. Fixed shape multi-dimensional data (i.e. visibility data) is currently represented as nested `FixedSizeListArrays <fixed_size_list_layout_>`_ .
2. Variably-shaped multi-dimensional (i.e. subtable data) is currently represented as nested `ListArrays <variable_size_list_layout_>`_.
3. Complex values are represented as an extra `FixedSizeListArray <fixed_size_list_layout_>`_ nesting of two floats.
4. Currently, it is not trivially trivial (repetition intended here) to convert between the above and numpy via ``to_numpy`` calls on Arrow Arrays, but it is relatively trivial to reinterpret the underlying data buffers from either API. This is done transparently in ``getcol`` and ``putcol`` functions (see usage below).

Going forward, `FixedShapeTensorArray <fixed_shape_tensor_array_>`_ and `VariableShapeTensorArray <variable_shape_tensor_array_>`_ will provide more ergonomic structures for representing multi-dimensional data. First class support for complex values in Apache Arrow will require implementing a `C++ extension type <cpp_extension_type_>`_ within Arrow itself:

Some other edge cases have not yet been implemented, but could be with some thought.

* Columns with unconstrained rank (ndim == -1) whose rows, in practice, have differing dimensions.
  Unconstrained rank columns whose rows actually have the same rank are catered for.
* Not yet able to handle TpRecord columns. Probably simplest to convert these rows to json and store as a string.
* Not yet able to handle TpQuantity columns. Possible to represent as a run-time parametric Arrow DataType.


Installation
------------

Binary wheels are providing for Linux and MacOSX for both x86_64 and arm64 architectures

.. code-block:: bash

  $ pip install arcae

Usage
-----

Example usage with Arrow Tables:

  .. code-block:: python

    import json
    from pprint import pprint

    import arcae
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Obtain (partial) Apache Arrow Table from a CASA Table
    casa_table = arcae.table("/path/to/measurementset.ms")
    arrow_table = casa_table.to_arrow()        # read entire table
    arrow_table = casa_table.to_arrow(index=(slice(10, 20),)
    assert isinstance(arrow_table, pa.Table)

    # Print JSON-encoded Table and Column keywords
    pprint(json.loads(arrow_table.schema.metadata[b"__arcae_metadata__"]))
    pprint(json.loads(arrow_table.schema.field("DATA").metadata[b"__arcae_metadata__"]))

    pq.write_table(arrow_table, "measurementset.parquet")

Some reading and writing functionality from python-casacore_ is replicated,
with added support for some `NumPy Advanced Indexing <numpy_advanced_indexing_>`_.

  .. code-block:: python

    casa_table = arcae.table("/path/to/measurementset.ms", readonly=False)
    # Get rows 10 and 2, and channels 16 to 32, and all correlations
    data = casa_table.getcol("DATA", index=([10, 2], slice(16, 32), None)
    # Write some modified data back
    casa_table.putcol("DATA", data + 1*1j, index=([10, 2], slice(16, 32), None)

See the test cases for further use cases.


Exporting Measurement Sets to Arrow Parquet Datasets
----------------------------------------------------

Install the ``applications`` optional extra.

  .. code-block:: bash

    pip install arcae[applications]

Then, an export script is available:

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

Etymology
---------

Noun: **arca** f (genitive **arcae**); first declension
A chest, box, coffer, safe (safe place for storing items, or anything of a similar shape)

Pronounced: `ar-ki <arcae_pronounce_>`_.


.. _python-casacore: https://github.com/casacore/python-cascore
.. _fixed_size_list_layout: https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout
.. _variable_size_list_layout: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
.. _fixed_shape_tensor_array: https://arrow.apache.org/docs/python/generated/pyarrow.FixedShapeTensorArray.html
.. _variable_shape_tensor_array: https://github.com/apache/arrow/pull/38008
.. _numpy_advanced_indexing: https://numpy.org/doc/stable/user/basics.indexing.html#advanced-indexing
.. _cpp_extension_type: https://arrow.apache.org/docs/cpp/api/datatype.html#extension-types
.. _arcae_pronounce: https://translate.google.com/?sl=la&tl=en&text=arcae%0A&op=translate
