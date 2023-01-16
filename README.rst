C++ and Python Arrow Bindings for casacore
==========================================


Rationale
---------

It's useful to

* Apache Arrow Tables have very similar structure to CASA Tables
* It's easy to convert Arrow Tables between many different languages
* Converting CASA Tables to Arrow in the C++ layer avoids the GIL
* It also gives us access to CASA astronometric routines.

Building
--------

* Install casacore C++ libraries and headers, most easily done by installing KERN
* Install Apache Arrow libraries, following instructions for C++ `here <https://arrow.apache.org/install/>`_.
* Install cmake

.. code-block:: bash

    $ mkdir build
    $ cd build
    $ cmake ..
    $ cmake --build .


Then, the following should convert an MS table to an Arrow table and print it.

.. code-block:: bash

    $ src/tests/test_runner

Limitations
-----------

* Only converts ScalarColumns at present (ANTENNA1, TIME)
* FixedShape ArrayColumns (DATA, FLAG, UVW) should be easy enough
* Variably shaped columns are completely possibly to represent with Arrow ListTypes!

