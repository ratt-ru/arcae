C++ and Python Arrow Bindings for casacore
==========================================


Rationale
---------

* The structure of Apache Arrow Tables is highly similar to that of CASA Tables
* It's easy to convert Arrow Tables between many different languages
* Converting CASA Tables to Arrow in the C++ layer avoids the GIL
* Once in Apache Arrow format, it is easy to store data in modern, cloud-native disk formats such as parquet and orc.
* Access to non thread-safe CASA Tables is constrained to a ThreadPool containing a single thread
* It also allows us to write astrometric routines in C++, potentially side-stepping thread-safety and GIL issues with the CASA Measures server.

Building
--------

This guide is targeted at Ubuntu 20.04, mostly because it provides an easy
install of casacore via kernsuite.
This guide can be adapted to other OS's if you're willing to build casacore yourself.
This software should be built with the new C++11 ABI.

* Install casacore C++ libraries and headers via `kernsuite <https://kernsuite.info/installation/>`_.

  .. code-block:: bash

    $ sudo apt install casacore-dev

  Note this installs a version of casacore built with the new C++11 ABI: `-D_GLIBCXX_USE_CXX11_ABI=1``

* Create a Python 3.8 virtual environment and built the Cython extension

  .. code-block:: bash

    $ virtualenv -p python3.8 ~/venv/carrow
    $ source ~/venv/carrow/bin/activate
    (carrow) $ pip install -U pip setuptools wheel
    (carrow) $ pip install -r requirements.txt
    (carrow) $ python setup.py build_ext --inplace

* Run the test cases

  .. code-block::

    (carrow) $ py.test -s -vvv


Limitations
-----------

Some edge cases have not yet been implemented, but could be with some thought.

* Not yet able to handle columns with unconstrained rank (ndim == -1).
* Not yet able to handle TpRecord columns. Probably easiest to convert these rows to json and store as a string.
* Not yet able to handle TpQuantity columns. Possible to represent as a run-time parametric Arrow DataType.
