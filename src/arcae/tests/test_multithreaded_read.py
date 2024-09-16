import concurrent.futures as cf
import threading

import numpy as np
import pytest
from numpy.testing import assert_array_equal

import arcae

try:
    import pytest_benchmark
except ImportError:
    pytest_benchmark = None

# This test suite benchmarks reads in two cases:
#
# 1. Multiple read requests issued from multiple threads through a single table object
# 2. Multiple read requests issued from multiple threads through multiple table objects
#    that have all opened the same file
#
# Notes:
# * COMPARE should be set to False when testing pure I/O, but is useful for
#   ensuring that no data corruption occurs.
# * Larger dimension sizes are more representative of real world scenarios
# * Remember to drop caches between tests
COMPARE = False
THREADS = 8
STEP = 10
MS_PARAMS = {"row": 100 * STEP, "chan": 1024, "corr": 4}


@pytest.mark.skipif(not pytest_benchmark, reason="pytest-benchmark not installed")
@pytest.mark.parametrize(
    "ramp_ms",
    [MS_PARAMS],
    indirect=True,
    ids=lambda c: f"ramp_ms: {','.join(f'{k}={v}' for k, v in c.items())}",
)
def test_singlefile_multithread_read(ramp_ms, benchmark):
    def read_single(T, startrow, nrow):
        data = T.getcol("COMPLEX_DATA", startrow=startrow, nrow=nrow)
        if COMPARE:
            expected = np.arange(startrow, startrow + nrow)[:, None, None]
            assert_array_equal(data, np.broadcast_to(expected, data.shape))

    def test_():
        futures = []

        with arcae.table(ramp_ms) as T:
            nrow = T.nrow()

            for startrow in range(0, nrow, STEP):
                lnrow = min(STEP, nrow - startrow)
                future = pool.submit(read_single, T, startrow, lnrow)
                futures.append(future)

            for future in cf.as_completed(futures):
                future.result()

    with cf.ThreadPoolExecutor(THREADS) as pool:
        benchmark(test_)


@pytest.mark.skipif(not pytest_benchmark, reason="pytest-benchmark not installed")
@pytest.mark.parametrize(
    "ramp_ms",
    [MS_PARAMS],
    indirect=True,
    ids=lambda c: f"ramp_ms: {','.join(f'{k}={v}' for k, v in c.items())}",
)
def test_multifile_multithreaded_read(ramp_ms, benchmark):
    local = threading.local()

    def read_multiple(ms, startrow, nrow):
        try:
            table_cache = local.table_cache
        except AttributeError:
            table_cache = local.table_cache = {}

        try:
            T = table_cache[ms]
        except KeyError:
            print(f"Opening {ms} in thread {threading.get_ident()}")
            T = table_cache[ms] = arcae.table(ms)

        # with arcae.table(ms) as T:
        data = T.getcol("COMPLEX_DATA", startrow=startrow, nrow=nrow)
        if COMPARE:
            expected = np.arange(startrow, startrow + nrow)[:, None, None]
            assert_array_equal(data, np.broadcast_to(expected, data.shape))

    def test_(nrow):
        futures = []

        for startrow in range(0, nrow, STEP):
            lnrow = min(STEP, nrow - startrow)
            future = pool.submit(read_multiple, ramp_ms, startrow, lnrow)
            futures.append(future)

        for future in cf.as_completed(futures):
            future.result()

    with cf.ThreadPoolExecutor(THREADS) as pool:
        with arcae.table(ramp_ms) as T:
            nrow = T.nrow()

        benchmark(test_, nrow)
