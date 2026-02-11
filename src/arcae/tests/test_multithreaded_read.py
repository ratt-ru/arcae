import concurrent.futures as cf
import os
import sys

import numpy as np
import pytest
from numpy.testing import assert_array_equal

import arcae

try:
    import pytest_benchmark
except ImportError:
    pytest_benchmark = None

# This test suite benchmarks reads for the following case:
#
# 1. Multiple read requests issued from multiple threads through a single table object
#
# Notes:
# * COMPARE should be set to False when testing pure I/O, but is useful for
#   ensuring that no data corruption occurs.
# * Larger dimension sizes are more representative of real world scenarios
# * Remember to drop caches between tests
COMPARE = False
THREADS = 16
INSTANCES = THREADS
STEP = 10
MS_PARAMS = {"row": 10000 * STEP, "chan": 1024, "corr": 4}
DROP_CACHES = True


def maybe_drop_caches():
    if not DROP_CACHES:
        print("Disk caches were not dropped. This benchmark may not be accurate.")
        return

    if sys.platform != "linux":
        raise NotImplementedError("Dropping caches on {sys.platform}")

    if os.geteuid() != 0:
        raise RuntimeError("Need to run as root to drop caches")

    os.sync()
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write("3\n")


@pytest.mark.skipif(not pytest_benchmark, reason="pytest-benchmark not installed")
@pytest.mark.parametrize(
    "ramp_ms",
    [MS_PARAMS],
    indirect=True,
    ids=lambda c: f"ramp_ms: {','.join(f'{k}={v}' for k, v in c.items())}",
)
def test_singlefile_multithread_read(ramp_ms, benchmark):
    def read_single(T, startrow, nrow):
        data = T.getcol("COMPLEX_DATA", index=(slice(startrow, startrow + nrow),))
        if COMPARE:
            expected = np.arange(startrow, startrow + nrow)[:, None, None]
            assert_array_equal(data, np.broadcast_to(expected, data.shape))

    def test_(pool):
        futures = []

        with arcae.table(ramp_ms, ninstances=INSTANCES, lockoptions="nolock") as T:
            nrow = T.nrow()

            for startrow in range(0, nrow, STEP):
                lnrow = min(STEP, nrow - startrow)
                future = pool.submit(read_single, T, startrow, lnrow)
                futures.append(future)

            for future in cf.as_completed(futures):
                future.result()

    with cf.ThreadPoolExecutor(THREADS) as pool:
        print("Benchmarking starts")
        benchmark.pedantic(test_, args=(pool,), setup=maybe_drop_caches, rounds=10)
