import pytest
from numpy.testing import assert_array_equal
from pyarrow.lib import ArrowNotImplementedError

import arcae


def test_safe_multithreaded_writes():
    """Assert that this version of arcae does not
    support multithreaded writes"""
    assert not arcae.safe_multithreaded_writes()


def test_writes_succeeds_ninstances_1(column_case_table):
    """Test that writing when ninstances==1 succeeds"""
    with arcae.table(column_case_table, ninstances=1, readonly=False) as T:
        data = T.getcol("FIXED")
        T.putcol("FIXED", data + 1)
        assert_array_equal(T.getcol("FIXED"), data + 1)


def test_writes_fail_ninstances_2(column_case_table):
    """Test that attempting to write when ninstances > 1 fails"""
    with arcae.table(column_case_table, ninstances=2, readonly=False) as T:
        data = T.getcol("FIXED")
        with pytest.raises(
            ArrowNotImplementedError,
            match="Write support when number of table instances 2",
        ):
            T.putcol("FIXED", data + 1)
