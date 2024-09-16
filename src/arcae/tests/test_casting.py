import numpy as np
import pytest
from numpy.testing import assert_array_almost_equal, assert_array_equal

from arcae.lib.arrow_tables import Table


def test_casting(getcol_table):
    with Table.from_filename(getcol_table, readonly=False) as T:
        complex_data = T.getcol("COMPLEX_DATA")
        assert complex_data.dtype == np.complex128

        expected = (complex_data + 1 + 1j).astype(np.complex64)
        T.putcol("COMPLEX_DATA", expected)

        result = T.getcol("COMPLEX_DATA")
        assert result.dtype == np.complex128
        assert_array_almost_equal(result, expected)
        T.putcol("COMPLEX_DATA", result)

        uvw = T.getcol("TIME")
        assert uvw.dtype == np.float64
        expected = uvw + 1
        T.putcol("TIME", expected)
        result = T.getcol("TIME")
        assert result.dtype == np.float64
        assert_array_equal(expected, result)

        flag = T.getcol("FLAG")
        assert flag.dtype == np.uint8
        float_flag = np.where(flag == 0, 0.0, 1.0)
        assert float_flag.dtype == np.float64
        T.putcol("FLAG", float_flag)
        result = T.getcol("FLAG")
        assert result.dtype == np.uint8
        assert_array_equal(result, flag)


@pytest.mark.xfail(reason="Some 32.0 values get written as 0")
def test_casting_flag_fail(getcol_table):
    with Table.from_filename(getcol_table, readonly=False) as T:
        flag = T.getcol("FLAG")
        assert flag.dtype == np.uint8
        float_flag = np.where(flag == 0, 0.0, 32.0)
        assert float_flag.dtype == np.float64
        T.putcol("FLAG", float_flag)
        result = T.getcol("FLAG")
        assert result.dtype == np.uint8
        assert_array_equal(result, flag)
