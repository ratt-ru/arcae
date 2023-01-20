import pytest

from casa_arrow import pytable

@pytest.fixture(scope="module")
def arrow_table():
    return pytable.open_table("/home/simon/data/WSRT_polar.MS_p0/")

@pytest.mark.xfail(reason="https://github.com/apache/arrow/issues/32291 and https://github.com/apache/arrow/pull/10565#issuecomment-885786527")
def test_complex_type_access_fail(arrow_table):
    data = arrow_table.column("DATA")
    print(data[0])

def test_pytable(arrow_table):
    uvw = arrow_table.column("UVW")
    print(uvw[16])

