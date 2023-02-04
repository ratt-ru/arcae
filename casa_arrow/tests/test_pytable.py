import os

import numpy as np
# import pyrap.tables as pt

import pytest

from casa_arrow import pytable

@pytest.fixture(scope="module")
def arrow_table():
    return pytable.open_table("/home/simon/data/WSRT_polar.MS_p0")

@pytest.mark.xfail(reason="https://github.com/apache/arrow/issues/32291 and https://github.com/apache/arrow/pull/10565#issuecomment-885786527")
def test_complex_type_access_fail(arrow_table):
    data = arrow_table.column("DATA")
    weight = arrow_table.column("WEIGHT")
    flag_cat = arrow_table.column("FLAG_CATEGORY")
    print(data[0])
    print(weight)
    print(flag_cat)

def test_pytable(arrow_table):
    uvw = arrow_table.column("UVW")
    print(uvw[16])

@pytest.fixture
def variable_table(tmp_path):
    column = "FOO"
    nrow = 10

    # Column descriptor
    desc = {
        "desc": {
            "_c_order": True,
            "comment": f"{column} column",
            "dataManagerGroup": "",
            "dataManagerType": "",
            "keywords": {},
            "ndim": 2,
            "maxlen": 0,
            "option": 0,
            "valueType": "int",
        },
        "name": column,
    }

    table_desc = pt.maketabdesc([desc])
    table_name = os.path.join(str(tmp_path), "test.table")

    with pt.table(table_name, table_desc, nrow=nrow, ack=False) as T:
        for i in range(nrow):
            T.putcell(column, i, np.full((10, 5 + i), i))

    return table_name

def test_variable_column():
    arrow_table = pytable.open_table("/home/simon/code/casa-arrow/casa_arrow/tests/test.table")
    foo = arrow_table.column("FOO")
    assert len(foo) == 10
