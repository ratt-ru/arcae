import os

import numpy as np
# import pyrap.tables as pt

import pytest

from casa_arrow import pytable


@pytest.mark.xfail(reason="https://github.com/apache/arrow/issues/32291 and https://github.com/apache/arrow/pull/10565#issuecomment-885786527")
def test_complex_type_access_fail():
    T = pytable.SafeTableProxy("/home/simon/data/WSRT_polar.MS_p0")
    arrow_table = T.read_table(0, T.nrow())
    data = arrow_table.column("DATA")
    weight = arrow_table.column("WEIGHT")
    flag_cat = arrow_table.column("FLAG_CATEGORY")

@pytest.mark.parametrize("table_path, table_name", [
    ("/home/simon/data/WSRT_polar.MS_p0", "MAIN"),
    ("/home/simon/data/WSRT_polar.MS_p0::ANTENNA", "ANTENNA"),
    ("/home/simon/data/WSRT_polar.MS_p0::FEED", "FEED"),
    ("/home/simon/data/WSRT_polar.MS_p0::POLARIZATION", "POLARIZATION"),
    ("/home/simon/data/WSRT_polar.MS_p0::SPECTRAL_WINDOW", "SPECTRAL_WINDOW"),

    ("/home/simon/data/HLTau_B6cont.calavg.tav300s", "MAIN"),
    ("/home/simon/data/HLTau_B6cont.calavg.tav300s::ANTENNA", "ANTENNA"),
    ("/home/simon/data/HLTau_B6cont.calavg.tav300s::FEED", "FEED"),
    ("/home/simon/data/HLTau_B6cont.calavg.tav300s::POLARIZATION", "POLARIZATION"),
    ("/home/simon/data/HLTau_B6cont.calavg.tav300s::SPECTRAL_WINDOW", "SPECTRAL_WINDOW"),

], ids=lambda id: "X" if id.startswith("/") else id)
def test_parquet_write(tmp_path, table_path, table_name):
    T = pytable.SafeTableProxy("/home/simon/data/WSRT_polar.MS_p0")
    arrow_table = T.read_table(0, T.nrow())
    import pyarrow.parquet as pq
    print(arrow_table)
    pq.write_table(arrow_table, str(tmp_path / f"{table_name}.parquet"))

@pytest.fixture
def variable_table(tmp_path):
    try:
        import pyrap.tables as pt
    except ImportError as e:
        raise RuntimeError("Can't install pyrap.tables as "
            "the bundled shared objects are compiled with "
            "-D_GLIBCXX_USE_CXX11_ABI=0, while the kernsuite "
            "shared objects are compiled with "
            "-D_GLIBCXX_USE_CXX11_ABI=1") from e


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
    T = pytable.SafeTableProxy("/home/simon/code/casa-arrow/casa_arrow/tests/table3.table")
    arrow_table = T.read_table(0, T.nrow())
    foo = arrow_table.column("FOO")
    assert len(foo) == 10
    print(foo)
