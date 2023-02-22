import multiprocessing as mp
import os

import numpy as np
import pytest


def generate_column_cases_table(path):
    import pyrap.tables as pt
    nrow = 3

    # Table descriptor
    table_desc = [
        {
            "desc": {
                "_c_order": True,
                "comment": "VARIABLE column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "ndim": 3,
                "maxlen": 0,
                "option": 0,
                "valueType": "int",
            },
            "name": "VARIABLE",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "VARIABLE_STRING column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "ndim": 3,
                "maxlen": 0,
                "option": 0,
                "valueType": "string",
            },
            "name": "VARIABLE_STRING",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "FIXED column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "ndim": 2,
                "shape": [2, 4],
                "maxlen": 0,
                "option": 0,
                "valueType": "int",
            },
            "name": "FIXED",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "FIXED_STRING column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "ndim": 2,
                "shape": [2, 4],
                "maxlen": 0,
                "option": 0,
                "valueType": "string",
            },
            "name": "FIXED_STRING",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "SCALAR column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "maxlen": 0,
                "option": 0,
                "valueType": "int",
            },
            "name": "SCALAR",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "SCALAR_STRING column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "maxlen": 0,
                "option": 0,
                "valueType": "string",
            },
            "name": "SCALAR_STRING",
        },
        {
            "desc": {
                "_c_order": True,
                "comment": "UNCONSTRAINED column",
                "dataManagerGroup": "",
                "dataManagerType": "",
                "keywords": {},
                "maxlen": 0,
                "ndim": -1,
                "option": 0,
                "valueType": "int",
            },
            "name": "UNCONSTRAINED",
        }

    ]

    table_desc = pt.maketabdesc(table_desc)
    table_name = os.path.join(path, "test.table")

    with pt.table(table_name, table_desc, nrow=nrow, ack=False) as T:
        for i in range(nrow):
            T.putcell("VARIABLE", i, np.full((3, 1 + i, 2), i))
            T.putcell("FIXED", i, np.full((2, 4), i))
            T.putcell("SCALAR", i, i)

            T.putcell("VARIABLE_STRING", i, np.full((3, 1 + i, 2), str(i)))
            T.putcell("FIXED_STRING", i, np.full((2, 4), str(i)))
            T.putcell("SCALAR_STRING", i, str(i))

        T.putcell("UNCONSTRAINED", 0, np.full((2, 3, 4), 0))
        T.putcell("UNCONSTRAINED", 1, np.full((4, 3), 1))
        T.putcell("UNCONSTRAINED", 1, 1)

    return table_name


@pytest.fixture
def column_case_table(tmp_path_factory):
    # Generate casa table in a spawned process, otherwise
    # the pyrap.tables casacore libraries will be loaded in
    # and interfere with system casacore libraries
    mp.set_start_method('spawn')

    path = tmp_path_factory.mktemp("column_cases")

    with mp.Pool(1) as pool:
        result = pool.apply_async(generate_column_cases_table, (str(path),))
        return result.get()
