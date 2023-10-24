import os


import numpy as np
from numpy.testing import assert_array_equal
import pyarrow as pa
import pyarrow.dataset as pad
import pyarrow.parquet as pq

import pytest

import arcae
from arcae.lib.arrow_tables import Table

@pytest.mark.parametrize("table_suffix, table_name", [
    ("", "MAIN"),
    ("::ANTENNA", "ANTENNA"),
    ("::FEED", "FEED"),
    ("::POLARIZATION", "POLARIZATION"),
    ("::SPECTRAL_WINDOW", "SPECTRAL_WINDOW"),
])
def test_parquet_write(tmp_path, tau_ms, table_suffix, table_name):
    """ Test conversion of a representative MS and some of its subtables to parquet format """
    T = arcae.table(f"{tau_ms}{table_suffix}").to_arrow()
    pq.write_table(T, str(tmp_path / f"{table_name}.parquet"))


def test_column_selection(column_case_table):
    with arcae.table(column_case_table) as T:
        assert sorted(T.to_arrow().column_names) == [
            "FIXED",
            "FIXED_STRING",
            "SCALAR",
            "SCALAR_STRING",
            # Even though the column is unconstrained, ndim is the same
            "UNCONSTRAINED_SAME_NDIM",
            "VARIABLE",
            "VARIABLE_STRING"
        ]

    with arcae.table(column_case_table) as T:
        assert sorted(T.to_arrow(0, 1).column_names) == [
            "FIXED",
            "FIXED_STRING",
            "SCALAR",
            "SCALAR_STRING",
            # When retrieving a single row, we can get values from an unconstrained column
            "UNCONSTRAINED",
            "UNCONSTRAINED_SAME_NDIM",
            "VARIABLE",
            "VARIABLE_STRING"
        ]

    with arcae.table(column_case_table) as T:
        assert sorted(T.to_arrow(0, 1, "VARIABLE").column_names) == ["VARIABLE"]

    with arcae.table(column_case_table) as T:
        assert sorted(T.to_arrow(0, 1, ["VARIABLE", "FIXED"]).column_names) == ["FIXED", "VARIABLE"]


def test_column_cases(column_case_table, capfd):
    """ Test code paths """
    T = arcae.table(column_case_table).to_arrow()

    assert T.column("VARIABLE").to_pylist() == [
        [[[0, 0]], [[0, 0]], [[0, 0]]],
        [[[1, 1], [1, 1]], [[1, 1], [1, 1]], [[1, 1], [1, 1]]],
        [[[2, 2], [2, 2], [2, 2]], [[2, 2], [2, 2], [2, 2]], [[2, 2], [2, 2], [2, 2]]]]

    assert T.column("FIXED").to_pylist() == [
        [[0, 0, 0, 0], [0, 0, 0, 0]],
        [[1, 1, 1, 1], [1, 1, 1, 1]],
        [[2, 2, 2, 2], [2, 2, 2, 2]]]

    assert T.column("SCALAR").to_pylist() == [0, 1, 2]

    assert T.column("VARIABLE_STRING").to_pylist() == [
        [[['0', '0']], [['0', '0']], [['0', '0']]],
        [[['1', '1'], ['1', '1']], [['1', '1'], ['1', '1']], [['1', '1'], ['1', '1']]],
        [[['2', '2'], ['2', '2'], ['2', '2']], [['2', '2'], ['2', '2'], ['2', '2']], [['2', '2'], ['2', '2'], ['2', '2']]]]

    assert T.column("FIXED_STRING").to_pylist() == [
        [['0', '0', '0', '0'], ['0', '0', '0', '0']],
        [['1', '1', '1', '1'], ['1', '1', '1', '1']],
        [['2', '2', '2', '2'], ['2', '2', '2', '2']]]

    assert T.column("SCALAR_STRING").to_pylist() == ['0', '1', '2']

    # Unconstrained columns not yet handled
    captured = capfd.readouterr()
    assert "UNCONSTRAINED" not in T.column_names
    assert "Ignoring UNCONSTRAINED" in captured.err


def test_complex_cases(complex_case_table):
    from arcae.arrow_tables import ComplexDoubleType
    from arcae import config

    table = arcae.table(complex_case_table)

    with config.set(**{"casa.convert.strategy": "fixed complex"}):
        T = table.to_arrow()
        assert T.column("COMPLEX").type == pa.list_(pa.list_(ComplexDoubleType(), 4), 2)

    with config.set(**{"casa.convert.strategy": "fixed"}):
        T = table.to_arrow()
        assert T.column("COMPLEX").type == pa.list_(pa.list_(pa.list_(pa.float64(), 2), 4), 2)

    with config.set(**{"casa.convert.strategy": "list complex"}):
        T = table.to_arrow()
        assert T.column("COMPLEX").type == pa.list_(pa.list_(ComplexDoubleType()))

    with config.set(**{"casa.convert.strategy": "list"}):
        T = table.to_arrow()
        assert T.column("COMPLEX").type == pa.list_(pa.list_(pa.list_(pa.float64())))


def test_getcol(getcol_table):
    T = arcae.table(getcol_table)

    assert_array_equal(T.getcol("TIME"), [0, 1, 2])
    assert_array_equal(T.getcol("STRING"), ["0", "1", "2"])

    assert_array_equal(T.getcol("FLOAT_DATA"), [
        [[0, 0, 0, 0], [0, 0, 0, 0]],
        [[1, 1, 1, 1], [1, 1, 1, 1]],
        [[2, 2, 2, 2], [2, 2, 2, 2]]])

    assert_array_equal(T.getcol("NESTED_STRING"), [
        [["0", "0", "0", "0"], ["0", "0", "0", "0"]],
        [["1", "1", "1", "1"], ["1", "1", "1", "1"]],
        [["2", "2", "2", "2"], ["2", "2", "2", "2"]]])

    assert_array_equal(T.getcol("COMPLEX_DATA"), [
        [[0 + 0j, 0 + 0j, 0 + 0j, 0 + 0j], [0 + 0j, 0 + 0j, 0 + 0j, 0 + 0j]],
        [[1 + 1j, 1 + 1j, 1 + 1j, 1 + 1j], [1 + 1j, 1 + 1j, 1 + 1j, 1 + 1j]],
        [[2 + 2j, 2 + 2j, 2 + 2j, 2 + 2j], [2 + 2j, 2 + 2j, 2 + 2j, 2 + 2j]]])


    for r in range(0, T.nrow(), 2):
        assert_array_equal(
            T.getcol("COMPLEX_DATA", startrow=r, nrow=1),
            [[[r + r*1j]*4]*2])

    with pytest.raises(TypeError, match="variably shaped column VARDATA"):
        T.getcol("VARDATA")

    with pytest.raises(pa.lib.ArrowException, match="NONEXISTENT does not exist"):
        T.getcol("NONEXISTENT")

def test_partial_read(sorting_table):
    """ Tests that partial reads work """
    T = arcae.table(sorting_table)
    full = T.to_arrow()
    nrows = [1, 2, 3, 4]
    assert sum(nrows) == len(full) == 10

    start = 0

    for nrow in nrows:
        assert full.take(list(range(start, start + nrow))) == T.to_arrow(start, nrow)
        start += nrow

def test_table_taql(sorting_table):
    """ Tests that basic taql queries work """
    with arcae.table(sorting_table) as T:
        AT = T.to_arrow()

    with Table.from_taql(f"SELECT * FROM {sorting_table} ORDER BY TIME, ANTENNA1, ANTENNA2") as Q:
        assert AT.sort_by([
            ("TIME", "ascending"),
            ("ANTENNA1", "ascending"),
            ("ANTENNA2", "ascending")]) == Q.to_arrow()

    with Table.from_taql(f"SELECT * FROM {sorting_table} ORDER BY ANTENNA1, ANTENNA2, TIME") as Q:
        assert AT.sort_by([
            ("ANTENNA1", "ascending"),
            ("ANTENNA2", "ascending"),
            ("TIME", "ascending")]) == Q.to_arrow()

def test_complex_taql(sorting_table):
    """ Test a relatively complex taql query """

    query = f"""
    SELECT
        ANTENNA1,
        ANTENNA2,
        GROWID() as ROW,
        GAGGR(TIME) as TIME,
        GAGGR(FIELD_ID) as FIELD_ID,
        GAGGR(SCAN_NUMBER) as SCAN_NUMBER
    FROM
        {sorting_table}
    GROUPBY
        ANTENNA1,
        ANTENNA2
    """

    with arcae.table(sorting_table) as T:
        AT = T.to_arrow()
        group_cols = ["ANTENNA1", "ANTENNA2"]
        agg_cols = [
            ("TIME", "list"),
            ("FIELD_ID", "list"),
            ("ROW", "list"),
            ("SCAN_NUMBER", "list")
        ]
        AT = AT.append_column("ROW", pa.array(np.arange(len(AT))))
        AT = AT.group_by(group_cols).aggregate(agg_cols)
        new_names = [c[:-len("_list")] if c.endswith("_list")
                     else c for c in AT.column_names]
        AT = AT.rename_columns(new_names)

    with Table.from_taql(query) as T:
        QT = T.to_arrow()
        # Ensure fields are ordered the same and have same types
        # TAQL returns indexing columns as int64 instead of original int32
        assert AT.select(QT.column_names).cast(QT.schema).equals(QT)


def test_table_partitioning(sorting_table):
    T = arcae.table(sorting_table)

    partitions = T.partition(["FIELD_ID", "DATA_DESC_ID"])
    assert len(partitions) == 3
    ddids = sum((p.to_arrow().column("DATA_DESC_ID").unique().tolist() for p in partitions), [])
    assert ddids == [0, 0, 1]

    fields = sum((p.to_arrow().column("FIELD_ID").unique().tolist() for p in partitions), [])
    assert fields == [0, 1, 2]

    # Partitions are not sorted in TIME
    for P in partitions:
        time = P.to_arrow().column("TIME")
        assert time.sort() != time

@pytest.mark.parametrize("sort_keys", [
    "TIME",
    ["ANTENNA1", "ANTENNA2", "TIME"],
    ["ANTENNA1", "ANTENNA2"],
    ["TIME", "ANTENNA1", "ANTENNA2"]
])
def test_table_partitioning_and_sorting(sorting_table, sort_keys):
    partitions = arcae.table(sorting_table).partition(["FIELD_ID", "DATA_DESC_ID"], sort_keys)

    if isinstance(sort_keys, str):
        sort_keys = [sort_keys]

    for P in partitions:
        arrow = P.to_arrow()
        D = {k: arrow.column(k).to_numpy() for k in sort_keys}
        idx = np.lexsort(tuple(D[k] for k in reversed(sort_keys)))

        for k in sort_keys:
            assert_array_equal(D[k], D[k][idx])


def test_print_dataset_structure(partitioned_dataset):
    """ Print the directory stucture of the partitioned dataset """
    partitioned_dataset = str(partitioned_dataset)

    print("\nDATASET TREE\n")

    for root, dirs, files in os.walk(partitioned_dataset):
        level = root.replace(partitioned_dataset, "").count(os.sep)
        indent = " " * 4 * (level)
        print("{}{}/".format(indent, os.path.basename(root)))
        subindent = " " * 4 * (level + 1)
        for f in sorted(files):
            print("{}{}".format(subindent, f))


def test_dataset_predicates(partitioned_dataset):
    """ Illustrate native arrow dataset predicates """
    partitioned_dataset = pad.dataset(partitioned_dataset)
    predicate = ((pad.field("ANTENNA1") >= 0) & (pad.field("ANTENNA1") < 2) &
                 (pad.field("ANTENNA2") >= 2) & (pad.field("ANTENNA2") <= 3))

    T = partitioned_dataset.to_table(filter=predicate)
    antenna1 = T.column("ANTENNA1").to_numpy()
    antenna2 = T.column("ANTENNA2").to_numpy()

    assert antenna1.size > 0 and np.all((antenna1 >= 0) & (antenna1 < 2))
    assert antenna2.size > 0 and np.all((antenna2 >= 2) & (antenna2 <= 3))

def test_duckdb(partitioned_dataset):
    """ Illustrate integation between dataset and duckdb"""
    partitioned_dataset = pad.dataset(partitioned_dataset)
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()

    query = con.execute(f"SELECT TIME, ANTENNA1, ANTENNA2, DATA FROM partitioned_dataset "
                        f"WHERE ANTENNA1 >= 0 AND ANTENNA1 < 2 AND "
                        f"ANTENNA2 >= 2 AND ANTENNA2 <=3")
    rbr = query.fetch_record_batch()

    chunks = []

    while True:
        try:
            chunks.append(rbr.read_next_batch())
        except StopIteration:
            break

    query_table = pa.Table.from_batches(chunks)
    antenna1 = query_table.column("ANTENNA1").to_numpy()
    antenna2 = query_table.column("ANTENNA2").to_numpy()

    assert antenna1.size > 0 and np.all((antenna1 >= 0) & (antenna1 < 2))
    assert antenna2.size > 0 and np.all((antenna2 >= 2) & (antenna2 <= 3))


def test_config():
    from arcae.lib.arrow_tables import Configuration
    global_config = Configuration()

    assert global_config["validation-level"] == "full"

    global_config["blah"] = "foo"
    assert global_config["blah"] == "foo"
    assert len(global_config) == 2

    global_config["qux"] = "bar"
    assert global_config["qux"] == "bar"
    assert len(global_config) == 3

    try:
        global_config["foo"] == "bar"
    except KeyError as e:
        assert "foo" in e.args[0]

    assert global_config.get("foo") is None
    assert global_config.get("foo", "bar") == "bar"

    assert list(global_config.items()) == [("blah", "foo"), ("qux", "bar"), ("validation-level", "full")]

    try:
        global_config["foo"] = 1
    except TypeError as e:
        assert "(expected str, got int)" in e.args[0]

    del global_config["blah"]
    del global_config["qux"]

    assert list(global_config.items()) == [("validation-level", "full")]


def test_config_context_mgr():
    from arcae.lib.arrow_tables import Configuration
    from arcae import config
    global_config = Configuration()
    assert list(global_config.items()) == [("validation-level", "full")]

    with config.set(**{"foo": "bar", "qux-baz": "blah"}):
        assert global_config["foo"] == "bar"
        assert global_config["qux-baz"] == "blah"

    with pytest.raises(KeyError):
        assert global_config["foo"] == "foo"

    with pytest.raises(KeyError):
        assert global_config["qux-baz"] == "blah"

    with config.set(**{"foo": "bar"}):
        assert global_config["foo"] == "bar"

        with config.set(**{"qux": "baz"}):
            assert global_config["foo"] == "bar"
            assert global_config["qux"] == "baz"

        with pytest.raises(KeyError):
            global_config["qux"]

    with pytest.raises(KeyError):
        global_config["foo"]

    assert list(global_config.items()) == [("validation-level", "full")]