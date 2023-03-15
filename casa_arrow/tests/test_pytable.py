import numpy as np
import pyarrow as pa
import pyarrow.dataset as pad
import pyarrow.parquet as pq

import pytest

from casa_arrow._arrow_tables import Table


@pytest.mark.parametrize("table_suffix, table_name", [
    ("", "MAIN"),
    ("::ANTENNA", "ANTENNA"),
    ("::FEED", "FEED"),
    ("::POLARIZATION", "POLARIZATION"),
    ("::SPECTRAL_WINDOW", "SPECTRAL_WINDOW"),
])
def test_parquet_write(tmp_path, tau_ms, table_suffix, table_name):
    T = Table(f"{tau_ms}{table_suffix}").read_table()
    pq.write_table(T, str(tmp_path / f"{table_name}.parquet"))

def test_column_cases(column_case_table, capfd):
    T = Table(column_case_table).read_table()

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


def test_dataset_predicates(tau_ms, tmp_path):
    pq.write_table(Table(tau_ms).read_table(), str(tmp_path / f"MAIN.parquet"))
    dataset = pad.dataset(tmp_path)
    predicate = ((pad.field("ANTENNA1") >= 0) & (pad.field("ANTENNA1") < 2) &
                 (pad.field("ANTENNA2") >= 2) & (pad.field("ANTENNA2") <= 3))

    T = dataset.to_table(filter=predicate)
    antenna1 = T.column("ANTENNA1").to_numpy()
    antenna2 = T.column("ANTENNA2").to_numpy()

    assert np.all(antenna1 >= 0) and np.all(antenna1 < 2)
    assert np.all(antenna2 >= 2) and np.all(antenna2 <= 3)


def test_duckdb(tau_ms, tmp_path):
    duckdb = pytest.importorskip("duckdb")
    pq.write_table(Table(tau_ms).read_table(), str(tmp_path / f"MAIN.parquet"))

    observation = pad.dataset(tmp_path)  # noqa
    con = duckdb.connect()

    query = con.execute(f"SELECT TIME, ANTENNA1, ANTENNA2, DATA FROM observation "
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

    assert np.all(antenna1 >= 0) and np.all(antenna1 < 2)
    assert np.all(antenna2 >= 2) and np.all(antenna2 <= 3)

