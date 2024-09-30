import numpy as np
import pyarrow as pa
import pytest

from arcae.lib.arrow_tables import GroupSortData, merge_groups

SORT_KEYS = [
    ("GROUP_0", "ascending"),
    ("GROUP_1", "ascending"),
    ("TIME", "ascending"),
    ("ANTENNA1", "ascending"),
    ("ANTENNA2", "ascending"),
    ("ROW", "ascending"),
]


def test_sorting():
    data = pa.Table.from_pydict(
        {
            "GROUP_0": pa.array([0, 1, 0, 1], pa.int32()),
            "GROUP_1": pa.array([1, 0, 1, 0], pa.int32()),
            "TIME": pa.array([3, 2, 1, 0], pa.float64()),
            "ANTENNA1": pa.array([0, 0, 0, 0], pa.int32()),
            "ANTENNA2": pa.array([1, 1, 1, 1], pa.int32()),
            "ROW": pa.array([0, 1, 2, 3], pa.int64()),
        }
    )

    gsd = GroupSortData(
        [
            data["GROUP_0"].combine_chunks(),
            data["GROUP_1"].combine_chunks(),
        ],
        data["TIME"].combine_chunks(),
        data["ANTENNA1"].combine_chunks(),
        data["ANTENNA2"].combine_chunks(),
        data["ROW"].combine_chunks(),
    )

    assert gsd.sort().to_arrow().equals(data.sort_by(SORT_KEYS))


@pytest.mark.parametrize("seed", [42])
@pytest.mark.parametrize("n, chunks", [(100, 33), (125, 42)])
def test_merging(n, chunks, seed):
    rng = np.random.default_rng(seed=seed)
    group_0 = rng.integers(0, 10, n)
    group_1 = rng.integers(0, 10, n)
    time = rng.random(n)
    ant1 = rng.integers(0, 10, n)
    ant2 = rng.integers(0, 10, n)
    row = rng.integers(0, 10, n)

    data = pa.Table.from_pydict(
        {
            "GROUP_0": pa.array(group_0, pa.int32()),
            "GROUP_1": pa.array(group_1, pa.int32()),
            "TIME": pa.array(time, pa.float64()),
            "ANTENNA1": pa.array(ant1, pa.int32()),
            "ANTENNA2": pa.array(ant2, pa.int32()),
            "ROW": pa.array(row, pa.int64()),
        }
    )

    gsds = []

    # Split test data into GroupSortData and sort
    for start in range(0, n, chunks):
        batch = data.slice(start, chunks)
        gsd = GroupSortData(
            [
                batch["GROUP_0"].combine_chunks(),
                batch["GROUP_1"].combine_chunks(),
            ],
            batch["TIME"].combine_chunks(),
            batch["ANTENNA1"].combine_chunks(),
            batch["ANTENNA2"].combine_chunks(),
            batch["ROW"].combine_chunks(),
        )

        gsds.append(gsd.sort())

    # Test that merging matches sorted data
    assert merge_groups(gsds).to_arrow().equals(data.sort_by(SORT_KEYS))
