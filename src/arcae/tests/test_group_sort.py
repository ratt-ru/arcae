import pyarrow as pa

from arcae.lib.arrow_tables import GroupSortData


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

    keys = [
        ("GROUP_0", "ascending"),
        ("GROUP_1", "ascending"),
        ("TIME", "ascending"),
        ("ANTENNA1", "ascending"),
        ("ANTENNA2", "ascending"),
        ("ROW", "ascending"),
    ]

    assert gsd.sort().to_arrow().equals(data.sort_by(keys))
