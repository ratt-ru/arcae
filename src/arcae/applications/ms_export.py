import json
import os
import os.path
import shutil

try:
    from rich.console import Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
except ImportError:
    raise ImportError("rich is not installed.\n" "pip install arcae[applications]")

import pyarrow.parquet as pq

import arcae


class MSExporter:
    class UserInterface:
        def __init__(self):
            self.main_progress = Progress(
                SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn()
            )
            self.main_sub_progress = Progress(
                SpinnerColumn(),
                TextColumn(" " * 4),
                *Progress.get_default_columns(),
                TimeElapsedColumn(),
            )
            self.subtable_progress = Progress(
                SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn()
            )
            self.subtable_sub_progress = Progress(
                SpinnerColumn(),
                TextColumn(" " * 4),
                *Progress.get_default_columns(),
                TimeElapsedColumn(),
            )

        def generate(self):
            progress_group = Group(
                self.main_progress,
                self.main_sub_progress,
                self.subtable_progress,
                self.subtable_sub_progress,
            )
            return Panel(progress_group)

    def __init__(
        self,
        path,
        partition=("FIELD_ID", "DATA_DESC_ID"),
        format="arrow",
        row_chunks=10000,
        output=None,
    ):
        self.path = path
        self.partition = partition
        self.format = format
        self.row_chunks = row_chunks
        self.output = output or os.path.join(os.getcwd(), f"output.{format}")
        self.ui = MSExporter.UserInterface()

    def export(self):
        table = arcae.table(self.path)
        fragment = table.to_arrow(0, 1)
        subtables = self.find_subtables(fragment.schema.metadata)
        shutil.rmtree(self.output, ignore_errors=True)

        with Live(self.ui.generate()):
            self.export_main(table)
            self.export_subtables(subtables)

    def export_main(self, main_table):
        partition_tables = main_table.partition(self.partition)
        main_task = self.ui.main_progress.add_task("", total=len(partition_tables))

        _, table_name = os.path.split(self.path)

        for pi, part_table in enumerate(partition_tables):
            self.ui.main_progress.update(
                main_task,
                description=f"Converted {table_name} ({pi+1}/{len(partition_tables)}) partitions",
            )

            first = True
            part_task = self.ui.main_sub_progress.add_task("", total=part_table.nrow())

            for r, startrow in enumerate(range(0, part_table.nrow(), self.row_chunks)):
                nrow = min(startrow + self.row_chunks, part_table.nrow()) - startrow
                AT = part_table.to_arrow(startrow=startrow, nrow=nrow)

                if first:
                    key = tuple(((p, AT.column(p)[0].as_py()) for p in self.partition))
                    msg = ", ".join(f"{k}: {v}" for k, v in key)
                    out_dir = os.path.join(
                        self.output, "MAIN", *[f"{k}={v}" for k, v in key]
                    )
                    os.makedirs(out_dir, exist_ok=True)
                    first = False

                pq.write_table(AT, os.path.join(out_dir, f"data{r}.parquet"))
                description = f"Converting {msg}: {startrow}-{startrow + nrow}"
                self.ui.main_sub_progress.update(
                    part_task, description=description, advance=nrow
                )

            self.ui.main_progress.update(main_task, advance=1)
            self.ui.main_sub_progress.remove_task(part_task)

        self.ui.main_progress.update(
            main_task,
            description=f"Converted {table_name} to {len(partition_tables)} partitions",
        )

    def export_subtables(self, subtables):
        main_task = self.ui.subtable_progress.add_task("", total=len(subtables))

        for s, subtable_name in enumerate(subtables):
            self.ui.subtable_progress.update(
                main_task,
                description=f"Converted {subtable_name} ({s+1}/{len(subtables)}) subtables",
            )

            subtable = arcae.table(f"{self.path}::{subtable_name}")
            out_dir = os.path.join(self.output, subtable_name)
            os.makedirs(out_dir, exist_ok=True)
            sub_task = self.ui.subtable_sub_progress.add_task("", subtable.nrow())

            for r, startrow in enumerate(range(0, subtable.nrow(), self.row_chunks)):
                nrow = min(startrow + self.row_chunks, subtable.nrow()) - startrow
                AT = subtable.to_arrow(startrow=startrow, nrow=nrow)
                pq.write_table(AT, os.path.join(out_dir, f"data{r}.parquet"))
                description = (
                    f"Converting {subtable_name}: {startrow}-{startrow + nrow}"
                )
                self.ui.subtable_sub_progress.update(
                    sub_task, description=description, advance=nrow
                )

            self.ui.subtable_progress.update(main_task, advance=1)
            self.ui.subtable_sub_progress.remove_task(sub_task)

        self.ui.subtable_progress.update(
            main_task, description=f"Converted {len(subtables)} subtables"
        )

    @classmethod
    def is_valid_subtable(cls, subtable):
        if subtable.startswith("ASDM"):
            return False

        return True

    @classmethod
    def find_subtables(cls, metadata):
        subtables = []

        try:
            meta = json.loads(metadata[b"__arcae_metadata__"])
            casa_kw = meta["__casa_descriptor__"]["_keywords_"]
        except KeyError:
            pass
        else:
            for k, v in casa_kw.items():
                if (
                    isinstance(v, str)
                    and v.startswith("Table: ")
                    and cls.is_valid_subtable(k)
                ):
                    subtables.append(k)

        return subtables
