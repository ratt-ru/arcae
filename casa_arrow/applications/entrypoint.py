import os
import os.path
import shutil
import time

import click
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn

import casa_arrow as ca
import pyarrow.parquet as pq


@click.group()
@click.pass_context
@click.option("--debug/--no-debug", default=False)
def main(ctx, debug):
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug

def process_partitions(ctx, param, value):
    if isinstance(value, str):
        partitions = [p.strip() for p in value.split(",")]
        partitions = [p for p in partitions if p]
        return partitions

    raise click.BadParameter("partition must be of the form \"COLUMN1,COLUMN2,COLUMNN\"")

@main.command()
@click.pass_context
@click.argument("ms", required=True)
@click.option("-p", "--partition", default="FIELD_ID,PROCESSOR_ID,DATA_DESC_ID", callback=process_partitions)
@click.option("-f", "--format", type=click.Choice(["arrow"]), default="arrow")
@click.option("-nr", "--nrow", type=int, default=1000)
@click.option("-o", "--output", type=str, default=None)
def export(ctx, ms, partition, format, nrow, output):
    table = ca.table(ms)
    columns = table.columns()

    if not output:
        output = os.path.join(os.getcwd(), f"output.{format}")

    shutil.rmtree(output)

    if not set(partition).issubset(columns):
        raise ValueError(f"Some partition(s) "
                         f"{set(partition).difference(columns)} "
                         f"are not columns of {ms}")

    partition_tables = table.partition(partition)
    partition_data = {}

    with Progress(SpinnerColumn(), *Progress.get_default_columns(), TimeElapsedColumn()) as progress:
        for pt in partition_tables:
            T = pt.to_arrow(0, 1, partition)
            key = tuple(((p, T.column(p).to_numpy()[0]) for p in partition))
            msg = ", ".join(f"{k}: {v}" for k, v in key)
            task = progress.add_task(msg, total=pt.nrow())
            partition_data[key] = {"task": task, "table": pt, "msg": msg}

        while not progress.finished:
            for key, task_data in partition_data.items():
                parts = [f"{k}={v}" for k, v in key]
                out_dir = os.path.join(output, "MAIN", *parts)
                os.makedirs(out_dir, exist_ok=True)

                for r, startrow in enumerate(range(0, task_data["table"].nrow(), nrow)):
                    nr = min(startrow + nrow, task_data["table"].nrow()) - startrow
                    AT = pt.to_arrow(startrow=startrow, nrow=nr)
                    msg = task_data["msg"]
                    pq.write_table(AT, os.path.join(out_dir, f"data{r}.parquet"))
                    progress.update(task_data["task"], description=f"{msg}: {startrow}-{startrow + nr}", advance=nr)
