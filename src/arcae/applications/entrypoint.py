try:
    import click
except ImportError as e:
    raise ImportError(
        "click is not installed.\n" "pip install arcae[applications]"
    ) from e

from arcae.applications.ms_export import MSExporter


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

    raise click.BadParameter('partition must be of the form "COLUMN1,COLUMN2,COLUMNN"')


@main.command()
@click.pass_context
@click.argument("ms", required=True)
@click.option(
    "-p",
    "--partition",
    default="FIELD_ID,PROCESSOR_ID,DATA_DESC_ID",
    callback=process_partitions,
)
@click.option("-f", "--format", type=click.Choice(["arrow"]), default="arrow")
@click.option("-nr", "--nrow", type=int, default=1000)
@click.option("-o", "--output", type=str, default=None)
def export(ctx, ms, partition, format, nrow, output):
    MSExporter(ms, partition, format, nrow, output).export()
