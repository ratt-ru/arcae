# Needed to load in the arrow c++ shared libraries
import pyarrow as pa  # noqa
from typing import TYPE_CHECKING

__version__ = "0.2.0"

if TYPE_CHECKING:
    from arcae.lib.arrow_tables import Table

def table(filename: str) -> "Table":
    # Defer cython module import, to avoid conflicts between arcae casacore libraries
    # and python-casacore casacore libraries
    from arcae.lib.arrow_tables import Table
    return Table.from_filename(filename)
