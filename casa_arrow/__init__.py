# Needed to load in the arrow c++ shared libraries
import pyarrow as pa  # noqa
from typing import TYPE_CHECKING

__version__ = "0.1.0"

if TYPE_CHECKING:
    from casa_arrow_arrow_tables import Table

def table(filename: str) -> "Table":
    # Defer cython module import, to avoid conflicts between casa_arrow casacore libraries
    # and python-casacore casacore libraries
    from casa_arrow.arrow_tables import Table
    return Table(filename)
