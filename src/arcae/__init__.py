# Needed to load in the arrow c++ shared libraries
import pyarrow as pa  # noqa
import os
import sys
from typing import TYPE_CHECKING

__version__ = "0.2.2"

if TYPE_CHECKING:
    from arcae.lib.arrow_tables import Table

PYTHON_CASACORE_FOUND = "casacore" in sys.modules
COEXIST_WITH_PYTHON_CASACORE = int(os.environ.get("ARCAE_WITH_CASACORE", 0)) != 0

if PYTHON_CASACORE_FOUND and not COEXIST_WITH_PYTHON_CASACORE:
    raise RuntimeError(
        "python-casacore has already been imported and "
        "this may lead to extension module symbol conflicts "
        "and segfaults."
        "See https://github.com/ratt-ru/arcae/issues/72 "
        "for further information. "
        "Set ARCAE_WITH_CASACORE=1 if you wish to "
        "continue regardless.")


def table(filename: str) -> "Table":
    # Defer cython module import, to avoid conflicts between arcae casacore libraries
    # and python-casacore casacore libraries
    from arcae.lib.arrow_tables import Table
    return Table.from_filename(filename)
