from tempfile import TemporaryDirectory
import os

import numpy as np

from arcae.lib.arrow_tables import Table

NROW = 10

def sanity():
  """Sanity check an arcae install"""
  with TemporaryDirectory(prefix="arcae-sanity") as dir:
    ms = os.path.join(dir, "sanity.ms")

    with Table.ms_from_descriptor(ms) as T:
      T.addrows(NROW)
      np.testing.assert_equal(T.nrow(), NROW)
      ants_inv = np.flipud(np.arange(NROW))
      T.putcol("ANTENNA1", ants_inv)
      np.testing.assert_array_equal(T.getcol("ANTENNA1"), ants_inv)