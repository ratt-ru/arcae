#!/bin/sh

OS=$(uname -s)
ARCH=$(uname -m)

if [[ "$OS" == "Linux" && "$ARCH" == "x86_64" ]]; then
  # We can only rely on the python-casacore wheels for linux x86
  echo "Running complete test suite on $OS $ARCH"
  python -m pytest -s -vvv --pyargs arcae
else
  echo "Running sanity check test on $OS $ARCH"
  python -c "from arcae.testing import sanity; sanity()"
fi
