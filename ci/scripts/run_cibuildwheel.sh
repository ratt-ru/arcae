#!/bin/bash

set -e
set -x

# OpenMP is not present on macOS by default
if [[ $(uname) == "Darwin" ]]; then
  export MACOSX_DEPLOYMENT_TARGET=12.0
  curl -L -O "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh"

  # Install miniforge
  MINIFORGE_PATH=$HOME/miniforge
  bash Miniforge3-$(uname)-$(uname -m).sh -b -p $MINIFORGE_PATH
  source "${MINIFORGE_PATH}/etc/profile.d/conda.sh"
  conda create -n arcae-build -c conda-forge compilers llvm-openmp python
  conda activate arcae-build
fi

python -m pip install cibuildwheel
python -m cibuildwheel
