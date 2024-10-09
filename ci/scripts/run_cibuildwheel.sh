#!/bin/bash

set -e
set -x

# OpenMP is not present on macOS by default
if [[ $(uname) == "Darwin" ]]; then
  export MACOSX_DEPLOYMENT_TARGET=12.0
  if [[ "$CIBW_BUILD" == *-macosx_arm64 ]]; then
    OPENMP_URL="https://anaconda.org/conda-forge/llvm-openmp/11.1.0/download/osx-arm64/llvm-openmp-11.1.0-hf3c4609_1.tar.bz2"
  else
    OPENMP_URL="https://anaconda.org/conda-forge/llvm-openmp/11.1.0/download/osx-64/llvm-openmp-11.1.0-hda6cdc1_1.tar.bz2"
  fi

  # This depends on miniforge installation
  conda create -n arcae-build $OPENMP_URL
  PREFIX="$CONDA_HOME/envs/arcae-build"

  export CC=/usr/bin/clang
  export CXX=/usr/bin/clang++
  export CPPFLAGS="$CPPFLAGS -Xpreprocessor -fopenmp"
  export CFLAGS="$CFLAGS -I$PREFIX/include"
  export CXXFLAGS="$CXXFLAGS -I$PREFIX/include"
  export LDFLAGS="$LDFLAGS -Wl,-rpath,$PREFIX/lib -L$PREFIX/lib -lomp"
fi

python -m pip install cibuildwheel
python -m cibuildwheel
