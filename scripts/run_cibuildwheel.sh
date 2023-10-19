#!/bin/bash

PYTHON_VERSION="3.8"

function usage {
  echo ""
  echo "Usage: $0 -p|--python <python version> -h|--help"
  echo ""
  echo "Build manylinux wheel."
  echo ""
  echo "Options:"
  echo "  -p, --python     Python wheel version."
  echo "  -h, --help       Print usage"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--python)
      PYTHON_VERSION=$2
      shift
      shift
      ;;
    -h|--help)
      usage
      shift
      ;;
    *)
      echo "Invalid option: $1"
      usage
      ;;
  esac
done

if [[ $PYTHON_VERSION =~ ^3\.([0-9]+) ]]; then
    export CPYTHON_VERSION="cp3${BASH_REMATCH[1]}"
else
    echo "Invalid $PYTHON_VERSION"
    exit 1
fi

export VCPKG_HOST_BINARY_CACHE=/tmp/vcpkg-cache
export VCPKG_INSTALLED_DIR=/project/vcpkg/installed
export MANYLINUX_PLATFORM=manylinux_x86_64
export CIBW_BUILD=$CPYTHON_VERSION-$MANYLINUX_PLATFORM
export CIBW_BUILD_FRONTEND=build
export CIBW_BEFORE_ALL_LINUX="yum install -y zip flex bison gcc-gfortran"
export CIBW_MANYLINUX_X86_64_IMAGE="quay.io/pypa/manylinux_2_28_x86_64"
export VCPKG_TARGET_TRIPLET=x64-linux-dynamic-cxx17-abi1-rel
export CIBW_ENVIRONMENT_LINUX="\
CMAKE_ARGS=-DBUILD_TESTING=OFF \
VCPKG_DEFAULT_BINARY_CACHE=/host$VCPKG_HOST_BINARY_CACHE \
VCPKG_INSTALLED_DIR=$VCPKG_INSTALLED_DIR \
VCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET \
LD_LIBRARY_PATH=$VCPKG_INSTALLED_DIR/$VCPKG_TARGET_TRIPLET/lib"
export CIBW_REPAIR_WHEEL_COMMAND_LINUX="auditwheel repair -w {dest_dir} {wheel} --exclude libarrow_python.so --exclude libarrow.so.1300"
export CIBW_TEST_SKIP="*"
export CIBW_TEST_EXTRAS=
export CIBW_TEST_COMMAND="python -c 'from arcae.lib.arrow_tables import Table'"
export CIBW_VERBOSITY=3

echo "Creating VCPKG Binary Cache in $VCPKG_HOST_BINARY_CACHE"
mkdir -p $VCPKG_HOST_BINARY_CACHE
python -m cibuildwheel --platform linux
