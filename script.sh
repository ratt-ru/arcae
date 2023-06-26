export VCPKG_HOST_BINARY_CACHE=/tmp/vcpkgc
mkdir -p $VCPKG_HOST_BINARY_CACHE
export VCPKG_INSTALLED_DIR=/project/vcpkg/installed
export CIBW_BUILD=cp38-manylinux_x86_64
export CIBW_BUILD_FRONTEND=build
export CIBW_BEFORE_ALL_LINUX="yum install -y zip flex bison gcc-gfortran"
export VCPKG_TARGET_TRIPLET=x64-linux-dynamic-cxx17-abi0-rel
export CIBW_ENVIRONMENT_LINUX="\
VCPKG_DEFAULT_BINARY_CACHE=/host$VCPKG_HOST_BINARY_CACHE \
VCPKG_INSTALLED_DIR=$VCPKG_INSTALLED_DIR \
VCPKG_TARGET_TRIPLET=$VCPKG_TARGET_TRIPLET \
LD_LIBRARY_PATH=$VCPKG_INSTALLED_DIR/$VCPKG_TARGET_TRIPLET/lib"
export CIBW_REPAIR_WHEEL_COMMAND_LINUX="auditwheel repair -w {dest_dir} {wheel} --exclude libarrow_python.so --exclude libarrow.so.1200"
export CIBW_TEST_EXTRAS=test
export CIBW_TEST_COMMAND="echo \$(pwd) && py.test -s -vvv --pyargs casa_arrow"
export CIBW_VERBOSITY=3
python -m cibuildwheel --platform linux
