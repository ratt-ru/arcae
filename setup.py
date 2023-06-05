import os
from os.path import join as pjoin
from skbuild import setup
from setuptools import find_packages

import numpy as np
import pyarrow as pa

if VCPKG_ROOT := os.environ.get("VCPKG_ROOT"):
    toolchain = pjoin(VCPKG_ROOT, "scripts", "buildsystems", "vcpkg.cmake")

    if not os.path.exists(toolchain):
        raise ValueError("Toolchain {toolchain} does not exist")
else:
    raise ValueError("VCPKG_ROOT not set")

pa.create_library_symlinks()
CI_VCPKG_PATH = pjoin(os.getcwd(), "ci", "vcpkg")
OVERLAY_TRIPLETS = pjoin(CI_VCPKG_PATH, "custom-triplets")
PORT_DIR = pjoin(CI_VCPKG_PATH, "custom-ports")
CUSTOM_PORTS = ["casacore"]
CUSTOM_PORT_PATHS = [pjoin(PORT_DIR, port) for port in CUSTOM_PORTS]
OVERLAY_PORTS = os.path.pathsep.join(CUSTOM_PORT_PATHS)

CMAKE_ARGS = [
    f"-DNUMPY_INCLUDE={np.get_include()}",
    f"-DPYARROW_INCLUDE={pa.get_include()}",
    f"-DPYARROW_LIBDIRS={' '.join(pa.get_library_dirs())}",
    f"-DPYARROW_LIBS={' '.join(pa.get_libraries())}",
    f"-DCMAKE_TOOLCHAIN_FILE={toolchain}",
    f"-DVCPKG_OVERLAY_TRIPLETS={OVERLAY_TRIPLETS}",
    f"-DVCPKG_OVERLAY_PORTS={OVERLAY_PORTS}",
    f"-DVCPKG_TARGET_TRIPLET=x64-linux-dynamic-cxx17-abi0",
    f"-DVCPKG_HOST_TRIPLET=x64-linux-dynamic-cxx17-abi0",
    f"-S {CI_VCPKG_PATH}"]
setup(
    packages=find_packages(),
    cmake_args=CMAKE_ARGS,
    cmake_source_dir=CI_VCPKG_PATH,
)
