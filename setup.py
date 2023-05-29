import os
from skbuild import setup

# from Cython.Build import cythonize
import numpy as np
import pyarrow as pa


def create_extensions():
    casa_libs = ["casa_derivedmscal", "casa_meas", "casa_ms", "casa_tables"]

    pa.create_library_symlinks()
    include_dirs = [np.get_include(), pa.get_include()]
    library_dirs = pa.get_library_dirs()
    libraries = casa_libs + pa.get_libraries()

    with open("build_log.txt", "w") as f:
        print(f"include_dirs={include_dirs}", file=f)
        print(f"library_dirs={library_dirs}", file=f)
        print(f"libraries={libraries}", file=f)

    ext_modules = [Extension("casa_arrow._arrow_tables", ["casa_arrow/*.pyx"])]
    ext_modules = cythonize(ext_modules)

    for ext in ext_modules:
        # The Numpy C headers are currently required
        ext.include_dirs.extend(include_dirs)
        ext.libraries.extend(libraries)
        ext.library_dirs.extend(library_dirs)

        if os.name == "posix":
            ext.extra_compile_args.extend(["--std=c++17"])

    root = "casa_arrow"

    return {
            "ext_modules": ext_modules,
            "packages": [root] + [f"{root}.{item}" for item in find_packages(where=root)],
            "include_package_data": True,
     }

if VCPKG_ROOT := os.environ.get("VCPKG_ROOT"):
    toolchain = os.path.join(VCPKG_ROOT, "scripts", "buildsystems", "vcpkg.cmake")

    if not os.path.exists(toolchain):
        raise ValueError("Toolchain {toolchain} does not exist")
else:
    raise ValueError("VCPKG_ROOT not set")


pa.create_library_symlinks()

CMAKE_ARGS = [
    f"-DPYARROW_INCLUDE={pa.get_include()}",
    f"-DPYARROW_LIBDIRS={' '.join(pa.get_library_dirs())}",
    f"-DPYARROW_LIBS={' '.join(pa.get_libraries())}",
    f"-DCMAKE_TOOLCHAIN_FILE={toolchain}"]

setup(
    packages=["casa_arrow"],
    cmake_args=CMAKE_ARGS
)
