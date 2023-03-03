# -*- coding: utf-8 -*-
from glob import glob
import os
from setuptools import setup, find_packages

from Cython.Build import cythonize
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

    ext_modules = cythonize("casa_arrow/arrow_tables.pyx")

    for ext in ext_modules:
        # The Numpy C headers are currently required
        ext.include_dirs.extend(include_dirs)
        ext.libraries.extend(libraries)
        ext.library_dirs.extend(library_dirs)
        ext.sources.extend(list(sorted(glob("src/*.cpp"))))

        if os.name == "posix":
            ext.extra_compile_args.extend(["--std=c++17"])

    root = "casa_arrow"

    return {
            "ext_modules": ext_modules,
            "packages": [root] + [f"{root}.{item}" for item in find_packages(where=root)],
            "include_package_data": True,
     }


setup(**create_extensions())
