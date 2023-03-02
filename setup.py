# -*- coding: utf-8 -*-
from glob import glob
import os
from setuptools import setup
from typing import Dict, Any

from Cython.Build import cythonize
import numpy as np
import pyarrow as pa


packages = ["casa_arrow", "casa_arrow.tests"]

package_data = {"": ["*"]}

setup_kwargs = {
    "name": "casa-arrow",
    "version": "0.1.0",
    "description": "",
    "long_description": "C++ and Python Arrow Bindings for casacore",
    "author": "Simon Perkins",
    "author_email": "simon.perkins@gmail.com",
    "maintainer": "None",
    "maintainer_email": "None",
    "url": "None",
    "packages": packages,
    "package_data": package_data,
    "python_requires": ">=3.8,<4.0",
    "setup_requires": ["pyarrow == 11.0.0"],
}


def build(setup_kwargs: Dict[str, Any]):
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

    setup_kwargs.update(
        {
            "ext_modules": ext_modules,
            "zip_safe": False,
        }
    )


build(setup_kwargs)

setup(**setup_kwargs)
