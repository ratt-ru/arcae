from typing import Dict, Any

def build(setup_kwargs: Dict[str, Any]):
    import os
    from Cython.Build import cythonize
    from glob import glob
    import numpy as np
    import pyarrow as pa
    from pybind11.setup_helpers import Pybind11Extension

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
            ext.extra_compile_args.extend(["--std=c++17", "-D_GLIBCXX_USE_CXX11_ABI=1"])


    # ext_modules.append(
    #     Pybind11Extension(
    #         "casa_arrow._pytable",
    #         list(sorted(glob("src/*.cpp"))),  # Sort source files for reproducibility
    #         include_dirs=include_dirs,
    #         library_dirs=library_dirs,
    #         extra_compile_args=["--std=c++17", "-D_GLIBCXX_USE_CXX11_ABI=1"],
    #         extra_link_args=["--std=c++17", "-D_GLIBCXX_USE_CXX11_ABI=1"],
    #         libraries=libraries,
    #     ),
    # )

    setup_kwargs.update(
        {
            "ext_modules": ext_modules,
            "zip_safe": False,
        }
    )
