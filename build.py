from typing import Dict, Any

def build(setup_kwargs: Dict[str, Any]):
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

    ext_modules = [
        Pybind11Extension(
            "casa_arrow.pytable",
            list(sorted(glob("src/*.cpp"))),  # Sort source files for reproducibility
            include_dirs=include_dirs,
            library_dirs=library_dirs,
            extra_compile_args=["-O0", "-g", "--std=c++17", "-D_GLIBCXX_USE_CXX11_ABI=1"],
            libraries=libraries
        ),
    ]

    setup_kwargs.update(
        {
            "ext_modules": ext_modules,
            "zip_safe": False,
        }
    )
