from pathlib import Path
import pyarrow as pa
import re
from setuptools import find_packages
from skbuild import setup

pa.create_library_symlinks()

# https://github.com/scikit-build/scikit-build/issues/521
for i in (Path(__file__).resolve().parent / "_skbuild").rglob("CMakeCache.txt"):
    i.write_text(re.sub("^//.*$\n^[^#].*build-env.*$", "", i.read_text(), flags=re.M))

setup(packages=find_packages())
