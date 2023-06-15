from skbuild import setup
from setuptools import find_packages
import pyarrow as pa

pa.create_library_symlinks()

setup(packages=find_packages())
