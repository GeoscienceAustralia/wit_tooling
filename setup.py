import numpy as np
import sys

from setuptools import setup, find_packages, Extension
from setuptools import setup, Extension
from os import path

from Cython.Distutils import build_ext

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()

macros = []

npinclude = np.get_include()
rdinclude = np.random.__path__[0] + '/'

if sys.platform == 'darwin':
    # Needs openmp lib installed: brew install libomp
    cc_flags = ["-I/usr/local/include", "-Xpreprocessor", "-fopenmp"]
    ld_flags = ["-L/usr/local/lib", "-lomp"]
else:
    cc_flags = ['-fopenmp']
    ld_flags = ['-fopenmp']

build_cfg = dict(
    include_dirs=[npinclude],
    extra_compile_args=cc_flags,
    extra_link_args=ld_flags,
    define_macros=macros,
)

extensions = [
        Extension('wit_tooling.polygon_drill', ['wit_tooling/polygon_drill.pyx'], **build_cfg),
]

setup(
    name="wit_tooling",
    version='2.1',
    description='WIT tooling',
    long_description_content_type='text/markdown',
    author='Emma Ai',
    author_email='emma.ai@ga.gov.au',
    packages=find_packages(exclude=['test']),
    python_requires='>=3.5',
    setup_requires=["Cython>=0.23"],
    install_requires=["numpy>=1.16", "Cython>=0.23", "mpi4py>=3.0.3"],
    extras_require={
                    'dev': ['check-manifest'],
    },
    cmdclass = {'build_ext': build_ext},
    ext_modules = extensions
)
