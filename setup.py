from argparse import ArgumentParser
from distutils.core import setup
from multiprocessing import cpu_count

from Cython.Build import cythonize

setup(
    name='mapit',
    ext_modules=cythonize([
        'as2org.pyx',
        'routing_table.pyx'
    ], nthreads=0)
)
