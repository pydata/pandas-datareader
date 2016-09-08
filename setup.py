#!/usr/bin/env python
# -*- coding: utf-8 -*-

from ast import parse
import os
from setuptools import setup, find_packages


NAME = 'pandas-datareader'


def version():
    """Return version string."""
    with open(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           'pandas_datareader',
                           '__init__.py')) as input_file:
        for line in input_file:
            if line.startswith('__version__'):
                return parse(line).body[0].value.s


def readme():
    with open('README.rst') as f:
        return f.read()

INSTALL_REQUIRES = (
    ['pandas', 'requests>=2.3.0', 'requests-file', 'requests-ftp']
)

setup(
    name=NAME,
    version=version(),
    description="Data readers extracted from the pandas codebase,"
                "should be compatible with recent pandas versions",
    long_description=readme(),
    license='BSD License',
    author='The PyData Development Team',
    author_email='pydata@googlegroups.com',
    url='https://github.com/pydata/pandas-datareader',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Operating System :: OS Independent',
        'Programming Language :: Cython',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Scientific/Engineering',
    ],
    keywords='data',
    install_requires=INSTALL_REQUIRES,
    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),
    test_suite='tests',
    zip_safe=False,
)
