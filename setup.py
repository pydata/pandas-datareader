#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

import versioneer

NAME = "pandas-datareader"


def readme():
    with open("README.md") as f:
        return f.read()


install_requires = []
with open("./requirements.txt") as f:
    install_requires = f.read().splitlines()
with open("./requirements-dev.txt") as f:
    tests_require = f.read().splitlines()

setup(
    name=NAME,
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Data readers extracted from the pandas codebase,"
    "should be compatible with recent pandas versions",
    long_description=readme(),
    license="BSD License",
    author="The PyData Development Team",
    author_email="pydata@googlegroups.com",
    url="https://github.com/pydata/pandas-datareader",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
    ],
    keywords="data",
    install_requires=install_requires,
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    test_suite="tests",
    tests_require=tests_require,
    zip_safe=False,
    python_requires=">=3.6",
)
