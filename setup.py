#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup

import versioneer

with open("./requirements.txt") as f:
    install_requires = f.read().splitlines()

with open("./requirements-dev.txt") as f:
    dev_requires = f.read().splitlines()

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=install_requires,
    extras_require={
        "dev": dev_requires
    },
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    zip_safe=False,
    python_requires=">=3.6",
)
