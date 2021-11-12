#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module is preparing the python path to access all modules.
"""
import os
import sys
from pathlib import Path

# sys.path.insert(0, os.path.dirname(__file__))
#
# PATH_ABSOLUT = Path().parent.absolute()

all_paths = {
    "yaml_path": Path(os.path.dirname(os.path.realpath(__file__))).joinpath("resources/running_exchanges/"),
}
