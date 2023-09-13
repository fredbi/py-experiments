"""
Module dataframe insulates external dependencies
for the DataFrame abstraction.

Other modules consuming a DataFrame don't have to
directly import from dask
"""

from __future__ import annotations

__auhor__ = "fredbi"

# from typing import (
#    TypeAlias,   # python 3.10
# )
from dask.dataframe import DataFrame as DaskDataFrame
from dask.dataframe import Series as DaskSeries

DataFrame = DaskDataFrame
Series = DaskSeries
