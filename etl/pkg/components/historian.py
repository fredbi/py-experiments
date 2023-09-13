"""
Module historian provides utiities to build a historical record into the target database.
"""

from __future__ import annotations

__author__ = "fredbi"

from typing import Tuple
import dataclasses
from dataclasses import dataclass

from dask import (
    dataframe,
    delayed,
)

from etl.pkg.components import interface
from etl.pkg.dataframe import DataFrame

@dataclass
class HistoryBuilder(interface.Pipable):
    """
    Identifier _summary_

    Args:
        interface (_type_): _description_
    """
    def __post_init__(self):
        pass

    @delayed
    def pipe(self, input_df: DataFrame, **kwargs) -> Tuple[DataFrame,DataFrame]:
        """
        _summary_

        Args:
            input_df (dataframe): _description_

        Returns:
            dataframe: _description_
        """
        return None
