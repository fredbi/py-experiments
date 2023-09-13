"""
Module identifiers provides utiities to match identifiers against the target database.
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
from etl.pkg import validators


@dataclass
class Identifier(interface.Pipable):
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
        identify_entity picks all external identifiers from the input dataframe
        then return a dataframe with 2 columns:
        [{external identifier}, {numerical_identifier}].


        Args:
            input_df (dataframe): _description_

        Returns:
            dataframe: _description_
        """

        return None
