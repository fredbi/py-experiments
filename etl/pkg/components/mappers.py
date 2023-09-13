"""
Module mappers provides utiities to map taxonomies.
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
from etl.pkg import resolvers


@dataclass
class TaxonomyMapper(interface.Pipable):
    """
    Identifier _summary_

    Args:
        interface (_type_): _description_
    """

    def __post_init__(self):
        pass

    @delayed
    def pipe(self, input_df: DataFrame, **kwargs) -> Tuple[DataFrame, DataFrame]:
        """
        resolve_taxonomies

        Args:
            input_df (dataframe): _description_

        Returns:
            [dataframe]: _description_
        """
        return None

    @delayed
    def check_taxonomies(self, _input_df: DataFrame) -> DataFrame:
        """
        _summary_

        Args:
            input_df (dataframe): _description_

        Returns:
            dataframe: _description_
        """
        return None


@dataclass
class EntityMapper(interface.Pipable):
    """
    Identifier _summary_

    Args:
        interface (_type_): _description_
    """

    def __post_init__(self):
        pass

    @delayed
    def pipe(self, input_df: DataFrame, **kwargs) -> Tuple[DataFrame, DataFrame]:
        """
        resolve_taxonomies

        Args:
            input_df (dataframe): _description_

        Returns:
            [dataframe]: _description_
        """
        return None

    @delayed
    def map_remainder(self, _input_df: DataFrame) -> DataFrame:
        """
        _summary_

        Args:
            input_df (dataframe): _description_

        Returns:
            dataframe: _description_
        """
        return None

    @delayed
    def map_target_entities(self, _input_df: DataFrame) -> [DataFrame]:
        """
        _summary_

        Args:
            input_df (dataframe): _description_

        Returns:
            [dataframe]: _description_
        """
        return None
