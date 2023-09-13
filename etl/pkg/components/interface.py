"""
Module components exposes the abstract class Executable,
an interface which knows how to report an execution.
"""

from __future__ import annotations

__author__ = "fredbi"

from abc import ABC, abstractmethod
from typing import Tuple

from etl.pkg.dataframe import DataFrame

# type ResultWithReport = Tuple[DataFrame, DataFrame] python3.12
ResultWithReport = Tuple[DataFrame, DataFrame]


class Executable(ABC):
    """
    Executable knows how to execute things.
    """

    @abstractmethod
    def execute(self, **kwargs) -> DataFrame:
        """
        execute a workflow and return a report.

        Returns:
            DataFrame: the execution report folded as a DataFrame
        """
        return None


class Pipable(ABC):
    """
    Pipable knows how to pipe an input dataframe into a result dataframe,
    together with a report dataframe.
    """

    @abstractmethod
    def pipe(self, input_df: DataFrame, **kwargs) -> ResultWithReport:
        """
        Process the input dataframe.

        Returns:
            Tuple[DataFrame,DataFrame]: the output DataFrame and an execution report.
        """
        return None


class NopPipe(Pipable):
    """
    NopPipe is a no-operation pipe used for empty initializers
    """

    def pipe(self, input_df: DataFrame, **kwargs) -> Tuple[DataFrame, DataFrame]:
        return Tuple[None, None]
