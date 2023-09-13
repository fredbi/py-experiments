"""
Module validators exposes utilities to validate dataframes.
"""

from __future__ import annotations

__author__ = "fredbi"

import re
from typing import Any, Callable, Dict, Tuple

from dask import dataframe as ddf

from etl.pkg import exceptions
from etl.pkg.dataframe import DataFrame, Series

# from etl.pkg.components import interface

# import pydantic

SeriesValidator = Callable[[Series], DataFrame]


class FieldsValidator(Dict[str, Tuple[SeriesValidator,str]]):
    """
    FieldsValidator knows how to distribute validators from a dictionary of fields.

    Args:
        Dict (_type_): _description_
    """

    def __init__(self, *args):
        super().__init__(*args)

    def get_field_validator(self, field: str) -> [SeriesValidator,str]:
        """
        get_field_validator returns the validator for a given field name.

        Args:
            field (str): field (column) name

        Raises:
            UndeclaredField: raise d if the field is not declared in the dictionary

        TODO(fred): return Tuple with validator's name.

        Returns:
            SeriesValidator: a SeriesValidator function
        """
        validator,name = super().get(field)
        if validator is None:
            raise exceptions.UndeclaredFieldError

        return validator,name


def regexp_validator(regexp: re.Pattern, **kwargs) -> Tuple[SeriesValidator,str]:
    """
    regexp_validator returns a function that checks that the inît string value
    matches a regexp, using the fullmatch operator on the first column of the dataframe.

    Args:
        regexp (re.Pattern): the regexp to check

    Returns:
        SeriesValidator: the validator function
    """

    def validator(input_series: Series) -> DataFrame:
        return input_series.fullmatch(regexp, **kwargs).rename(
            f"{input_series.name}_match",
        )

    return validator,"regexp"


def na_validator() -> Tuple[SeriesValidator,str]:
    """na_validators checks for N/A values in the input series"""

    def validator(input_series: Series) -> DataFrame:
        return input_series.isna().rename(f"{input_series.name}_na")

    return validator,"na"


def minmax_validator(
    minimum: Any, maximum: Any, inclusive: str = "both"
) -> Tuple[SeriesValidator,str]:
    """minmax_validator returns a function that checks that the values
    of the input series are within the (min,max) boundaries.

    Arguments:
      inclusive:"left", "right", "both"

    Returns:
        SeriesValidator: the validator function on an input Series
    """

    def validator(input_series: Series) -> DataFrame:
        return input_series.between(minimum, maximum, inclusive).rename(
            f"{input_series.name}_between"
        )

    return validator,"minmax"


def date_validator(date_format="%Y-%M-%d", **kwargs) -> Tuple[SeriesValidator,str]:
    """date_validator returns a function that validates a date from
    a string, given a date format.
    
    The default is the ISO date format YYYY-MM-DD.
    """

    def validator(input_series: Series) -> DataFrame:
        return ddf.to_datetime(
            input_series,
            format=date_format,
            errors="coerce",
            **kwargs,
        ).notnull()

    return validator,"date"
