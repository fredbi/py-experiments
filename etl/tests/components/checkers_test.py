"""
Tests for module etl.pkg.components.checkers     
"""

from __future__ import annotations

__author__ = "fredbi"

import logging
import unittest
from typing import Tuple

import pandas as pd
from dask import dataframe as ddf

from etl.pkg import dataframe, policy, validators
from etl.pkg.components import checkers

LOGGER = logging.getLogger(__name__)  # for debug


class TestContentChecker(unittest.TestCase):
    """
    TestContentChecker exercises the components.ContentChecker class.
    """

    def make_checker(
        self,
        fields_validator: validators.FieldsValidator,
        validation_policy: policy.Policy = policy.strict(),
    ) -> checkers.ContentChecker:
        """Build a checker with a strict policy"""

        return checkers.ContentChecker(
            fields_validator=fields_validator, policy=validation_policy
        )

    def make_frame(self) -> pd.DataFrame:
        """make_frame generates a panda dataframe from testing"""

        return pd.DataFrame(dict(x=list([1, 2, 3, 4]), y=list(["a", "b", "c", "d"])))

    def test_with_nop_validation(self):
        """Exercise the checker pipe with an always true validator"""

        test_case = self.make_frame()
        input_df = ddf.from_pandas(test_case, 2)

        checker = self.make_checker(
            fields_validator=validators.FieldsValidator({"x": self.nop_validator()})
        )
        output_df, report_df = checker.pipe(input_df)

        result, report = ddf.compute(output_df, report_df)

        self.assertEqual(result.index.size, test_case.index.size)
        result_columns = list(result.columns.values)
        for input_column in test_case.columns:
            self.assertTrue(input_column in result_columns)

        self.assertTrue("etl_validate_nop_x" in result_columns)

        report_columns = list(report.columns.values)
        self.assertEqual(report.index.size, 0)
        for report_column in test_case.columns:
            self.assertTrue(report_column in report_columns)

        self.assertTrue("etl_validate_nop_x" in report_columns)
        self.assertTrue("etl_validate_nop_x_error" in report_columns)

    def test_with_multiple_validations(self):
        """Exercise the checker pipe with several validators"""

        test_case = self.make_frame()
        input_df = ddf.from_pandas(test_case, 2)

        checker = self.make_checker(
            fields_validator=validators.FieldsValidator(
                {
                    "x": self.index_validator(2),
                    "y": self.nop_validator(),
                }
            ),
            validation_policy=policy.skip(),
        )
        output_df, report_df = checker.pipe(input_df)

        # TODO: document exec graph
        result, report = ddf.compute(output_df, report_df)

        LOGGER.info("result: %s", result)
        LOGGER.info("report: %s", report)

    def nop_validator(self) -> Tuple[validators.SeriesValidator, str]:
        """A validator that just returns true"""

        def validator(input_series: dataframe.Series) -> dataframe.DataFrame:
            return input_series.map(lambda x: True).rename(
                f"{input_series.name}_nop",
            )

        return validator, "nop"

    def index_validator(self, index: int) -> Tuple[validators.SeriesValidator, str]:
        """A validator that returns true on a given index"""

        def validator(input_series: dataframe.Series) -> dataframe.DataFrame:
            return (
                ddf.merge(
                    input_series.to_frame(),
                    input_series[input_series.index == index],
                    how="left",
                    left_index=True,
                    right_index=True,
                    indicator=True,
                )["_merge"]
                .replace(["left_only", "both"], value=[False, True])
                .rename(
                    f"{input_series.name}_index",
                )
            )

        return validator, "index"
