"""
Module checkers provide data checkers.
"""

from __future__ import annotations

__author__ = "fredbi"

import dataclasses
from dataclasses import dataclass
from typing import Any, List

# from dask import dataframe, delayed
from etl.pkg import exceptions, policy, resolvers, validators
from etl.pkg.components import interface
from etl.pkg.dataframe import DataFrame


@dataclass
class SchemaChecker(interface.Pipable):
    """
    Schema _summary_

    Args:
        interface (_type_): _description_
    """

    schema_resolver: resolvers.SchemaResolver

    def __post_init__(self):
        pass

    def pipe(self, input_df: DataFrame, **kwargs) -> interface.ResultWithReport:
        """
        schema_check verifies the correctness of the input schema.
        # TODO: is it still relevant?

        Returns:
            dataframe: _description_
        """
        # TODO
        return input_df, None


@dataclass(frozen=True)
class ContentChecker(interface.Pipable):
    """
    ContentChecker exposes a pipeline to validate the input columns of a DataFrame.

    The pipe method parallelizes all field validations.

    The content checker is aware of the validation policy provided when filtering the input.
    """

    fields_validator: validators.FieldValidator = dataclasses.field(
        default_factory=validators.FieldsValidator
    )
    policy: policy.Policy = dataclasses.field(default_factory=policy.strict)

    def __post_init__(self):
        if self.fields_validator is None:
            raise exceptions.DevMistakeError(
                "ContentChecker's FieldsValidator cannot be left to None"
            )

    def pipe(self, input_df: DataFrame, **kwargs) -> interface.ResultWithReport:
        """
        pipe performs validation on every column to validate
        in the input dataframe.

        The list of raw input columns to be validated by the ETL
        is configured by the "fieldsValidator"
        argument when constructing an instance of the ETL processor.

        The validation of each column is carried out in parallel.

        The behavior depends on the policy configured for this ETL instance:
        if a failure occurs on a field marked with a RequiresValid policy,
        this pipeline raises an `exceptions.InvalidDataError` exception.

        Args:
            input_df (dataframe): the input dataframe to submit to validations

        Returns:
            dataframe: the input dataframe augmented with boolean columns
            with their validation status.
        """

        # schedule a parallel check on the columns configured as subject to validation
        columns = set(input_df.columns)
        want_to_check = set(self.fields_validator.keys())
        columns_to_check = list(columns.intersection(want_to_check))
        validation_flag_columns: List[str] = []
        result = input_df
        report = input_df

        for column_to_check in columns_to_check:
            # schedule a parallel check with reporting
            validator, validator_name = self.fields_validator.get_field_validator(
                column_to_check
            )
            validation_flag_column = f"etl_validate_{validator_name}_{column_to_check}"
            validation_flag_columns.append(validation_flag_column)
            error_msg_column = f"{validation_flag_column}_error"
            error_msg = f"invalid {column_to_check}: {validator_name}"

            validated_series = validator(input_df[column_to_check], **kwargs)
            output_df = validated_series.to_frame(validation_flag_column)

            # result filtering depends on the policy
            action_on_error = self.policy.action_on_invalid_field(column_to_check)

            # this is what pandas expects, so pylint: disable=singleton-comparison
            if action_on_error == policy.ActionOnIssue.IGNORE:
                # ignored errors are pushed forward
                error_msg = f"{error_msg} (error ignored)"
            else:
                output_df = output_df.loc[output_df[validation_flag_column] == True]

            if action_on_error == policy.ActionOnIssue.SKIP:
                error_msg = f"{error_msg} (line skipped)"

            # produce error report, with an error message
            unfiltered_report = validated_series.to_frame(validation_flag_column)
            report_df = (
                unfiltered_report.loc[
                    # this is what pandas expects, so pylint: disable=singleton-comparison
                    unfiltered_report[validation_flag_column]
                    == False
                ]
                .assign(error_msg=error_msg)
                .rename(columns={"error_msg": error_msg_column})
            )

            if action_on_error == policy.ActionOnIssue.FAIL:
                # insert a transform that will raise an exception on any input line
                # and interrupt the process
                # TODO(fred): interrupting right away is probably not what we want
                report_df = report_df.apply(
                    _raise_on_invalid,
                    axis=1,
                    meta=report_df,
                    na_action="ignore",
                    args=(column_to_check,),
                )

            # merge validation results in output dataframes
            result = result.merge(
                output_df, how="left", left_index=True, right_index=True
            )
            report = report.merge(
                report_df, how="left", left_index=True, right_index=True
            )

        # all checks are concatenated horizontally, valid lines are excluded from the final report
        # only valid lines are included in the result, save when the policy is "IGNORE".
        if action_on_error in (policy.ActionOnIssue.SKIP, policy.ActionOnIssue.FAIL):
            result = result.dropna(how="any", subset=validation_flag_columns)

        report = report.dropna(how="all", subset=validation_flag_columns)

        return result, report


def _raise_on_invalid(value: Any, column_name: str) -> Any:
    """Inconditionally raises an exception whenever a value is passed"""

    raise exceptions.InvalidDataError from Exception(
        f"invalid value in column {column_name}: {value}"
    )
