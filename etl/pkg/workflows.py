"""
Module workflows provide generic ETL workflows
to build ETL jobs.

Workflows are based on dask dataframes and delayed objects.
"""

from __future__ import annotations

__author__ = "fredbi"

# from sets import Set
# from functools import cached_property
import dataclasses
from dataclasses import dataclass
from datetime import date
from typing import List

from dask import dataframe, delayed

from etl.pkg import exceptions, policy, resolvers, validators
from etl.pkg.components import checkers, historian, identifiers, interface, mappers
from etl.pkg.dataframe import DataFrame
from etl.pkg.runtime import Runtime

# from dask.distributed import Client


@dataclass(init=True, frozen=True)  # /*python.310: kw_only=True, */
class Base(interface.Executable):
    """
    Base workflow used to be composed into more advanced ETL workflows.

    Raises:
        TypeError: _description_
    """

    runtime: Runtime
    start_date: date = date.today()
    end_date: date = None

    identifier_resolver: resolvers.IdentifierResolver = dataclasses.field(
        default_factory=resolvers.IdentifierResolver
    )
    taxonomy_resolver: resolvers.TaxonomyResolver = dataclasses.field(
        default_factory=resolvers.TaxonomyResolver
    )
    schema_resolver: resolvers.SchemaResolver = dataclasses.field(
        default_factory=resolvers.SchemaResolver
    )
    fields_validator: validators.FieldsValidator = dataclasses.field(
        default_factory=validators.FieldsValidator
    )
    policy_resolver: policy.PolicyResolver = policy.PolicyResolver(
        policy=policy.strict()
    )

    _raw: DataFrame = None

    def __post_init__(self):
        """
        __post_init__ checks for the validity of the parameters
        injected into the constructor.

        Raises:
            TypeError: a dev error caused a misconfiguration
            when instantiating the ETL processor
        """
        self._raw = None

        if (
            self.start_date is None
            or self.runtime is None
            or self.identifier_resolver is None
            or self.taxonomy_resolver is None
            or self.fields_validator is None
        ):
            raise exceptions.DevMistakeError from TypeError(
                "Base constructor argument cannot be set to None"
            )

    def execute(self, **kwargs) -> DataFrame:
        pass


@dataclass(init=True, frozen=True)
class Generic(Base):
    """
    Generic provides a generic implementation
    for a typical data integration job (aka ETL processor).
    """

    # composition of unitary ETL processing components
    _schema_checker: interface.Pipable = dataclasses.field(default=interface.NopPipe)
    _content_checker: interface.Pipable = dataclasses.field(default=interface.NopPipe)
    _identifier: interface.Pipable = dataclasses.field(default=interface.NopPipe)
    _taxonomy_mapper: interface.Pipable = dataclasses.field(default=interface.NopPipe)
    _entity_mapper: interface.Pipable = dataclasses.field(default=interface.NopPipe)
    _history_builder: interface.Pipable = dataclasses.field(default=interface.NopPipe)

    def __post_init__(self):
        super().__post_init__()

        self._schema_checker = checkers.SchemaChecker(
            schema_resolver=super.schema_resolver
        )
        self._content_checker = checkers.ContentChecker(
            fields_validator=super.fields_validator
        )
        self._identifier = identifiers.Identifier()
        self._taxonomy_mapper = mappers.TaxonomyMapper()
        self._entity_mapper = mappers.EntityMapper()
        self._history_builder = historian.HistoryBuilder()

    def read_csv(self, urlpath: str, **kwargs) -> DataFrame:
        """
        read_csv binds the input CSV file to a lazy dataframe.

        Args:
            urlpath (str): the file path.

        Returns:
            DataFrame: a lazy dataframe to provide as input
        """

        try:
            self._raw = dataframe.read_csv(urlpath, **kwargs)
        except Exception as error:
            raise error

        return self._raw

    def execute(self, **kwargs) -> DataFrame:
        """
        execute the generic workflow.

        Returns:
            dataframe: the concatenated report of all pipelines for this workflow
        """
        # TODO: concat all reports
        # TODO: recyclable
        # TODO: close items
        if self._raw is None:
            raise exceptions.DevMistakeError(
                "raw input cannot be left undefined:"
                "you should bind the input before planning the execution",
            )

        return self._schedule_workflow(input_df=self._raw, **kwargs).compute()

    def _schedule_workflow(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """
        schedule_workflow prepares a lazy workflow for generic data integration tasks.

        NOTE: schedule_workflow doesn't actually run the computation.

        TODO: dask visualize output diagram here

        Args:
            raw (dataframe): raw input dataframe

        Returns:
            dataframe: a (lazy) concatenated report of all processing pipelines in this workflow
        """
        # 1. Check schema report, or exception raised
        valid_schema, report_schema = self._schema_checker.pipe(
            input_df=input_df, **kwargs
        )

        # 2. Perform surface validations on all columns configured for validation (e.g. dates, ...)
        valid_on_surface, report_validation = self._schema_checker.pipe(
            input_df=input_df, 
        )

        # 3. Perform column transforms on all columns configured for transformation (e.g. dates, case-sensitive strings, ...)
        # TODO: transformed, report_transformed = self._transformer.pipe(input_df=valid_on_surface,)

        # FRED: add dates columns
        identified, report_identified = self._identifier.pipe(
            input_df=valid_on_surface,  # [[self.identifier_resolver.keys()]]
            **kwargs,
        )  # -> df(ext,id)

        # FRED: use caching here
        mapped_taxonomies, report_taxonomies = self._taxonomy_mapper.pipe(
            valid_on_surface,
            **kwargs,  # [self.taxonomy_resolver.keys()]
        )  # -> [df]

        # checked_taxonomies = self.check_taxonomies(
        #    resolved_taxonomies
        # )  # applies policy
        mapped_remainder, report_mapped = self._entity_mapper.pipe(
            input_df=valid_on_surface,
            **kwargs,
        )  # map

        with_taxonomies: List[DataFrame] = []

        # for checked_taxonomy in checked_taxonomies:
        #    # mmmh: correct pattern for dask??
        #    with_taxonomies = identified.join(
        #        with_taxonomies, on=self.taxonomy_resolver.keys()[i]
        #    )

        # with_identifier = with_taxonomies.join(
        #    identified, on=self.identifier_resolver.keys()
        # )
        # with_everything = with_identifier.join(
        #    mapped_remainder,
        #    on=self.identifier_resolver.keys(),
        # )

        mapped_target_entities = []
        # mapped_target_entities = self.map_target_entities(with_everything)

        reports: List[DataFrame] = []
        for mapped_target_entity in mapped_target_entities:
            reports.append(self._history_builder.pipe(mapped_target_entity))

        return dataframe.concat(reports)

    def did_produce_recyclable(self) -> bool:
        """_summary_

        Returns:
            bool: TODO
        """
        return True
