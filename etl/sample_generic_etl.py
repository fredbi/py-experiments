#!/usr/bin/env python3

"""
Script sample_generic_ETL illustrates the scaffolding of a data integration workflow
using the helper components provided by the modules exposed by etl.pkg.
"""

from __future__ import annotations

__author__ = "fredbi"

from datetime import date

from etl.pkg import (
    dataframe,
    exceptions,
    flags,
    io,
    policy,
    resolvers,
    runtime,
    validators,
    workflows,
)

# input configuration:
# * we need to specify which column(s) act as identifiers
# * we need to specifiy which columns are subject to taxonomy mappings
# * we need to specify the CSV schema

FIELDS_WITH_EXTERNAL_IDENTIFIERS = resolvers.IdentifierResolver(
    {"security_id": "isin"},
)
"""List of columns used as external identifiers"""

FIELDS_WITH_TAXONOMIES = resolvers.TaxonomyResolver(
    {
        "taxo1": "TAXO.MNEMO1",
        "taxo2": "TAXO.MNEMO2",
    }
)
"""List of columns subject to taxonomy mappings"""

FIELDS_WITH_VALIDATION = validators.FieldsValidator()
"""List of columns subject to validation"""

CSV_SCHEMA = {"id", "taxo1", "payload1", "taxo2", "payload2"}
"""Definition of the input schema"""

def execute(args_namespace: flags.Namespace) -> None:
    """
    execute the ETL integration workflow.

    * binds an input CSV to a lazy dataframe, then
    * executes the generic ETL processing workflow to ingest this input

    ## TODOs
    * [ ] recyclable interface pb here, recycled output is not
    * [ ] know yet to the file resolver
    * [ ] schema as dtype
    * [ ] taxonomies config should do mappings as well
    * [ ] name_prefix from flags
    * [ ] map flag overrides to config
    """
    start_date: date = args_namespace.start_date  # start_date override, if any
    end_date: date = args_namespace.end_date  # end_date override, if any

    #  injected_dependencies provides with all the resources
    # needed at runtime (config, logger, databases).
    #
    # The RuntimeContext takes care of relinquishing resources after
    # the job is done (or has failed).
    injected_dependencies = runtime.Runtime(
        resources=None,
    )

    # If needed, config override from CLI flags, e.g. log level etc.
    # injected_dependencies.get_config().setenv(
    #   vars(args_namespace),
    # )

    with runtime.context(injected_dependencies) as injected_runtime:
        file_iterator = io.FileResolverIterator(
            # Folder to be searched
            dir_name=args_namespace.input_dir,
            # Pattern of the file, e.g. input_*.csv
            radix_pattern=args_namespace.input_filename_pattern,
            # Search for subdirectories?
            recurse=args_namespace.recurse,
            # Add individual files
            input_files=args_namespace.input_file_name,
            # Specifies the start_date override, if any
            start_date=start_date,
            report_path=args_namespace.report_dir,
        )

        # policy_resolver knows how to deal with data issues
        policy_resolver = policy.PolicyResolver(
            policy.from_config(injected_runtime.get_config()),
        )

        # Now iterate over the resolved input files.
        #
        # Note that we don't stream over multiple files, but integrate them
        # one by one sequentially.

        for file in file_iterator:
            # the file context takes care of post-processing input files,
            # such as moving them to "bad" or "archive".
            with io.file_resolver_context(
                resolver=file, runtime=runtime
            ) as file_resolver:
                logger = injected_runtime.get_logger().bind(
                    module="etl",
                    input_file=file_resolver.get_file_name(),
                )

                etl = workflows.Generic(
                    runtime=injected_runtime,
                    # Dates, as provided with the file being processed
                    start_date=file_resolver.get_start_date(),
                    end_date=end_date,
                    # resolvers
                    identifier_resolver=FIELDS_WITH_EXTERNAL_IDENTIFIERS,
                    taxonomy_resolver=FIELDS_WITH_TAXONOMIES,
                    # extra validators
                    fields_validator=FIELDS_WITH_VALIDATION,
                    # issue handling policy
                    policy_resolver=policy_resolver,
                )

                report: dataframe.DataFrame

                try:
                    logger.info("bind a single CSV input to the integration pipeline")
                    etl.read_csv(file_resolver.get_file_name())

                    logger.info(
                        "executing the generic integration workflow on a single input file"
                    )
                    report = etl.execute()
                except (
                    exceptions.InvalidFileError,
                    exceptions.InvalidDataError,
                ) as error:
                    logger.error(
                        "invalid input data detected while processing input",
                        action="etl",
                        err=error,
                    )

                    # leave it to the file_resolver to handle this
                    raise error
                else:
                    logger.info("integration completed, pending housekeeping actions")
                    report.to_csv(file_resolver.output_report())


def main() -> None:
    """
    main entry point.

    Parses and validates CLI flags, then executes the job with this parameterization.
    """

    parser = flags.ArgumentParser(
        description="a generic data integration ETL job provided as a template",
    )

    # namespace is a dict-like object created by CLI args parsing.
    namespace = parser.parse_args()
    execute(namespace)


if __name__ == "__main__":
    main()
