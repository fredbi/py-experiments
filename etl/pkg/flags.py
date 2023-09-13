"""
Module flags exposes a default flags configuration to build a CLI.

The ArgumentParser class derives from the standard library's argparse.ArgumentParser,
only adding standard flags that we would like to have across the board.

In addition, there are a few validations carried out on the passed arguments.

See argparse.ArgumentParser for usage (parsing, using resulting namespace).
"""

from __future__ import annotations

__author__ = "fredbi"

import pathlib
from datetime import date
import argparse

from etl.pkg import exceptions

Namespace = argparse.Namespace


class ArgumentParser(argparse.ArgumentParser):
    """
    ArgumentParser provides default CLI flags, with validation.

    See `argparse.ArgumentParser`
    """

    def __init__(
        self,
        description: str = "ETL job",
        epilog="""
            'start_date' is not required, but becomes so if the date cannot be extracted from the file names.
            
            You need to specify at least one of 'input_folder' or 'input_files'.
            'end_date' addresses the special use case when integrated date has a predetermined lifespan.
            """,
        **kwargs,
    ):
        kwargs["description"] = description
        kwargs["epilog"] = epilog
        super().__init__(**kwargs)

        self.add_argument(
            "--input-dir",
            metavar="{path to input folder}",
            type=str,
            required=False,
            default=None,
            help="a directory where to look for input files",
        )

        self.add_argument(
            "--recurse",
            # action=argparse.BooleanOptionalAction, python3.9
            action="store_true",
            default=False,
            help="when input_dir is specified, "
            "whether to recurse into the input_dir folder to find files, no glob expression",
        )

        self.add_argument(
            "input_files",  # positional argument
            metavar="{filename[, other file...]}",
            type=str,
            action="extend",
            nargs="*",
            default=None,
            help="the full path of the input file "
            "(several files may be specified), no glob expressions",
        )

        self.add_argument(
            "--input-filename-pattern",
            metavar="{filename[, other file...]}",
            type=str,
            required=False,
            default=None,
            help="a pattern (glob expression) to filter input files "
            "when exploring the input directory. "
            "Does not apply to explicitly listed files",
        )

        self.add_argument(
            "--start-date",
            metavar="YYYY-MM-DD",
            type=iso_date,
            required=False,
            default=None,
            help="overrides the business start date "
            "(by default, extract the start date from the input file name)",
        )

        self.add_argument(
            "--end-date",
            metavar="YYYY-MM-DD",
            type=iso_date,
            required=False,
            default=None,
            help="overrides the business end date "
            "(this will integrate records with a specific end date "
            "(bound excluded), instead of '+Infinity')",
        )

        self.add_argument(
            "--report-dir",
            metavar="{folder where to output execution reports}",
            type=str,
            required=False,
            default=".",
            help="location of execution reports",
        )

        self.add_argument(
            "--log-level",
            metavar="{log level}",
            type=str,
            nargs="?",
            required=False,
            choices=["error", "warn", "info", "debug"],
            default="info",
            help="verbosity of the logs",
        )

    def _validate(self, namespace: argparse.Namespace) -> None:
        if not namespace.input_dir and len(namespace.input_files) == 0:
            raise exceptions.FlagsError from Exception(
                "at least a folder or an input file must be provided"
            )

        if namespace.input_dir:
            path = pathlib.Path(namespace.input_dir)
            if not path.exists():
                raise exceptions.FlagsError from Exception(
                    f"the provided input folder must exist: {path.name}"
                )

            if not path.is_dir():
                raise exceptions.FlagsError from Exception(
                    f"the provided input folder must be a directory: {path.name}"
                )

        if len(namespace.input_files) > 0:
            for file in namespace.input_files:
                path = pathlib.Path(file)
                if not path.exists():
                    raise exceptions.FlagsError from Exception(
                        f"the provided input file must exist: {path.name}"
                    )

                if path.is_dir():
                    raise exceptions.FlagsError from Exception(
                        f"the provided input file must not be a directory: {path.name}"
                    )

        if namespace.end_date:
            if namespace.start_date and not namespace.start_date < namespace.end_date:
                raise exceptions.FlagsError from Exception(
                    "when both dates are specified, we must have end_date>start_date"
                )

    def parse_args(
        self, args=None, namespace: argparse.Namespace = None
    ) -> argparse.Namespace:
        """
        parse_args parses the command line arguments, then validate the arguments.

        See `argparse.ArgumentParser`.

        Args:
            args (List[str], optional): arguments from the command line.
            Defaults to None.
            namespace (argparse.Namespace, optional): an input object to hold the parsed arguments.
            Defaults to None.

        Returns:
            argparse.Namespace: parsed arguments.
            If none was passed as input, as new object is created.
        """

        namespace = super().parse_args(args, namespace)
        self._validate(namespace)

        return namespace


def iso_date(arg: str) -> date:
    """iso_date validates a date input as {YYYY-MM-DD}"""
    try:
        return date.fromisoformat(arg)
    except ValueError:
        raise exceptions.FlagsError from Exception(
            "the provided date must be a valid ISO8601 date "
            f"(yyyy-mm-dd), but got: {arg}"
        )
