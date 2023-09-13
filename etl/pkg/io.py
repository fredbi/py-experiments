"""
Module io exposes utilities to manipulate file names.

## Features
* an input file resolver (`FileResolver`) to resolve date extraction from name
* an iterator (`FileResolverIterator`) to walk over multiple files in a folder
* a context manager `file_resolver_context` to manage file moving after 
  processing is done

## Limitations
* At this moment, this module only supports files on a local file system
  (no remote resources such as cloud storage)
"""

from __future__ import annotations

__author__ = "fredbi"

import glob
import itertools
import pathlib
import re
import shutil
from contextlib import contextmanager
import dataclasses
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterator, List

from etl.pkg import exceptions
from etl.pkg.runtime import Runtime

ISO_DATE_PREFIX = r"(\d{4})(\d{2})(\d{2})"
"""The default regexp used to extract the start_date from a file name"""

BAD_FOLDER = "bad"
"""The default folder where bad files are moved"""

ARCHIVE_FOLDER = "archive"
"""The default folder where successfully integrated files are moved"""

RECYCLABLE_FOLDER = "recyclable"
"""The default folder where recyclable files are created"""

REPORT_FOLDER = "reports"
"""The default folder where report files are created"""


@dataclass
class FileResolver:
    """
    FileResolver resolves to a file on the local file system.

    It knows how to extract a start_date from the file name, unless this
    date is already provided. The defaut date pattern in the file name
    is an ISO date (yyyymmdd).

    The resolvers knows how to move the file once processing is complete:
    * move successfully integrated input files to an archive directory
    * move failed files to a "bad" directory
    * move recyclable files to a recyclable directory

    Todos(fredbi):
    * should extend to output files
    * support Azure Blob resources, not just local files

    Raises:
        DevMistake: occurs when the resolver is wrongly instantiated
    """

    file_name: str = None
    start_date: date = None
    date_pattern: str = ISO_DATE_PREFIX
    """
    date_pattern is datetime extraction pattern.
    We expect the resolved groups to be [year, month, day].
    """

    name_prefix: str = ""
    """ 
    name_prefix adds a prefix to match with the date regexp,
    this may be used to disambiguate date extraction.
    """

    bad_path: str = BAD_FOLDER
    archive_path: str = ARCHIVE_FOLDER
    recyclable_path: str = RECYCLABLE_FOLDER
    report_path: str = REPORT_FOLDER

    _date_regexp: re.Pattern = dataclasses.field(default=re.compile(ISO_DATE_PREFIX))

    def __post_init__(self):
        """
        __post_init__ checks for the validity of parameters
        injected into the constructor.

        Raises:
            DevMistakeError: a dev error caused a misconfiguration
            when instantiating the FileResolver
            ConfigError: injected a misconfigured regular expression to parse dates
        """
        if self.file_name is None:
            raise exceptions.DevMistakeError from TypeError(
                " FileResolver file name cannot be set to None"
            )

        try:
            self._date_regexp = re.compile(f"{self.name_prefix}{self.date_pattern}")
        except re.error as error:
            raise exceptions.ConfigError("invalid regular expression passed") from error

    def get_file_name(self) -> str:
        """
        get_file_name yields the file name.

        Returns:
            str: fullyl qualified file name
        """
        return self.file_name

    def set_date_from_name(self) -> None:
        """
        set_date_from_name extracts the start_date from the file name.

        This is skipped if a start_date override is set on this FileResolver.
        Otherwise, the file_name is parsed according to the date regexp set for this resolver
        (defaut is an ISO date).
        """
        if self.start_date is not None:
            return

        base_name = pathlib.PurePath(self.file_name).name
        matched = self._date_regexp.search(base_name)

        if not matched or len(matched.groups()) < 3:
            raise exceptions.InvalidFileError(
                "could not extract start_date from file name. "
                f"Expected the file name to contain a date like {self.date_pattern}, "
                f"but got {base_name}"
            )

        year, month, day = matched.groups()[0], matched.groups()[1], matched.groups()[2]
        try:
            self.start_date = date(int(year), int(month), int(day))
        except (
            Exception
        ) as error:  # we immediately reraise after logging, so pylint: disable=broad-exception-caught
            raise exceptions.InvalidFileError(
                f"could not extract start_date from file name: {base_name}, "
                f"got date components: {year}/{month}/{day}"
            ) from Exception(error)

    def _to_archive(self) -> pathlib.Path:
        """
        _to_archive yields the new location of the file into the archive folder.

        """

        return _to_folder(self.file_name, self.archive_path)

    def _to_bad(self) -> pathlib.Path:
        """
        _to_bad yields the new location of the file into the bad folder.
        """

        return _to_folder(self.file_name, self.bad_path)

    def output_report(self) -> pathlib.Path:
        """
        output_report yields the location of the workflow report for this file
        """

        return _to_folder(self.file_name, self.report_path)

    def output_recyclable(self) -> pathlib.Path:
        """
        output_report yields the location of any recyclable file produced
        """

        return _to_folder(self.file_name, self.recyclable_path)

    def move_to_archive(self) -> pathlib.Path:
        """
        move_to_archive moves the file pointed to by this resolver to the archive folder.
        """
        return _move_file(self.file_name, self._to_archive())

    def move_to_bad(self) -> pathlib.Path:
        """
        move_to_bad moves the file pointed to by this resolver to the bad folder.
        """
        return _move_file(self.file_name, self._to_bad())


def _move_file(file_name: str, target: pathlib.Path) -> pathlib.Path:
    folder = pathlib.Path(target.parent)
    folder.mkdir(mode=0o750, parents=True, exist_ok=True)
    shutil.move(file_name, target)

    return target


def _to_folder(file_name: str, folder: str) -> pathlib.Path:
    """
    _to_folder defines a target file with an inserted timestamp.
    See formatting at
    https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    """

    present = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    pth = pathlib.PurePath(file_name)
    target = pathlib.PurePath(folder)

    if not target.is_absolute():
        return (
            pth.parent.joinpath(folder, "placeholder")
            .with_name(f"{pth.stem}_{present}")
            .with_suffix(pth.suffix)
        )

    return (
        target.joinpath(folder, "placeholder")
        .with_name(f"{pth.stem}_{present}")
        .with_suffix(pth.suffix)
    )


@contextmanager
def file_resolver_context(
    resolver: FileResolver, runtime: Runtime
) -> Iterator[FileResolver]:
    """
    file_resolver_context provides a context manager to handle a FileResolver resource.

    The context manager handles the moving of sucessfully processed or failed files
    at the end of a "with" block.

    Args:
        resolver (FileResolver): the FileResolver instance to handle

    Yields:
        Iterator[FileResolver]: an iterator over FileResolvers that matched the search criteria.
    """
    logger = runtime.get_logger().bind(module="file_resolver")

    try:
        resolver.set_date_from_name()

        yield resolver
    except (exceptions.InvalidFileError, exceptions.InvalidDataError) as error:
        logger.error("invalid input file", action="move_to_bad", err=error)

        try:
            resolver.move_to_bad()

            # bubble up the error: we want to advertise this as a failure
            raise error
        except exceptions.IOEtlError as nested_error:
            logger.error("failed to move file", action="move_to_bad", err=nested_error)

            raise nested_error
    else:
        logger.info("completed", action="move_to_archive")

        try:
            resolver.move_to_archive()
        except exceptions.IOEtlError as nested_error:
            logger.error(
                "failed to move file", action="move_to_archive", err=nested_error
            )

            raise nested_error
    finally:
        logger.info("file resolver terminated")


@dataclass
class FileResolverIterator(Iterator[FileResolver]):
    """
    FileResolverIterator provides an iterator over FileResolvers resolved from
    either an explicit list of input_files or walking the dir_name folder with
    a glob pattern.
    """

    dir_name: str = None
    recurse: bool = False
    radix_pattern: str = None
    input_files: List[str] = None
    start_date: date = None
    date_pattern: str = ISO_DATE_PREFIX
    name_prefix: str = ""

    bad_path: str = BAD_FOLDER
    archive_path: str = ARCHIVE_FOLDER
    recyclable_path: str = RECYCLABLE_FOLDER
    report_path: str = REPORT_FOLDER

    _file_iterator: Iterator = None
    _iterator: Iterator = None

    def __post_init__(self):
        if self.dir_name and not self.radix_pattern:
            raise exceptions.DevMistakeError(
                "FileResolverIterator with a dir_name should have a radix_pattern"
            )

        # no input provided
        if not self.dir_name and not self.input_files:
            raise exceptions.DevMistakeError(
                "FileResolverIterator should get an input directory, a list of files or both"
            )

        # only single files provided
        if not self.dir_name:
            self._file_iterator = self.input_files

            return

        # only folder provided
        root_dir = pathlib.Path(self.dir_name).absolute()
        search_path = root_dir.joinpath(self.radix_pattern)
        walk_dir = glob.iglob(
            pathname=str(search_path),
            # root_dir=root_dir, # python3.10
            recursive=self.recurse,
        )

        if not self.input_files:
            self._file_iterator = walk_dir

            return

        # both kinds of input provided
        self._file_iterator = itertools.chain(
            self.input_files,
            walk_dir,
        )

    def __iter__(self):
        self._iterator = map(
            lambda input_file_name: FileResolver(
                file_name=input_file_name,
                start_date=self.start_date,
                name_prefix=self.name_prefix,
                date_pattern=self.date_pattern,
                bad_path=self.bad_path,
                archive_path=self.archive_path,
                recyclable_path=self.recyclable_path,
                report_path=self.report_path,
            ),
            self._file_iterator,
        )

        return self

    def __next__(self):
        return self._iterator.__next__()
