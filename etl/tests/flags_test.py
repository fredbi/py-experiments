"""
Tests for module etl.pkg.flags     
"""

from __future__ import annotations

__author__ = "fredbi"


import datetime
import tempfile

# import logging
import unittest

from etl.pkg import exceptions, flags
from etl.tests.utils_test import TEST_DATE, create_test_file

# LOGGER = logging.getLogger(__name__) # for debug


class TestArgumentParser(unittest.TestCase):
    """
    TestArgumentParser exercises the flags.ArgumentParser class.
    """

    def parser(self) -> flags.ArgumentParser:
        """parser instantiates a fresh CLI args parser"""

        return flags.ArgumentParser(
            description="a test CLI",
            # exit_on_error=False, # python3.9
            prog="test",
        )

    def test_happy_path(self):
        """Should properly parse and validate args"""

        namespace = self.parser().parse_args(
            [
                "--input-dir",
                ".",
                "--recurse",
                "--input-filename-pattern",
                "^*_test.py",
                "--log-level",
                "info",
                "--start-date",
                "2023-09-04",
            ]
        )
        self.assertIsNotNone(namespace)
        self.assertEqual(namespace.input_dir, ".")
        self.assertEqual(namespace.recurse, True)
        self.assertEqual(namespace.input_filename_pattern, "^*_test.py")
        self.assertEqual(namespace.log_level, "info")
        self.assertEqual(namespace.start_date, datetime.date(2023, 9, 4))

    def test_happy_with_files(self):
        """Should properly parse and validate args with explicit files"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date, "flags_fixture")
            namespace = self.parser().parse_args(
                [
                    "--input-dir",
                    ".",
                    "--recurse",
                    "--input-filename-pattern",
                    "^*_test.py",
                    "--log-level",
                    "info",
                    "--start-date",
                    "2023-09-04",
                    test_file,
                ]
            )
            self.assertIsNotNone(namespace)
            self.assertEqual(namespace.input_dir, ".")
            self.assertEqual(namespace.recurse, True)
            self.assertEqual(namespace.input_filename_pattern, "^*_test.py")
            self.assertEqual(namespace.log_level, "info")
            self.assertEqual(namespace.start_date, datetime.date(2023, 9, 4))
            self.assertListEqual(namespace.input_files, [test_file])

    def test_invalid_args(self):
        """Should exit on invalid argument"""

        with self.assertRaises(SystemExit):
            self.parser().parse_args(["--xyz"])

    def test_invalid_start_date(self):
        """Should reject invalid date"""

        with self.assertRaises(exceptions.FlagsError):
            self.parser().parse_args(
                [
                    "--input-dir",
                    ".",
                    "--start-date",
                    "2023-09-31",
                ]
            )

    def test_valid_end_date(self):
        """Should get valid end date"""

        namespace = self.parser().parse_args(
            [
                "--input-dir",
                ".",
                "--start-date",
                "2023-09-04",
                "--end-date",
                "2023-10-01",
            ]
        )
        self.assertEqual(namespace.end_date, datetime.date(2023, 10, 1))

    def test_valid_standalone_end_date(self):
        """Should get valid standaone_end date"""

        namespace = self.parser().parse_args(
            [
                "--input-dir",
                ".",
                "--end-date",
                "2023-10-01",
            ]
        )
        self.assertEqual(namespace.end_date, datetime.date(2023, 10, 1))

    def test_invalid_end_date(self):
        """Should reject end_date past start_date"""

        with self.assertRaises(exceptions.FlagsError) as ctx:
            self.parser().parse_args(
                [
                    "--input-dir",
                    ".",
                    "--start-date",
                    "2023-09-30",
                    "--end-date",
                    "2023-09-01",
                ]
            )

        error = repr(ctx.exception.__cause__)
        self.assertRegex(error, "must have end_date>start_date")

    def test_invalid_no_input(self):
        """Should reject with no input specified.

        NOTE: the error context (exception chaining) is lost after
        the with statement, hence can't use assertRaisesRegex.
        """

        with self.assertRaises(
            exceptions.FlagsError,
        ) as ctx:
            self.parser().parse_args(
                [
                    "--start-date",
                    "2023-09-30",
                ]
            )

        error = repr(ctx.exception.__cause__)
        self.assertRegex(error, "at least a folder or an input file")

    def test_invalid_file_not_existing(self):
        """Should reject with no existing file"""

        with self.assertRaises(
            exceptions.FlagsError,
        ) as ctx:
            self.parser().parse_args(["--start-date", "2023-09-30", "zorg.csv"])

        error = repr(ctx.exception.__cause__)
        self.assertRegex(error, "the provided input file must exist")

    def test_invalid_file_is_a_dir(self):
        """Should reject with input file as a directory"""

        with self.assertRaises(
            exceptions.FlagsError,
        ) as ctx:
            self.parser().parse_args(["--start-date", "2023-09-30", "."])

        error = repr(ctx.exception.__cause__)
        self.assertRegex(error, "the provided input file must not")

    def test_invalid_dir_not_existing(self):
        """Should reject with no existing directory"""

        with self.assertRaises(
            exceptions.FlagsError,
        ) as ctx:
            self.parser().parse_args(["--input-dir", "zorg"])

        error = repr(ctx.exception.__cause__)
        self.assertRegex(error, "the provided input folder must exist")

    def test_invalid_dir_not_folder(self):
        """Should reject with input_dir not a directory"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date, "flags_fixture")

            with self.assertRaises(
                exceptions.FlagsError,
            ) as ctx:
                self.parser().parse_args(["--input-dir", test_file])

            error = repr(ctx.exception.__cause__)
            self.assertRegex(error, "the provided input folder must be a directory")


if __name__ == "__main__":
    unittest.main()
