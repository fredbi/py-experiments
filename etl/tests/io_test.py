"""
Tests for module etl.pkg.io     
"""

__author__ = "fredbi"


import datetime
import glob
# import logging
import pathlib
import tempfile
import unittest

from etl.pkg import exceptions, io, runtime
from etl.tests.utils_test import FIXTURE_PREFIX, TEST_DATE, create_test_file

# LOGGER = logging.getLogger(__name__)  # for debug

class TestFileResolver(unittest.TestCase):
    """
    TestFileResolver exercises the io.FileResolver class.
    """

    def test_happy_file_resolver_archive(self):
        """
        Happy path for file resolver, with start date in file name,
        then moved to archive.
        """

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file)
            self.assertEqual(file_resolver.get_file_name(), test_file)

            file_resolver.set_date_from_name()
            self.assertEqual(file_resolver.start_date, datetime.date(2023, 9, 5))

            archived = file_resolver.move_to_archive()
            self.assertTrue(pathlib.Path(archived).exists())
            self.assertEqual(archived.parent, pathlib.Path(temp_dir, "archive"))

    def test_happy_file_resolver_bad(self):
        """
        Happy path for file resolver, with start date in file name,
        then moved to bad
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file)
            self.assertEqual(file_resolver.get_file_name(), test_file)

            bad = file_resolver.move_to_bad()
            self.assertTrue(pathlib.Path(bad).exists())
            self.assertEqual(bad.parent, pathlib.Path(temp_dir, "bad"))

    def test_ouput_report(self):
        """
        Happy path for file resolver, with start date in file name,
        check output_report
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file)
            self.assertEqual(file_resolver.get_file_name(), test_file)

            report = file_resolver.output_report()
            self.assertEqual(report.parent, pathlib.Path(temp_dir, "reports"))

    def test_ouput_recyclable(self):
        """
        Happy path for file resolver, with start date in file name,
        check output_recyclable
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file)
            self.assertEqual(file_resolver.get_file_name(), test_file)

            report = file_resolver.output_recyclable()
            self.assertEqual(report.parent, pathlib.Path(temp_dir, "recyclable"))

    def test_override_start_date(self):
        """
        Happy path for file resolver, with start date in file name,
        check that the provided start_date acts as an override
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(
                file_name=test_file, start_date=datetime.date(2023, 10, 8)
            )
            self.assertEqual(file_resolver.get_file_name(), test_file)

            file_resolver.set_date_from_name()
            self.assertEqual(file_resolver.start_date, datetime.date(2023, 10, 8))

    def test_guard_constructor(self):
        """Guard constructor on invalid arguments"""

        with self.assertRaises(exceptions.DevMistakeError):
            io.FileResolver(None)

    def test_date_extraction_no_date(self):
        """Exceptions on invalid date patterns"""

        file_resolver = io.FileResolver("test")

        with self.assertRaises(exceptions.InvalidFileError):
            file_resolver.set_date_from_name()

    def test_date_extraction_invalid_date(self):
        """Exceptions on invalid date"""

        file_resolver = io.FileResolver("test_20150631.txt")

        with self.assertRaises(exceptions.InvalidFileError):
            file_resolver.set_date_from_name()

    def test_date_extraction_many_dates(self):
        """Should work on pattern with date plus timestamp"""

        file_resolver = io.FileResolver("test_20150601_20230910231245.csv")
        file_resolver.set_date_from_name()
        self.assertEqual(file_resolver.start_date, datetime.date(2015, 6, 1))


class TestFileResolverIterator(unittest.TestCase):
    """
    TestFileResolverIterator exercises the io.FileResolverIterator class.
    """

    def test_iterator_from_dir(self):
        """Iterate over a collection of FileResolvers (no predetermined order)"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file_1 = create_test_file(temp_dir, test_date)
            test_file_2 = create_test_file(temp_dir, test_date)
            test_file_3 = create_test_file(temp_dir, test_date, prefix="unmatched")

            with tempfile.TemporaryDirectory() as temp_dir_2:
                test_file_4 = create_test_file(temp_dir_2, test_date)
                test_file_5 = create_test_file(temp_dir_2, test_date)
                test_file_6 = create_test_file(temp_dir_2, test_date, prefix="explicit")

                iterator = io.FileResolverIterator(
                    dir_name=temp_dir,
                    radix_pattern=f"{FIXTURE_PREFIX}*.csv",
                    input_files=[test_file_4, test_file_5, test_file_6],
                    name_prefix=f"{FIXTURE_PREFIX}_",
                )

                resolved = []
                for resolver in iterator:
                    resolved.append(resolver.get_file_name())

                self.assertEqual(len(resolved), 5)
                self.assertNotIn(test_file_3, resolved)
                for expected_file in [
                    test_file_1,
                    test_file_2,
                    test_file_4,
                    test_file_5,
                    test_file_6,
                ]:
                    self.assertIn(expected_file, resolved)


class TestFileResolverContext(unittest.TestCase):
    """
    TestFileResolverContext exercises the context manager for the io.FileResolver class.
    """

    def test_file_resolver_context_success(self):
        """File should be moved to some predefined archive upon success"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file, archive_path="success")
            with io.file_resolver_context(
                file_resolver, runtime.Runtime(resources=None)
            ):
                # DO SOMETHING
                pass

            # file should be archived
            self.assertFalse(pathlib.Path(file_resolver.get_file_name()).exists())
            find = glob.glob(
                str(pathlib.Path(temp_dir, "success", f"{FIXTURE_PREFIX}*.csv"))
            )
            found = list(find)
            self.assertEqual(len(found), 1)

    def test_file_resolver_context_failure(self):
        """File should be moved to some predefined sink upon error"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file, bad_path="failed")

            try:
                with io.file_resolver_context(
                    file_resolver, runtime.Runtime(resources=None)
                ):
                    # FAIL SOMEHOW, on specific (controlled) exception
                    raise exceptions.InvalidFileError
            except:  # that's ok here to mute exceptions, hence pylint: disable=bare-except
                pass

            # file should be moved to "failed"
            self.assertFalse(pathlib.Path(file_resolver.get_file_name()).exists())
            find = glob.glob(
                str(pathlib.Path(temp_dir, "failed", f"{FIXTURE_PREFIX}*.csv"))
            )
            found = list(find)
            self.assertEqual(len(found), 1)

    def test_file_resolver_context_other_failure(self):
        """File should NOT be moved on some uncontrolled error"""

        with tempfile.TemporaryDirectory() as temp_dir:
            test_date = TEST_DATE
            test_file = create_test_file(temp_dir, test_date)

            file_resolver = io.FileResolver(file_name=test_file, bad_path="failed")

            try:
                with io.file_resolver_context(
                    file_resolver, runtime.Runtime(resources=None)
                ):
                    # FAIL SOMEHOW, on uncontrolled exception (e.g. bug, technical outage)
                    raise TypeError
            except:  # that's ok here to mute exceptions, hence pylint: disable=bare-except
                pass

            # file should NOT be moved
            self.assertTrue(pathlib.Path(file_resolver.get_file_name()).exists())
