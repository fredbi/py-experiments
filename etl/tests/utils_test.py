"""Common utilities for tests"""

from __future__ import annotations

__author__ = "fredbi"

import os
import pathlib
import tempfile

FIXTURE_PREFIX = "test_fixture"
TEST_DATE = "20230905"


def create_test_file(
    temp_dir: str, test_date: str, prefix: str = FIXTURE_PREFIX
) -> str:
    """Create a test file in a temporary folder"""

    (
        fd,  # we close the file descriptor right away so pylint: disable=invalid-name
        test_file,
    ) = tempfile.mkstemp(suffix=".csv", prefix=f"{prefix}_{test_date}_", dir=temp_dir)
    os.close(fd)

    if temp_dir and temp_dir != ".":
        test_file = str(pathlib.Path(temp_dir, test_file))

    return test_file
