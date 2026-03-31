#!/usr/bin/env python3
"""
Tests for FnMatchPattern class from data_tools.py
"""

import unittest
import pathlib
from data_tools import FnMatchPattern


class TestFnMatchPattern(unittest.TestCase):
    """Test cases for FnMatchPattern class"""
    def test_patterns(self):
        patterns = [
            ("test.txt", "test.txt", True),
            ("*.txt", "document.txt", True),
            ("test?.txt", "test1.txt", True),
            ("test[0-9].txt", "test5.txt", True),
            ("**/test.txt", "dir1/test.txt", True),
            ("**/test.txt", "dir1/dir2/test.txt", True),
            ("**/test.txt", "test.txt", True),
            ("*.log", "logs/application.log", False),
            ("*.log", "system/logs/error.log", False),
            ("*.log", "debug.log", True),
            ("*.log", "logs/config.txt", False),

            ("*.*", "file.txt", True),
            ("*.*", "noextension", False),
            ("*.*", "dir/anything.txt", False),

            ("**/*.tif", "image1.tif", True),
            ("**/*.tif", "dir1/image2.tif", True),
            ("**/*.tif", "dir1/dir2/image3.tif", True),
            ("**/*.tif", "document.txt", False),

            ('Raw/**/*.*', 'Raw/data/file1.txt', True),
            ('Raw/**/*.*', 'Raw/file2.txt', False),
            ('Raw/**/*.*', 'data/file3.txt', False),

            ("**/*.*", "dir1/dir2/file.txt", True),
            ("**/*.*", "file.txt", True),
            ("**/*.*", "dir1/dir2/file", False),

            ("tomo*/*", "tomo123/file.txt", True),
            ("tomo*/*", "tomo/file.txt", True),
            ("tomo*/*", "tomo123/dir/file.txt", True),
            ("tomo*/*", "_run/mic.tif", False),

            ("**/*.yml", "config.yml", True)

        ]

        for pattern_str, path_str, expected in patterns:
            with self.subTest(pattern=pattern_str, path=path_str):
                pattern = FnMatchPattern.parse(pattern_str)
                self.assertEqual(pattern.match(pathlib.Path(path_str)), expected)

    def test_case_insensitivity(self):
        pattern = FnMatchPattern.parse("TEST.TXT")
        self.assertTrue(pattern.match(pathlib.Path("test.txt")))
        self.assertTrue(pattern.match(pathlib.Path("TEST.TXT")))
        self.assertTrue(pattern.match(pathlib.Path("Test.Txt")))

        pattern = FnMatchPattern.parse("*TEST.TXT")
        self.assertTrue(pattern.match(pathlib.Path("myTEST.TXT")))
        self.assertTrue(pattern.match(pathlib.Path("TEST.TXT")))

if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)

