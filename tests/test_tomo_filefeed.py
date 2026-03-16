import pathlib
import unittest

import data_tools
from cemproc.micrograph import MicrographScanner
from data_tools import DataRulesWrapper


class TestTomoFileFeed(unittest.TestCase):
    """Test cases for TomoFileFeed class"""
    def test_tomo_file_feed(self):
        path = "/samba/k3dosefractions/260315_MB_37/Movies"
        movie_patterns = DataRulesWrapper(data_tools.DataRule(patterns="**/*.tif", tags="movie", subfiles=True))
        scanned = data_tools.multiglob(pathlib.Path(path), movie_patterns)
        ms = MicrographScanner(iter(m[0] for m in scanned))

        for mic in ms:
            print(mic)

        # This is a placeholder test case. You should replace this with actual tests for the TomoFileFeed class.
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()

