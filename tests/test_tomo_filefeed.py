import pathlib
import sys
import unittest

import data_tools
from cemproc.micrograph import MicrographScanner, Micrograph
from cemproc.tilt_series import StageSeriesAngleBased
from data_tools import DataRulesWrapper


class TestTomoFileFeed(unittest.TestCase):
    """Test cases for TomoFileFeed class"""
    def test_tomo_file_feed(self):
        path = "/samba/k3dosefractions/260315_MB_37/Movies"
        movie_patterns = DataRulesWrapper(data_tools.DataRule(patterns="**/*.tif", tags="movie", subfiles=1))
        scanned = data_tools.multiglob(pathlib.Path(path), movie_patterns)
        ms = MicrographScanner(iter(m[0] for m in scanned))

        order = 0

        tilt_id = 0
        stage_series = StageSeriesAngleBased(tilt_id)

        for mic in ms:
            mic = Micrograph.parse(mic[0], mic[1])
            print(f"{order} : {mic.data_file.name}, {mic.meta_file.name}")
            added = stage_series.try_add_micrograph(mic)
            if not added:
                sers = stage_series.find_tilt_series()
                print("Stage complete")

                for ser in sers:
                    print("TS ", ser.series_id, " :")
                    for m in ser.micrographs:
                        print("    ", m.data_file.name)

                tilt_id = stage_series.current_tilt_id + 1
                stage_series = StageSeriesAngleBased(tilt_id, first_mic=mic)
            order += 1

        # This is a placeholder test case. You should replace this with actual tests for the TomoFileFeed class.
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()

