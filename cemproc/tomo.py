import datetime
import os
import pathlib
import re

from common import LmodEnvProvider
from data_tools import DataTransferSource, DataRule





class MicrographMetadata:
    def __init__(self, name: str, dt: datetime.datetime, stage_pos, image_shift, tilt_angle):
        self.name = name
        self.stage_pos = stage_pos
        self.image_shift = image_shift
        self.tilt_angle = tilt_angle
        self.dt = dt

    def __str__(self):
        return f"{self.name}  {self.dt}  {self.stage_pos}  {self.image_shift}  {self.tilt_angle}"

class Micrograph:
    def __init__(self, data_file: pathlib.Path, meta_file: pathlib.Path):
        self.data_file = data_file
        self.meta_file = meta_file
        self.metadata = self._extract_mdoc()

    def _extract_mdoc(self):
        content = self.meta_file.read_text()

        datetime_pattern = re.search(r'DateTime = (\d{2}-[A-Za-z]{3}-\d{4}  \d{2}:\d{2}:\d{2})', content).group(1)
        stage_position_pattern = re.search(r'StagePosition = ([\d\-.]+) ([\d\-.]+)', content)
        image_shift_pattern = re.search(r'ImageShift = ([\d\-.]+) ([\d\-.]+)', content)
        tilt_angle_pattern = re.search(r'TiltAngle = ([\d\-.]+)', content).group(1)

        return MicrographMetadata(
            name=self.data_file.name,
            dt=datetime.datetime.strptime(datetime_pattern, "%d-%b-%Y  %H:%M:%S"),
            stage_pos=(float(stage_position_pattern.group(1)), float(stage_position_pattern.group(2))),
            image_shift=(float(image_shift_pattern.group(1)), float(image_shift_pattern.group(2))),
            tilt_angle=float(tilt_angle_pattern),
        )




class TiltSeries:
    def __init__(self):
        self.micrographs = []

    def add_micrograph(self, mic: Micrograph):
        pass

    def stack(self):
        pass


class TomoSession():
    def __init__(self, session_id: str, source_dir: pathlib.Path, working_dir: pathlib.Path):
        self.session_id = session_id
        self.source_dir = source_dir
        self.working_dir = working_dir

    def run(self):

        # motion_corr = MotionCorr2(self.working_dir, 300, 0.16, 100, 100, 0, LmodEnvProvider())
        for f in self.source_dir.iterdir():
            mic = Micrograph
            print(f)
        pass


if __name__ == "__main__":
    testsess = TomoSession("test", pathlib.Path("/storage/brno14-ceitec/shared/cemcof/internal/projects/brain_tissue/250203_Nedozralova_tau_brain_tomo_43151c9e/Raw/Movies"),
                    pathlib.Path("/storage/brno14-ceitec/home/emcf/scratch/GPUA/tomotest")).run()

    pass