import os
import pathlib
import re
import subprocess

from common import LmodEnvProvider

class MotionCorr3:
    def __init__(self, out_dir: pathlib.Path, voltage, apix, pre_dose, frame_dose, gpu_id, lmod: LmodEnvProvider, gain_file: pathlib.Path=None, executable: str ='MotionCor3'):
        self.voltage = voltage
        self.apix = apix
        self.pre_dose = pre_dose
        self.frame_dose = frame_dose
        self.gpu_id = gpu_id
        self.gain_file = gain_file
        self.exec_env = lmod()
        self.exec = executable

    def _get_gain_command_part(self, in_micrograph: pathlib.Path):
        return f"-Gain {self.gain_file} -RotGain 3 -FlipGain 1" if self.gain_file and in_micrograph.suffix != ".mrcs" else ""

    def _build_command(self, in_micrograph: pathlib.Path, out_micrograph: pathlib.Path):
        tmap = {
            ".mrcs": f"-InMrc {in_micrograph}",
            ".tiff": f"-InTiff {in_micrograph}",
            ".tif": f"-InTiff {in_micrograph}",
            ".eer": f"-InEer {in_micrograph}"
        }

        input_part = tmap.get(in_micrograph.suffix, None)
        if not input_part:
            raise ValueError(f"Unsupported motioncor input file type: {in_micrograph}")

        return f"{self.exec} {input_part} -OutMrc {out_micrograph} -kV {self.voltage} -Iter 3 -Bft 150 {self._get_gain_command_part(in_micrograph)} -PixSize {self.apix} -Gpu {self.gpu_id}"

    def run(self, in_micrograph: pathlib.Path, out_micrograph: pathlib.Path, skip_if_results_exist=True):
        # Output must be mrc
        if out_micrograph.suffix != ".mrc":
            raise ValueError("Output file of motioncorr must be mrc")

        # Ensure dir exists
        out_micrograph.parent.mkdir(parents=True, exist_ok=True)

        command = self._build_command(in_micrograph, out_micrograph)

        if skip_if_results_exist and out_micrograph.exists():
            return out_micrograph, self.pre_dose

        result = subprocess.run(command, capture_output=True, text=True,
                                env=self.exec_env, shell=True)
        if result.returncode != 0:
            raise RuntimeError(f"MotionCor3 failed: {result.stderr}")

        pre_dose = self.pre_dose  # Assuming pre_dose is an instance variable

        match = re.search(r"Stack size:.*?(\d+)", result.stdout)
        if match:
            pre_dose += self.frame_dose * int(match.group(1))

        return out_micrograph, pre_dose