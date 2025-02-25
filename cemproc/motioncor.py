import os
import pathlib


class MotionCorr2:
    def __init__(self, out_dir: pathlib.Path, voltage, apix, pre_dose, frame_dose, gpu_id, lmod: LmodEnvProvider, executable: str ='MotionCor2'):
        self.out_dir = out_dir
        self.voltage = voltage
        self.apix = apix
        self.pre_dose = pre_dose
        self.frame_dose = frame_dose
        self.gpu_id = gpu_id
        self.exec_env = lmod('motionCor2/1.4.0')
        self.exec = executable
        self.log_path = pathlib.Path('motCor.log')

    def run(self, in_micrograph: pathlib.Path):
        mic_prefix = in_micrograph.stem
        is_mrc = in_micrograph.suffix == ".mrc"

        command = (
            f"{self.exec} {'-InMrc' if is_mrc else '-InTiff'} {mic_prefix}.{'mrcs' if is_mrc else 'tif'} "
            f"-OutMrc {mic_prefix}.mrc -kV {self.voltage} -Iter 3 -Bft 150 "
            f"{'' if is_mrc else '-Gain ../gain.mrc -RotGain 3 -FlipGain 1 '} "
            f"-PixSize {self.apix} -Gpu {self.gpu_id}"
        )

        os.system(f"{command} >> motCor.log 2>&1")

        pre_dose = self.pre_dose  # Assuming pre_dose is an instance variable

        with self.log_path.open("r") as log_file:
            for line in log_file:
                line_split = line.split()
                if line_split[:2] == ["Stack", "size:"]:
                    pre_dose += self.frame_dose * int(line_split[4])
                    break

        return pre_dose