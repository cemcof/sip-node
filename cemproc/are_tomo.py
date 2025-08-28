import pathlib
import subprocess

from common import LmodEnvProvider

class AreTomoResult:
    def __init__(self,
                 in_mrc: pathlib.Path,
                 out_mrc: pathlib.Path):
        in_stem = in_mrc.stem
        out_stem = out_mrc.stem
        out_dir = out_mrc.parent

        self.volume = out_dir / f"{out_stem}.mrc"
        self.align = out_dir / f"{in_stem}.aln"
        self.imod_dir = out_dir / f"{out_stem}_Imod"
        self.projXY = out_dir / f"{out_stem}_projXY.mrc"
        self.projXZ = out_dir / f"{out_stem}_projXZ.mrc"

    def move(self, target: pathlib.Path):
        # Move result files + imod dir
        self.volume.replace(target / self.volume.name)
        self.align.replace(target / self.align.name)
        # We dont need following for now
        # self.imod_dir.replace(target / self.imod_dir.name)
        # self.projXY.replace(target / self.projXY.name)
        # self.projXZ.replace(target / self.projXZ.name)


class AreTomo:
    def __init__(self, out_dir: pathlib.Path, lmod: LmodEnvProvider, apix, binning, tilt_axis, thickness, voltage):
        self.out_dir = out_dir
        self.lmod = lmod
        self.apix = apix
        self.binning = binning
        self.tilt_axis = tilt_axis
        self.thickness = thickness
        self.voltage = voltage
        self.exec_env = lmod()
        self.executable = "AreTomo"


    def run(self, in_mrc: pathlib.Path, out_mrc: pathlib.Path, tlt_file: pathlib.Path, skip_if_results_exist=True):
        com = (f'{self.executable} -InMrc {in_mrc} -OutMrc {out_mrc} -AngFile {tlt_file} -Kv {self.voltage} -PixSize {self.apix:.3f} '
               f'-OutBin {self.binning} -TiltAxis {self.tilt_axis} -1 -VolZ {self.thickness} -OutImod 1 -TiltCor 1')

        if not ( skip_if_results_exist and out_mrc.exists() and out_mrc.stat().st_size > 0 ):
            result = subprocess.run(com, shell=True, capture_output=True, text=True,
                                    env=self.exec_env)

            if result.returncode != 0:
                raise RuntimeError(f"Failed are_tomo {result.returncode} \n IN {com} \n ERR: {result.stderr} \n OUT: {result.stdout}")
            return AreTomoResult(in_mrc, out_mrc)
