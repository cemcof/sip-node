import pathlib
import subprocess
import typing

from common import LmodEnvProvider


class Imod:
    def __init__(self, lmod: LmodEnvProvider):
        self.lmod = lmod
        self.exec_env = self.lmod()

    def newstack(self, frames: typing.List[pathlib.Path], out_file: pathlib.Path, mode=2):
        frame_paths = " ".join(map(str, frames))
        command = f"newstack -mode {mode} {frame_paths} {out_file}"
        result = subprocess.run(command, capture_output=True, text=True, env=self.exec_env, shell=True)
        if result.returncode != 0:
            raise RuntimeError(f"Newstack error {result.returncode} \n ERR: {result.stderr} \n OUT: {result.stdout}")

