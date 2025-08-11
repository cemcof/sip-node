import pathlib
import subprocess
import typing

from common import LmodEnvProvider, IEnvironmentSetup


class Imod:
    def __init__(self, env_setup: IEnvironmentSetup):
        self.env_setup = env_setup
        self.exec_env = self.env_setup()

    def newstack(self, frames: typing.List[pathlib.Path], out_file: pathlib.Path, mode=2):
        frame_paths = " ".join(map(str, frames))
        command = f"newstack -mode {mode} {frame_paths} {out_file}"
        result = subprocess.run(command, capture_output=True, text=True, env=self.exec_env, shell=True)
        if result.returncode != 0:
            raise RuntimeError(f"Newstack error {result.returncode} \n ERR: {result.stderr} \n OUT: {result.stdout}")


"""
Reconstruction: 

def imod_processing(inStk, angle_file, volZ):
    com = 'tilt -THICKNESS %d -TILTFILE %s -inp %s -out volume_%s' % (volZ, angle_file, inStk, inStk)
    print('Running reconstruction using Imod')
    print(com)
    os.system(com)
"""