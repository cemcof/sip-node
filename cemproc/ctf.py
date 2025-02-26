import pathlib
import shutil
import subprocess

from common import LmodEnvProvider

class CtfFind4:
    def __init__(self, out_dir: pathlib.Path, lmod: LmodEnvProvider, voltage, apix, cs, ac, pwrSize, defocus_min, defocus_max, res_min, res_max, phase_plate, min_phase_shift,
                 max_phase_shift,
                 executable="ctffind"):
        self.lmod = lmod
        self.voltage = voltage
        self.apix = apix
        self.cs = cs
        self.ac = ac
        self.pwrSize = pwrSize
        self.defocus_min = defocus_min
        self.defocus_max = defocus_max
        self.res_min = res_min
        self.res_max = res_max
        self.phase_plate = phase_plate
        self.min_phase_shift = min_phase_shift
        self.max_phase_shift = max_phase_shift
        self.out_dir = out_dir
        self.executable = executable
        self.exec_env = self.lmod("ctffind4")

    def run(self, mrc_mic: pathlib.Path, mrc_pw: pathlib.Path):

        run_file = self.out_dir / "ctffind.run"

        # Ensure previous run file is removed
        if run_file.exists():
            run_file.unlink()

        # Prepare input file content
        params = [
            f"{mrc_mic}",
            f"{mrc_pw}",
            f"{self.apix}",
            f"{self.voltage}",
            f"{self.cs}",
            f"{self.ac}",
            f"{self.pwrSize}",
            f"{self.res_min}",
            f"{self.res_max}",
            f"{self.defocus_min}",
            f"{self.defocus_max}",
            "500.0",
            "no",
            "yes",
            "yes",
            "800.0",
            "yes" if self.phase_plate else "no"
        ]

        if self.phase_plate:
            params.extend([f"{self.min_phase_shift}", f"{self.max_phase_shift}", "0.1", "no"])

        # Write to run file
        with run_file.open("w") as f:
            f.write("\n".join(params) + "\n")

        # Execute command
        log_file = self.out_dir / "ctffind4.log"
        command = f"{self.executable} < {run_file} >> {log_file} 2>&1"

        with log_file.open("a") as log:
            subprocess.run(command, shell=True, stdout=log, stderr=subprocess.STDOUT, env=self.exec_env)

        # Attempt to move output file
        try:
            dw_file = mrc_mic.with_name(f"{mrc_mic.stem}_DW.mrc")
            if dw_file.exists():
                shutil.move(str(dw_file), str(mrc_mic))
        except Exception as e:
            print(f"Warning: {e}")

#
# def handleCtffindFiles(inFile, count, outFile):
#     f = open(inFile, 'r')
#     names = f.readline().split()
#     f.close()
#
#     out = open(outFile, 'w')
#
#     cur = 0
#     f = open(names[cur][:-3] + 'txt', 'r')
#     lines = f.readlines()
#     for line in lines:
#         lineSplit = line.split()
#         if lineSplit[1] == 'Input':
#             lineSplit[3] = 'tomo' + str(count) + '.mrc'
#             lineSplit[8] = str(len(names))
#         out.write(' '.join(lineSplit) + '\n')
#     f.close()
#
#     cur = cur + 1
#     while cur < len(names):
#         f = open(names[cur][:-3] + 'txt', 'r')
#         line = f.readlines()[-1].rstrip('\n').split()
#         line[0] = str(cur + 1)
#         out.write(' '.join(line) + '\n')
#         cur = cur + 1
#         f.close()
#
#     out.close()

