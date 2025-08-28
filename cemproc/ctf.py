import pathlib
import shutil
import subprocess

from common import LmodEnvProvider

class CtfResult:
    def __init__(self, mrc_pw: pathlib.Path, result_txt_file: pathlib.Path):
        self.mrc_pw = mrc_pw
        self.result_txt_file = result_txt_file

    def tabular_results(self):
        """
        Returns ctf results as strings - [0] = header, [1] = data
        Output file of ctf looks like this:
        # Columns: #1 micrograph number; #2 - defocus 1 [A]; #3 - defocus 2; #4 - azimuth of astigmatism; #5 - additional phase shift [radian]; #6 - cross correlation; #7 - spacing (in Angstroms) up to which CTF rings were fit successfully
        50016.20   48830.05    57.55     0.00    0.02228   10.0000
        """
        data_str = self.result_txt_file.read_text()
        # Get last two lines
        last_two_lines = data_str.splitlines()[-2:]
        # First line = tabluar header
        header = last_two_lines[0]
        # Second line = tabular data
        data = last_two_lines[1]
        return header, data


class CtfFind5:
    def __init__(self, out_dir: pathlib.Path, lmod: LmodEnvProvider, voltage, apix, cs, ac, pwr_size, defocus_min, defocus_max, res_min, res_max, phase_plate, min_phase_shift,
                 max_phase_shift,
                 executable="ctffind"):
        self.lmod = lmod
        self.voltage = voltage
        self.apix = apix
        self.cs = cs
        self.ac = ac
        self.pwr_size = pwr_size
        self.defocus_min = defocus_min
        self.defocus_max = defocus_max
        self.res_min = res_min
        self.res_max = res_max
        self.phase_plate = phase_plate
        self.min_phase_shift = min_phase_shift
        self.max_phase_shift = max_phase_shift
        self.out_dir = out_dir
        self.executable = executable
        self.exec_env = self.lmod()

    def run(self, mrc_mic: pathlib.Path, mrc_pw: pathlib.Path, skip_if_results_exist=False):
        # Prepare input file content

        # Example params sequence (output of ctffind)
        """
        Input image file name                              : /storage/brno14-ceitec/shared/cemcof/internal/DATA_25/250311_T_PARALLEL/_run/mic_00000_-8.0.mrc
        Output diagnostic image file name                  : /storage/brno14-ceitec/shared/cemcof/internal/DATA_25/250311_T_PARALLEL/_run/mic_00000_-8.0_pwr.mrc
        Pixel size                                         : 0.8336
        Acceleration voltage                               : 200
        Spherical aberration                               : 2.7
        Amplitude contrast                                 : 0.07
        Size of amplitude spectrum to compute              : 512
        Minimum resolution                                 : 50
        Maximum resolution                                 : 8
        Minimum defocus                                    : 5000
        Maximum defocus                                    : 40000
        Defocus search step                                : 500.0
        Do you know what astigmatism is present?           : no
        Slower, more exhaustive search?                    : yes
        Use a restraint on astigmatism?                    : yes
        Expected (tolerated) astigmatism                   : 800.0
        Find additional phase shift?                       : no
            ------- If yes, phase shift params
        Determine sample tilt?                             : no   -- version 5
        Determine samnple thickness?                       : no     -- version 5

        """

        # Add phase plate params if aplicable
        phase_plate_params = [f"yes", "{self.min_phase_shift}", f"{self.max_phase_shift}", "0.1"] if self.phase_plate else ['no']

        params = [
            f"{mrc_mic}",
            f"{mrc_pw}",
            f"{self.apix}",
            f"{self.voltage}",
            f"{self.cs}",
            f"{self.ac}",
            f"{self.pwr_size}",
            f"{self.res_min}",
            f"{self.res_max}",
            f"{self.defocus_min}",
            f"{self.defocus_max}",
            "500.0",
            "no",
            "yes",
            "yes",
            "800.0",
            *phase_plate_params,
            'no',
            'no',
            'no'
        ]

        command_input = "\n".join(params) + "\n"

        out_info = mrc_pw.parent / f"{mrc_pw.stem}.txt"
        if not ( skip_if_results_exist and out_info.exists() and out_info.stat().st_size > 0 and mrc_pw.exists() and mrc_pw.stat().st_size > 0 ):
            result = subprocess.run(self.executable, shell=True, input=command_input, capture_output=True, text=True, env=self.exec_env)
            if result.returncode != 0:
                raise RuntimeError(f"Failed ctffind {result.returncode} \n IN {command_input} \n ERR: {result.stderr} \n OUT: {result.stdout}")

        return CtfResult(mrc_pw, out_info)
        # Attempt to move output file
        # try:
        #     dw_file = mrc_mic.with_name(f"{mrc_mic.stem}_DW.mrc")
        #     if dw_file.exists():
        #         shutil.move(str(dw_file), str(mrc_mic))
        # except Exception as e:
        #     print(f"Warning: {e}")


# def runCtffind4(micPref, voltage, apix, cs, ac, pwrSize, defMin, defMax, resMin, resMax, phasePlate, minPhaseShift,
#                 maxPhaseShift):
#     module('load', 'ctffind4')
#     if os.path.exists('ctffind.run'):
#         os.remove('ctffind.run')
#         f = open('ctffind.run', 'w')
#     else:
#         f = open('ctffind.run', 'w')
#
#     # command - ctffind inMic outPwr apix voltage cs ac amplitudeSpectrumSize minRes maxRes minDef maxDef defStep isAnyKnownAstig slowSearch restrainAstig toleratedAstig additionalPhaseShift minPhase maxPhase phaseStep expertOptions
#
#     if (phasePlate == 0):
#         f.write(str(micPref) + '.mrc\n' + micPref + '_pwr.mrc\n' + str(apix) + '\n' + str(voltage) + '\n' + str(
#             cs) + '\n' + str(ac) + '\n' + str(pwrSize) + '\n' + str(resMin) + '\n' + str(resMax) + '\n' + str(
#             defMin) + '\n' + str(defMax) + '\n500.0\nno\nyes\nyes\n800.0\nno\nno\n')
#     else:
#         f.write(str(micPref) + '.mrc\n' + micPref + '_pwr.mrc\n' + str(apix) + '\n' + str(voltage) + '\n' + str(
#             cs) + '\n' + str(ac) + '\n' + str(pwrSize) + '\n' + str(resMin) + '\n' + str(resMax) + '\n' + str(
#             defMin) + '\n' + str(defMax) + '\n500.0\nno\nyes\nyes\n800.0\nyes\n' + str(minPhaseShift) + '\n' + str(
#             maxPhaseShift) + '\n0.1\nno\n')
#
#     f.close()
#     os.system(ctffind4Path + '< ctffind.run >> ' + 'ctffind4.log 2>&1')
#     try:
#         shutil.move(micPref + '_DW.mrc', micPref + '.mrc')
#     except:
#         pass
#     module('unload', 'ctffind4')
#
# def handleCtffindFiles(inFile, count, outFile):
#     f = open(inFile, 'r')
#     names = f.readline().split()
#     f.close()
#
#     out = open(outFile, 'w')
#h
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

