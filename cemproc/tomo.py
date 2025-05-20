import datetime
import logging
import math
import os
import pathlib
import re
import sys
import typing
import numpy as np

from sklearn.cluster import DBSCAN
from collections import defaultdict

import common
import configparser
from cemproc.ctf import CtfFind5
from cemproc.imod import Imod
from cemproc.motioncor import MotionCorr3
from common import LmodEnvProvider, StateObj
from data_tools import DataTransferSource, DataRule, FsTransferSource, DataRulesWrapper
from experiment import ExperimentStorageEngine


class MicrographMetadata:
    def __init__(self, name: str, dt: datetime.datetime, stage_pos, image_shift, tilt_angle):
        self.name = name
        self.stage_pos = stage_pos
        self.image_shift = image_shift
        self.tilt_angle = tilt_angle
        self.dt = dt

    def __str__(self):
        return f"{self.name:<20} {self.dt:<25} {str(self.stage_pos):<20} {str(self.image_shift):<20} {self.tilt_angle:<10.2f}"

class InvalidMicrographType(Exception):
    pass

class Micrograph:
    def __init__(self, data_file: pathlib.Path, meta_file: pathlib.Path, metadata: MicrographMetadata):
        self.data_file = data_file
        self.meta_file = meta_file
        self.metadata = metadata

        self.corrected_data_file = None
        self.ctf_pwr_file = None

    @classmethod
    def parse(cls, data_file: pathlib.Path, meta_file: pathlib.Path):
        # Mdoc is esentially .ini so we can use configparser from standard lib
        parser = configparser.ConfigParser(allow_unnamed_section=True)
        parser.read(meta_file)

        if "FrameSet = 0" not in parser:
            raise InvalidMicrographType()  # This is actually a multidimension one - thats not desired micrograph

        frame_sec = parser["FrameSet = 0"]
        dt = datetime.datetime.strptime(frame_sec.get("DateTime"), "%d-%b-%Y  %H:%M:%S")
        stage_pos_x, stage_pos_y = frame_sec['StagePosition'].split(' ')
        image_shift_x, image_shift_y = frame_sec['ImageShift'].split(' ')
        tilt_angle = frame_sec['TiltAngle']

        meta  = MicrographMetadata(
            name=data_file.name,
            dt=dt,
            stage_pos=(float(stage_pos_x), float(stage_pos_y)),
            image_shift=(float(image_shift_x), float(image_shift_y)),
            tilt_angle=float(tilt_angle),
        )
        return cls(data_file, meta_file, meta)

class TiltSeries:
    def __init__(self, series_id: int):
        self.micrographs = []
        self.series_id = series_id
        self.series_name = f"tomo{series_id}"

        self.stack_file = None
        self.stack_pw_file = None

    def add_micrograph(self, mic: Micrograph):
        self.micrographs.append(mic)

    def stack(self, out_file: pathlib.Path, out_file_pw: pathlib.Path, imod_runner: Imod):
        self.stack_file = out_file
        self.stack_pw_file = out_file_pw

        frames = [mic.corrected_data_file for mic in sorted(self.micrographs, key=lambda x: x.metadata.tilt_angle)]
        imod_runner.newstack(frames, out_file)

        frames_pw = [mic.ctf_pwr_file for mic in sorted(self.micrographs, key=lambda x: x.metadata.tilt_angle)]
        imod_runner.newstack(frames_pw, out_file_pw)

        return out_file, out_file_pw

    def has_angle(self, angle):
        for mic in self.micrographs:
            if abs(mic.metadata.tilt_angle - angle) < 0.01:
                return True
        return False

    def move_raw(self):
        for mic in self.micrographs:
            parent = mic.data_file.parent
            new_parent = parent / self.series_name
            new_parent.mkdir(exist_ok=True, parents=True)
            mic.data_file.rename(new_parent / mic.data_file.name)
            mic.meta_file.rename(new_parent / mic.meta_file.name)

    def move_results(self, target: pathlib.Path):
        target.mkdir(exist_ok=True, parents=True)
        self.stack_file.rename(target / self.stack_file.name)
        self.stack_pw_file.rename(target / self.stack_pw_file.name)

class StageSeriesAngleBased:
    def __init__(self, current_tilt_id: int, stage_pos_eps=2):
        self.tilt_series = []
        self.stage_pos_eps = stage_pos_eps
        self.current_tilt_id = current_tilt_id

    def try_add_micrograph(self, in_mic: Micrograph):

        # Check if this is new stage position
        for i_tlt in self.tilt_series:
            for i_mic in i_tlt.micrographs:
                if common.euclidean_distance(in_mic.metadata.stage_pos, i_mic.metadata.stage_pos) > self.stage_pos_eps:
                    return False

        # Find tilt series whose point is closest and still doesnt have micrograph with given angle
        tlt, dist = None, sys.maxsize
        for i_tlt in self.tilt_series:
            if not i_tlt.has_angle(in_mic.metadata.tilt_angle):
                for i_mic in i_tlt.micrographs:
                    dst = common.euclidean_distance(in_mic.metadata.image_shift, i_mic.metadata.image_shift)
                    if dst < dist:
                        tlt, dist = i_tlt, dst

        if not tlt:
            # Not found tilt series, create new one
            tlt = TiltSeries(self.current_tilt_id)
            self.current_tilt_id += 1
            self.tilt_series.append(tlt)

        tlt.add_micrograph(in_mic)
        return True

    def find_tilt_series(self):
        return self.tilt_series

class StageSeries:
    def __init__(self, current_tilt_id: int, stage_pos_eps: float = 2, image_shift_eps: float=2):
        self.micrographs = []
        self.stage_pos_eps = stage_pos_eps
        self.image_shift_eps = image_shift_eps
        self.current_tilt_id = current_tilt_id

    def try_add_micrograph(self, mic: Micrograph):
        for existing_mic in self.micrographs:
            if common.euclidean_distance(existing_mic.metadata.stage_pos, mic.metadata.stage_pos) > self.stage_pos_eps:
                return False

        self.micrographs.append(mic)
        return True

    def dump_mics_csv(self, path: pathlib.Path):
        with open(path, "w") as f:
            f.write("Name,Date,StagePosX,StagePosY,ImageShiftX,ImageShiftY,TiltAngle\n")
            for mic in self.micrographs:
                f.write(f"{mic.metadata.name},{mic.metadata.dt},{mic.metadata.stage_pos[0]},{mic.metadata.stage_pos[1]},{mic.metadata.image_shift[0]},{mic.metadata.image_shift[1]},{mic.metadata.tilt_angle}\n")

    def find_tilt_series(self):
        self.dump_mics_csv(pathlib.Path("stage_dump.csv"))
        shifts = np.array([m.metadata.image_shift for m in self.micrographs])

        db = DBSCAN(eps=self.image_shift_eps, min_samples=4, metric='euclidean').fit(
            shifts
        )

        def ts_factory():
            ts = TiltSeries(self.current_tilt_id)
            self.current_tilt_id += 1
            return ts

        tilt_sers = defaultdict(ts_factory)
        for mic, label in zip(self.micrographs, db.labels_):
            tilt_sers[label].add_micrograph(mic)

        return list(sorted(tilt_sers.values(), key=lambda x: x.series_id))

    def process_series(self):
        pass



class TomoProcessor:
    def __init__(self, working_dir: pathlib.Path, logger: logging.Logger, lmod_env_provider: LmodEnvProvider, **kwargs):
        self.working_dir = working_dir
        self.run_dir = working_dir / "_run"
        self.logger = logger

        self.processed_mics_file = self.run_dir / "processed_mics.dat"
        self.last_id_file = self.run_dir / "last_tilt_id.dat"
        self.processed_mics = set()
        self.next_tilt_id = 0

        self.current_stage = StageSeriesAngleBased(self.next_tilt_id)
        self._load_state()

        self.motion_corr_runner = MotionCorr3(out_dir=self.run_dir, lmod=LmodEnvProvider(lmod_env_provider.lmod_path, kwargs.get('motioncorr_module', 'MotionCor3/1.0.1')),
                                              voltage=kwargs['voltage'],
                                              apix=kwargs['apix'],
                                              pre_dose=kwargs['pre_dose'],
                                              frame_dose=kwargs['frame_dose'],
                                              gain_file=kwargs.get('gain_file', None),
                                              gpu_id=0)
        self.ctf_runner = CtfFind5(out_dir=self.run_dir, lmod=LmodEnvProvider(lmod_env_provider.lmod_path, kwargs.get('ctf_module', 'ctffind/5.0.2')),
                                   voltage=kwargs['voltage'],
                                   apix=kwargs['apix'],
                                   cs=kwargs['cs'],
                                   ac=kwargs['ac'],
                                   pwr_size=kwargs['pwr_size'],
                                   defocus_min=kwargs['defocus_min'],
                                   defocus_max=kwargs['defocus_max'],
                                   res_min=kwargs['res_min'],
                                   res_max=kwargs['res_max'],
                                   phase_plate=kwargs['phase_plate'],
                                   min_phase_shift=kwargs['min_phase_shift'],
                                   max_phase_shift=kwargs['max_phase_shift'])

        self.imod_runner = Imod(lmod=LmodEnvProvider(lmod_env_provider.lmod_path, kwargs.get('imod_module', 'imod')))


    def _load_state(self):
        if self.processed_mics_file.exists():
            self.processed_mics = set(self.processed_mics_file.read_text().splitlines())
        if self.last_id_file.exists():
            self.next_tilt_id = int(self.last_id_file.read_text()) + 1

    def consume_next_micrograph(self, mic: Micrograph):
        """ Expect micrographs given in sorted order - as generated by the instrument """

        # Lets run motioncorr/ctf on micrograph first
        out_mrc = self.run_dir / (mic.data_file.stem + ".mrc")
        mic.corrected_data_file, dose = self.motion_corr_runner.run(in_micrograph=mic.data_file, out_micrograph=out_mrc, skip_if_results_exist=True)

        # Lets run ctf estimation
        ctf_pwr_path = self.run_dir / (mic.data_file.stem + "_pwr.mrc")
        mic.ctf_pwr_file, mic.ctf_info_file = self.ctf_runner.run(mic.corrected_data_file, ctf_pwr_path, skip_if_results_exist=True)

        # After this is done, lets give this to group
        added = self.current_stage.try_add_micrograph(mic)
        if not added:
            tilt_sers = self.process_stage()
            self.current_stage = StageSeriesAngleBased(self.next_tilt_id)
            return tilt_sers

    def set_gainfile(self, path: pathlib.Path):
        self.motion_corr_runner.gain_file = path

    def mark_micrographs_as_processed(self, mics):
        with open(self.processed_mics_file, "a") as f:
            for mic in mics:
                self.processed_mics.add(mic.data_file.name)
                f.write(mic.data_file.name + "\n")

    def mark_tilt_id(self, tilt_id):
        self.last_id_file.write_text(str(tilt_id))

    def process_stage(self):
        tilt_sers = self.current_stage.find_tilt_series()
        for ts in tilt_sers:
            # For each titl series - newstack
            ts.stack(
                self.run_dir / f"tomo_{ts.series_id}.mrc",
                self.run_dir / f"tomo_{ts.series_id}_pw.mrc",
                self.imod_runner)

            # Tilt series done - mark id, clean, moves...
            ts.move_results(self.working_dir / ts.series_name)
            self.mark_micrographs_as_processed(ts.micrographs)
            self.mark_tilt_id(ts.series_id)

        return tilt_sers

class TomoSession:
    def __init__(self, tomo_processor: TomoProcessor, source_dir: pathlib.Path, exp_storage: ExperimentStorageEngine):
        self.tomo_processor = tomo_processor
        self.source_dir = source_dir
        self.exp_storage = exp_storage

    def run(self):
        movie_rules = self.exp_storage.data_rules.get_target_for({'raw', 'movie'})
        gain_rule = self.exp_storage.data_rules.get_target_for({'raw', 'gain'})
        fs_source = FsTransferSource(self.source_dir)
        glb_gain = fs_source.glob(DataRulesWrapper(gain_rule))
        glb_movies = fs_source.glob(DataRulesWrapper(movie_rules))

        gain_file = next(glb_gain, None)
        if gain_file:
            self.tomo_processor.set_gainfile(self.source_dir / gain_file[0])

        try:
            while True:
                print("CONSMED START")
                data = next(glb_movies)
                meta = next(glb_movies)

                try:
                    mic = Micrograph.parse(self.source_dir / data[0], self.source_dir / meta[0])
                    # We do tilt series processing only if not done on mic...
                    tilt_sers = self.tomo_processor.consume_next_micrograph(mic)
                    if tilt_sers:
                        # If we generated new tilt series, move raw movies accordingly
                        pass
                except InvalidMicrographType:
                    print(f"Skipping {data[0]}")
                    continue
                except Exception as e:
                    self.tomo_processor.logger.error(f"Failed to process {data[0]}", exc_info=e)

                print(f"CONSMED END {mic}")
        except StopIteration:
            pass  # Iterator is exhausted
