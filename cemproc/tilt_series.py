import pathlib
import sys
import typing
from collections import defaultdict

import numpy as np
from sklearn.cluster import DBSCAN

import common
from cemproc.imod import Imod
from cemproc.micrograph import Micrograph


class TiltSeries:
    def __init__(self, series_id: int):
        self.micrographs = [] # Keep sorted by tilt angle
        self.series_id = series_id
        self.series_name = f"tomo{series_id}"

        self.stack_file = None
        self.stack_pw_file = None
        self.tilt_angle_file = None
        self.ctf_results_file = None
        self.raw_movies_file = None
        self.are_tomo_result = None

    def add_micrograph(self, mic: Micrograph):
        self.micrographs.append(mic)
        self.micrographs.sort(key=lambda x: x.metadata.tilt_angle)

    def stack(self, out_file: pathlib.Path, out_file_pw: pathlib.Path, imod_runner: Imod):
        self.stack_file = out_file
        self.stack_pw_file = out_file_pw

        frames = [mic.corrected_data_file for mic in self.micrographs]
        imod_runner.newstack(frames, out_file)

        frames_pw = [mic.ctf_result.mrc_pw for mic in self.micrographs]
        imod_runner.newstack(frames_pw, out_file_pw)

        return out_file, out_file_pw

    def dump_ctf_results(self, out_file: pathlib.Path):
        with open(out_file, "w") as f:
            header, _ = self.micrographs[0].ctf_result.tabular_results()
            f.write(header)
            f.write("\n")
            for mic in self.micrographs:
                f.write(f"{mic.ctf_result.tabular_results()[1]}\n")
        self.ctf_results_file = out_file

    def dump_tilt_angles(self, out_file: pathlib.Path):
        with open(out_file, "w") as f:
            for mic in self.micrographs:
                f.write(f"{round(mic.metadata.tilt_angle,2):.2f}\n")
        self.tilt_angle_file = out_file

    def dump_raw_movies(self, out_file: pathlib.Path):
        self.raw_movies_file = out_file
        with open(out_file, "w") as f:
            for mic in self.micrographs:
                f.write(f"{mic.data_file.name}\n")

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
        self.ctf_results_file.rename(target / self.ctf_results_file.name)
        self.tilt_angle_file.rename(target / self.tilt_angle_file.name)
        self.raw_movies_file.rename(target / self.raw_movies_file.name)
        self.are_tomo_result.move(target)


class IStageSeries:
    """ Common interface for stage series implementations
        Stage series job is to collect micrographs and group them to tilt series objects """
    def try_add_micrograph(self, in_mic: Micrograph):
        raise NotImplementedError()

    def find_tilt_series(self):
        raise NotImplementedError()


class StageSeriesAngleBased(IStageSeries):
    def __init__(self, current_tilt_id: int, first_mic=None, stage_pos_eps=2):
        self.tilt_series = []
        self.stage_pos_eps = stage_pos_eps
        self.current_tilt_id = current_tilt_id
        if first_mic:
            added = self.try_add_micrograph(first_mic)
            assert added, "First micrograph should be accepted"

    def try_add_micrograph(self, in_mic: Micrograph):
        # Check if this is new stage position
        for i_tlt in self.tilt_series:
            for i_mic in i_tlt.micrographs:
                if common.euclidean_distance(in_mic.metadata.stage_pos, i_mic.metadata.stage_pos) > self.stage_pos_eps:
                    print(f"Stage too far: {common.euclidean_distance(in_mic.metadata.stage_pos, i_mic.metadata.stage_pos)} > {self.stage_pos_eps}\n"
                          f"{in_mic.data_file.name}: {in_mic.metadata} \n"
                          f"{i_mic.data_file.name}: {i_mic.metadata}")
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

    def find_tilt_series(self) -> typing.List[TiltSeries]:
        return self.tilt_series

class StageSeriesClustering(IStageSeries):
    """ Currently buggy and not operational - clusters seem not to be clustering and reasonable """
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

