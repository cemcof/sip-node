import configparser
import pathlib
import datetime
from collections import deque
from typing import Iterable, Iterator, Tuple


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

        self.angle_dose = None
        self.corrected_data_file = None
        self.ctf_result = None

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

class MicrographScanner:
    """
    Iterator that yields micrographs from sequence of files.

    Expects an iterable of file paths where each data file is followed
    by one or more metadata files. Only `.mdoc` metadata is considered;
    `.xml` and others are skipped. If a data file has no `.mdoc`
    metadata, a ValueError is raised.
    """
    def __init__(self, file_paths: Iterable[pathlib.Path]):
        self._queue = deque(pathlib.Path(p) for p in file_paths)

    def __iter__(self):
        return self

    def get_filebase(self, path: pathlib.Path):
        # Remove all suffixes in while loop
        while path.suffixes:
            path = path.with_suffix("")
        return path.name

    def _is_mdoc(self, f):
        return f.suffix.lower() == ".mdoc"

    def _is_meta(self, f):
        return self._is_mdoc(f) or f.suffix.lower() == ".xml"

    def __next__(self):
        if not self._queue:
            raise StopIteration

        first_file = self._queue.popleft()
        current_file_base = self.get_filebase(first_file)

        data_file = first_file if not self._is_meta(first_file) else None
        mdoc_file = first_file if self._is_mdoc(first_file) else None

        # Iterate until base name changes, capture data and meta files
        while self._queue:
            candidate = self._queue.popleft()
            candidate_file_base = self.get_filebase(candidate)

            print("CAND", candidate, candidate_file_base, self._is_meta(candidate), self._is_mdoc(candidate), current_file_base, candidate_file_base)
            if candidate_file_base != current_file_base:
                self._queue.appendleft(candidate)
                break

            if not self._is_meta(candidate) and not data_file:
                data_file = candidate
            if self._is_mdoc(candidate) and not mdoc_file:
                mdoc_file = candidate


        if mdoc_file is None:
            raise ValueError(f"No .mdoc metadata found for data file {data_file}")

        if data_file is None:
            raise ValueError(f"No data file found for metadata file {mdoc_file}")

        return data_file, mdoc_file
