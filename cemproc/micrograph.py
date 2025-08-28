import configparser
import pathlib
import datetime

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
