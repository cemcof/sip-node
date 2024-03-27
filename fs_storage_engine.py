import logging
import time
import configuration
import experiment
import yaml
from experiment import ExperimentWrapper
from common import multiglob
import data_tools, common
import pathlib
import shutil

class FsExperimentStorageEngine(experiment.ExperimentStorageEngine):
    def by_user_type_and_year_target_policy(self):
        base_path = pathlib.Path(self.config["BasePath"])
        data_year = self.exp.dt_created.strftime("%y")
        target_path = base_path / self.exp.user_type / f"DATA_{data_year}" / self.exp.secondary_id

        return target_path
    
    def standard_links_by_operator_target_policy(self):
        base_path = pathlib.Path(self.config["BasePath"])
        target_path = base_path / self.exp.user_type / f"OPERATORS" / self.exp.data_model["Operator"]["Fullname"].replace(" ", "_") / self.exp.secondary_id
        return target_path
    
    def _link_operator_directory(self):
        """If configured, create a link to the operator directory"""
        if not self.config["LinksByOperatorTargetPolicy"]:
            return

        # Create link
        target_path: pathlib.Path = getattr(self, self.config["LinksByOperatorTargetPolicy"])()

        # Ensure that the target directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            target_path.symlink_to(self.resolve_target_location(), target_is_directory=True)
            self.logger.info(f"Created link to operator directory: {target_path} -> {self.resolve_target_location()}")
        except FileExistsError:
            pass    

    def prepare(self):
        target_dir = self.resolve_target_location()
        target_dir.mkdir(parents=True, exist_ok=True)   
        self._link_operator_directory()

    def get_access_info(self):
        target_path = pathlib.Path(self.config["ServerBasePath"]) / self.resolve_target_location().relative_to(self.config["BasePath"])
        return {
            "Target": self.config["Server"],
            "Path": str(target_path),
            "Token": None
        }
        

    def is_accessible(self):
        try:
            target_location = self.resolve_target_location()
            return target_location.exists()
        except Exception as e:
            return False

    def resolve_target_location(self, src_relative: pathlib.Path = None) -> pathlib.Path:
        target_policy = getattr(self, self.config["TargetPolicy"])
        target = target_policy()
        return target if not src_relative else target / src_relative
    
    def file_exists(self, path_relative: pathlib.Path):
        return self.resolve_target_location(path_relative).exists()
    
    def read_file(self, path_relative: pathlib.Path, as_text=True):
        target = self.resolve_target_location(path_relative)
        return target.read_text() if as_text else target.read_bytes()
    
    def write_file(self, path_relative: pathlib.Path, content):
        target = self.resolve_target_location(path_relative)
        # Ensure target directory for the file exists
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content) if isinstance(content, str) else target.write_bytes(content)
    
    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path, skip_if_exists=True):
        if skip_if_exists and self.file_exists(path_relative):
            return False
        target = self.resolve_target_location(path_relative)
        # Ensure target directory for the file exists
        target.parent.mkdir(parents=True, exist_ok=True)
        timestart = time.time()
        shutil.copyfile(src_file, target)
        took_sec = time.time() - timestart
        file_size = target.stat().st_size
        self.logger.info(f"Transfered file {src_file.name} to the storage. {common.sizeof_fmt(file_size)}, {took_sec:.3f} sec")
        return True
    
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        target = self.resolve_target_location(path_relative_src)
        shutil.copyfile(target, path_dst)
        return True
    
    def purge(self):
        target = self.resolve_target_location()
        shutil.rmtree(target)

        
    def glob(self, patterns):
        target = self.resolve_target_location()
        for f in multiglob(target, patterns):
            yield f.relative_to(target)
    