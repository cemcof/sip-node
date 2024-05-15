import logging
import time
import configuration
import experiment
from experiment import ExperimentWrapper
from common import multiglob
import data_tools, common
import pathlib
import shutil

class FsExperimentStorageEngine(experiment.ExperimentStorageEngine):
    def __init__(self, experiment: ExperimentWrapper, logger: logging.Logger, data_rules: configuration.DataRulesWrapper, metadata_model: dict,
                 base_path, 
                 server_base_path,
                 server: str,
                
                 metadata_target="experiment.yml",
                 operator_links_folder=None) -> None:
        super().__init__(experiment, logger, data_rules, metadata_model, metadata_target)
        self.base_path = pathlib.Path(base_path)
        self.server_base_path = pathlib.Path(server_base_path)
        self.server = server
        self.operator_links_folder = pathlib.Path(operator_links_folder) if operator_links_folder else None

    def _link_operator_directory(self):
        """If configured, create a link to the operator directory"""
        if not self.operator_links_folder:
            return None

        # Create link
        target_path = self.operator_links_folder / f"OPERATORS" / self.exp.data_model["Operator"]["Fullname"].replace(" ", "_") / self.exp.secondary_id

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
        target_path = self.server_base_path / self.resolve_target_location().relative_to(self.base_path)
        return {
            "Target": self.server,
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
        target_path = self.base_path / self.get_exp_subpath()
        return target_path / (src_relative or "")
    
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
        # self.logger.info(f"Transfered file {src_file.name} to the storage. {common.sizeof_fmt(file_size)}, {took_sec:.3f} sec")
        return took_sec, file_size
    
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
    

def fs_storage_engine_factory(exp, e_config: configuration.JobConfigWrapper, logger, module_config: configuration.LimsModuleConfigWrapper, engine: str=None):
    conf: dict = module_config.get(engine or exp.storage.engine)
    if not conf:
        return None
    print(conf)
    return FsExperimentStorageEngine(
        exp, logger, e_config.data_rules, e_config.metadata["model"],

        base_path=conf["base_path"], 
        server_base_path=conf.get("server_base_path"),
        server=conf.get("server"),
        metadata_target=e_config.metadata["target"],
        operator_links_folder=conf.get("operator_links_folder"),
    )