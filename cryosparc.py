
import logging
import os, sys, re, json, pathlib
from io import TextIOWrapper
import subprocess
from common import StateObj, exec_state
import processing_tools
import experiment

class CryosparcWrapper(StateObj):
    """ Connects LIMS experiments and their processing with the cryosparc engine"""
    def __init__(self, 
                 exp_engine: experiment.ExperimentStorageEngine, 
                 config: dict): 
        self.exp_engine = exp_engine
        self.exp = exp_engine.exp
        self.config = config
        self.python_exec = config.get("PythonExec", "python3")
        self.cryosparc_cli_path = config.get("CryosparcCliPath", "cryosparc_cli.py")
        self.cm_path = config["CmPath"]

        project_location = self.exp_engine.resolve_target_location()
        self.project_path = project_location or pathlib.Path( config["ProjectsDir"]) / exp_engine.exp.secondary_id
        self.projects_dir = self.project_path.parent
        self.project_name = self.project_path.name

        self.email = config["Email"]
        self.cluster = config["ComputationalCluster"]

    def _invoke_cryosparc_cli(self, subprogram: str, args_extra: dict, stdin: str):
        args = [self.python_exec, self.cryosparc_cli_path, "-e", self.email, "--cm", self.cm_path, subprogram]
        for key, value in args_extra.items():
            args.append(key)
            args.append(value)
            
        self.exp_engine.logger.info(f"Invoking cryosparc engine: {' '.join(args)}")
        try:
            pc = subprocess.run(args, text=True, input=stdin, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            print("ERRR")
            print(e.stdout, e.stderr)
            # self.exp_engine.logger.error(f"Error during cryosparc invocation: {e}")
            raise e
        
        return pc.stdout, pc.stderr
    
    def _get_processing_source_path(self):
        self.exp_engine.resolve_source_dir
        if "SourceDataRoot" in self.config and self.config["SourceDataRoot"]:
            return pathlib.Path(self.scipion_config["SourceDataRoot"]) / self.exp.secondary_id
        else:
            return self.exp_engine.resolve_target_location()
        
    def create_project(self):
        args = {
            "-p": str(self.project_path),
            "-c": self.cluster,
            "-w": "-"
        }

        # Prepare workflow JSON
        # Copy protocols 
        workflow = self.exp.processing.workflow
        workflow: dict = json.loads(json.dumps(workflow)) # Deep copy

        # Get exposure values and set them to the workflow
        em_handler = processing_tools.EmMoviesHandler(self.exp_engine)
        movie_info = em_handler.find_movie_information()
        if not movie_info: # Not ready
            return None
        path_to_movies_relative : pathlib.Path = self.exp_engine.e_config.data_rules.with_tags("movie", "raw").data_rules[0].target
        processing_source_path = self._get_processing_source_path()
        workflow["exposure"] = {
            "file_engine_watch_path_abs" : str(processing_source_path / path_to_movies_relative),
            "file_engine_filter" : f"*{movie_info[0].suffix}",
            "gainref_path" : str(processing_source_path / movie_info[2]) if movie_info[2] else None # TODO - do we have to convert for cryosparc? 
        }

        # Use metadata to compute dose per stack (frame dose times number of frames)
        try:
            meta = self.exp_engine.read_metadata()
            dose = meta["DATA_fmDose"] * meta["DATA_numFrames"]
            workflow["mscope_params"]["total_dose_e_per_A2"] = dose
            self.exp_engine.logger.info(f"Computed dose per stack: {dose}")
        except Exception as e:
            self.exp_engine.logger.error(f"Error during dose computation: {e}")

        # Invoke the cryosparc engine
        stdout, stderr = self._invoke_cryosparc_cli("create", args, stdin=json.dumps(workflow))
        self.exp_engine.logger.info(f"Created cryosparc project {stdout}")
        self.exp.processing.pid = stdout
        self.exp.processing.state = experiment.ProcessingState.READY        
        return True
    
    def run_project(self):
        project_id, session_id = self.exp.processing.pid.split("/")
        args = {
            "--pid": project_id,
            "--sid": session_id
        }

        self._invoke_cryosparc_cli("run", args, stdin="")
        self.exp_engine.logger.info(f"Started cryosparc project {project_id} and session {session_id}")
        self.exp.processing.state = experiment.ProcessingState.RUNNING
    
    def get_state(self):
        return self.exp.processing.state


class CryosparcProcessingHandler(experiment.ExperimentModuleBase): 

    def provide_experiments(self):
        active_experiments = super().provide_experiments()
        return filter(lambda e: e.processing.engine == "cryosparc" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), active_experiments)

    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        cconf = self.module_config["CyrosparcConfig"]
        cw = CryosparcWrapper(exp_engine, cconf)

        exec_state(cw,
            {
                experiment.ProcessingState.UNINITIALIZED: cw.create_project,
                experiment.ProcessingState.READY: cw.run_project,
                experiment.ProcessingState.RUNNING: lambda: None, # TODO - feedback?
                experiment.ProcessingState.COMPLETED: lambda: None,
                experiment.ProcessingState.DISABLED: lambda: None,
            }
        )
        
    
