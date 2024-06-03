
import logging
import os, sys, re, json, pathlib
from io import TextIOWrapper
import subprocess
from common import StateObj, exec_state
import processing_tools
import fs_storage_engine
import experiment

class CryosparcWrapper(StateObj):
    """ Connects LIMS experiments and their processing with the cryosparc engine"""
    def __init__(self, 
                 exp_engine: experiment.ExperimentStorageEngine, 
                 config: dict): 
        self.exp_engine = exp_engine
        self.exp = exp_engine.exp
        self.config = config
        self.python_exec = config.get("python_exec", "python3")
        self.cryosparc_cli_path = config.get("cryosparc_cli_path", "cryosparc_cli.py")
        self.cm_path = config["cm_path"]

        target_loc = self.exp_engine.resolve_target_location()
        # Pokracovat tady - zadny processed target rule neni
        self.projects_dir = target_loc if target_loc else pathlib.Path(config["projects_dir"])
        self.project_path = self.projects_dir / f"cryosparc_{exp_engine.exp.secondary_id}"
        self.project_name = self.project_path.name
        self.raw_data_dir = target_loc if target_loc else self.projects_dir / f"raw_cryosparc_{exp_engine.exp.secondary_id}"

        self.email = config["email"]
        self.cluster = config["computational_cluster"]

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
        path_to_movies_relative : pathlib.Path = self.exp_engine.get_tag_target_dir("movie", "raw")
        workflow["exposure"] = {
            "file_engine_watch_path_abs" : str(self.raw_data_dir / path_to_movies_relative),
            "file_engine_filter" : f"*{movie_info[0].suffix}",
            "gainref_path" : str(self.raw_data_dir / movie_info[2]) if movie_info[2] else None # TODO - do we have to convert for cryosparc? 
        }

        # Use metadata to compute dose per stack (frame dose times number of frames)
        try:
            meta = self.exp_engine.read_metadata()
            dose = meta["DATA_fmDose"] * 8 # FROM METADATA * meta["DATA_numFrames"]
            workflow["mscope_params"]["total_dose_e_per_A2"] = dose
            self.exp_engine.logger.info(f"Computed dose per stack: {dose}")
        except Exception as e:
            self.exp_engine.logger.error(f"Error during dose computation: {e}")
            raise

        # Invoke the cryosparc engine
        self.exp_engine.logger.info(f"Creating cryosparc project at {self.project_path} with workflow: {json.dumps(workflow, indent=2)}")
        # Directory must exist or cryosparc fails
        self.project_path.mkdir(parents=True, exist_ok=True)
        stdout, stderr = self._invoke_cryosparc_cli("create", args, stdin=json.dumps(workflow))
        self.exp_engine.logger.info(f"Created cryosparc project {stdout}")
        self.exp.processing.pid = stdout.strip()
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
        exps = experiment.ExperimentsApi(self._api_session).get_experiments_by_states(
            processing_state=[
                experiment.ProcessingState.UNINITIALIZED, 
                       experiment.ProcessingState.READY, 
                       experiment.ProcessingState.RUNNING]
            )
        
        return filter(lambda e: e.processing.engine == "cryosparc" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), exps)

    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        cconf = self.module_config["cryosparc_config"]
        cw = CryosparcWrapper(exp_engine, cconf)

        exp_engine.restore_metadata({ k: v for k,v,_ in exp_engine.extract_metadata() })
        def running():
            # Fetch new data from storage -> processing project
            print(cw.raw_data_dir)

            raw_data_rules = exp_engine.data_rules.with_tags("raw")
            dw_result = exp_engine.download(cw.raw_data_dir, raw_data_rules, session_name="cs_processing")
            print(dw_result)
            # Return new data from processing project -> storage
            # exp_engine.upload(cw.project_path)

            # Check changes, kill the process?
        exec_state(cw,
            {
                experiment.ProcessingState.UNINITIALIZED: cw.create_project,
                experiment.ProcessingState.READY: cw.run_project,
                experiment.ProcessingState.RUNNING: running, # TODO - feedback?
                experiment.ProcessingState.COMPLETED: lambda: None,
                experiment.ProcessingState.DISABLED: lambda: None,
            }
        )
        
    
