
import datetime
import logging
import os, sys, re, json, pathlib
from io import TextIOWrapper
import subprocess
from common import StateObj, exec_state
import processing_tools
import data_tools
import fs_storage_engine
import common
from experiment import ExperimentModuleBase, ExperimentStorageEngine, ExperimentsApi, JobState, ProcessingState

class CryosparcWrapper(StateObj):
    """ Connects LIMS experiments and their processing with the cryosparc engine"""
    def __init__(self, 
                 exp_engine: ExperimentStorageEngine, 
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
            dose = meta["DATA_fmDose"] * 8 # FROM METADATA * meta["DATA_imageSizeZ"]
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
        self.exp.processing.state = ProcessingState.READY        
        return True
    
    def run_project(self):
        project_id, session_id = self.exp.processing.pid.split("/")
        args = {
            "--pid": project_id,
            "--sid": session_id
        }

        self._invoke_cryosparc_cli("run", args, stdin="")
        self.exp_engine.logger.info(f"Started cryosparc project {project_id} and session {session_id}")
        self.exp.processing.state = ProcessingState.RUNNING

    def stop_project(self): 
        project_id, session_id = self.exp.processing.pid.split("/")
        args = {
            "--pid": project_id,
            "--sid": session_id
        }

        self._invoke_cryosparc_cli("stop", args, stdin="")
        self.exp_engine.logger.info(f"Stopped cryosparc project {project_id} and session {session_id}")
        self.exp.processing.state = ProcessingState.FINALIZING
    
    def get_state(self):
        return self.exp.processing.state


class CryosparcProcessingHandler(ExperimentModuleBase): 

    def provide_experiments(self):
        exps = ExperimentsApi(self._api_session).get_experiments_by_states(
            processing_state=[
                ProcessingState.UNINITIALIZED, 
                       ProcessingState.READY, 
                       ProcessingState.RUNNING,
                       ProcessingState.FINALIZING]
            )
        
        return filter(lambda e: e.processing.engine == "cryosparc" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), exps)

    def step_experiment(self, exp_engine: ExperimentStorageEngine):
        cconf = self.module_config["cryosparc_config"]
        cw = CryosparcWrapper(exp_engine, cconf)

        def _filter_relevant_upload_results(up_result: list):
            up_result = [f for f in up_result if not (".log" in f[0] or "workspaces.json" in f[0])]
            return up_result

        def running():
            # Fetch new data from storage -> processing project
            dw_result, errs = exp_engine.download_raw(cw.raw_data_dir)

            # Return new data from processing project -> storage
            up_result, errs = exp_engine.upload_processed(cw.project_path, cw.project_path.name)
            print("RUN UP", up_result)

            up_result = _filter_relevant_upload_results(up_result)

            # Check if the processing is done
            # Some log files can change even though nothing reasonable is happening, ignore them
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if up_result or not exp_engine.exp.processing.last_update:
                # Update last processing change time 
                exp_engine.exp.processing.last_update = now_utc

            # Check if we should complete the processing
            timeout_delta = common.parse_timedelta(cconf.get("processing_timeout", "00:10:00.0"))
            change_delta = now_utc - exp_engine.exp.processing.last_update
            is_still_active = exp_engine.exp.state == JobState.ACTIVE
            print("RUN Check", str(now_utc), timeout_delta.seconds, change_delta.seconds, is_still_active,  exp_engine.exp.processing.last_update)
            if not is_still_active and timeout_delta < change_delta:
                # Last change is older than configured timeout - finish
                exp_engine.exp.processing.last_update = now_utc
                exp_engine.exp.processing.state = ProcessingState.STOP_REQUESTED

        def finalizing():
            up_result, errs = exp_engine.upload_processed(cw.project_path, cw.project_path.name)
            up_result = _filter_relevant_upload_results(up_result)
            print("FIn UP", up_result)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if up_result or not exp_engine.exp.processing.last_update:
                # Update last processing change time 
                exp_engine.exp.processing.last_update = now_utc
            timeout_delta = common.parse_timedelta(cconf.get("finalizing_timeout", "00:10:00.0"))
            change_delta = now_utc - exp_engine.exp.processing.last_update
            print("FIN Check", str(now_utc), timeout_delta.seconds, change_delta.seconds,  exp_engine.exp.processing.last_update)
            if timeout_delta < change_delta:
                exp_engine.exp.processing.state = ProcessingState.COMPLETED

        exec_state(cw,
            {
                ProcessingState.UNINITIALIZED: cw.create_project,
                ProcessingState.READY: cw.run_project,
                ProcessingState.RUNNING: running,
                ProcessingState.STOP_REQUESTED: cw.stop_project,
                ProcessingState.FINALIZING: finalizing,
                ProcessingState.COMPLETED: lambda: None,
                ProcessingState.DISABLED: lambda: None,
            }
        )
        
    
