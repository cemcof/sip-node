
import logging
import os, sys, re, json, pathlib
from io import TextIOWrapper
import subprocess
import argparse

import numpy as np
import processing_tools
import experiment

class CryosparcEngine:
    def __init__(self, email: str, file_out=sys.stdout, compute_configuration={}) -> None:
        self.file_out = file_out
        self.email = email
        self.compute_configuration = compute_configuration

        from cryosparc_compute import client
        self.host = os.environ['CRYOSPARC_MASTER_HOSTNAME']
        self.command_core_port = int(os.environ['CRYOSPARC_COMMAND_CORE_PORT'])
        self.command_rtp_port = int(os.environ['CRYOSPARC_COMMAND_RTP_PORT'])
        self.cli = client.CommandClient(host=self.host, port=self.command_core_port)
        self.rtp = client.CommandClient(host=self.host, port=self.command_rtp_port)
        self.user_id = self.cli.get_id_by_email(email)

    def _configure_project(self, project_id, session_id, workflow: processing_tools.WorkflowWrapper):

        # Set compute parameters
        cluster = self.compute_configuration["cluster"]
        compute_config = {
            "phase_one_lane": cluster,
            "phase_one_gpus": self.compute_configuration.get("phase_one_gpus", 1),
            "phase_two_lane": cluster,
            "phase_two_ssd": self.compute_configuration.get("phase_two_ssd", True),
            "auxiliary_lane": cluster,
            "auxiliary_ssd": self.compute_configuration.get("auxiliary_ssd", True)
        }

        for key, value in compute_config.items():
            self.rtp.update_compute_configuration(project_uid=project_id, session_uid=session_id, key=key, value=value)

        # Set exposure group parameters - data path, movie postfix and gain file, if any
        exposure_config = {
            "file_engine_watch_path_abs": workflow.find("filesPath"),
            "file_engine_filter": workflow.find("moviesSuffix"),
            "gainref_path": workflow.find("gainFile", None)
        }

        for key, value in exposure_config.items():
            if value is not None:
                self.rtp.exposure_group_update_value(project_uid=project_id, session_uid=session_id, exp_group_id=1, name=key, value=value)
        self.rtp.exposure_group_finalize_and_enable(project_uid=project_id, session_uid=session_id, exp_group_id=1)

        # Set session parameters
        apix = workflow.find("pixelSize")
        session_params = {
            "mscope_params": {
                "gainref_flip_y": "tif" in workflow.find("movieSuffix"),
                "psize_A": apix,
                "accel_kv": workflow.find("voltage"),
                "cs_mm": workflow.find("sphericalAberration"),
                "total_dose_e_per_A2": workflow.find("dosePerFrame"), # TODO - compute correct frame dose
            },
            "motion_settings": {
                "res_max_align": np.ceil(4.0*apix),
                "bfactor": 300,
            },
            "ctf_settings": {
                "res_max_align": np.floor(3.0*apix),
                "df_search_min": workflow.find("minDefocus"),
                "df_search_max": workflow.find("maxDefocus"),
            },
            "blob_pick": {
                "diameter": workflow.find("particleDiameter") * 0.8,
                "diameter_max": workflow.find("particleDiameter") * 1.2,
            },
            "extraction": {
                "box_size_pix": workflow.find("particleDiameter") * 1.5 / apix
            }
        }

        for sec, params in session_params.items():
            for key, value in params.items():
                self.rtp.set_param(project_uid=project_id, session_uid=session_id, param_sec=sec, param_name=key, value=value)


    def create_project(self, project_path: pathlib.Path, workflow):
        project_dir, project_name = project_path.parent, str(project_path.name)
        project_uid = self.cli.create_empty_project(owner_user_id=self.user_id, project_container_dir=project_dir, title=project_name)
        workflow = processing_tools.WorkflowWrapper(workflow)

        # Create a new Live session
        session_uid = self.rtp.create_new_live_workspace(project_uid=project_uid, created_by_user_id=self.user_id, title=f"{project_name} Live Session")

        # Configure project
        self._configure_project(project_uid, session_uid, workflow)
        
        # Write the project and session id to the output
        print(f"{project_uid}/{session_uid}", file=self.file_out)

    def run_project_session(self, session_id, project_id):
        self.rtp.start_session(project_uid=project_id, session_uid=session_id, user_id=self.user_id)

    def stop_project_session(self, session_id, project_id):
        self.rtp.dump_exposures(project_uid=project_id, session_uid=session_id)
        self.rtp.pause_session(project_uid=project_id, session_uid=session_id)
        self.rtp.mark_session_completed(project_uid=project_id, session_uid=session_id)

    def detach_project(self, project_id):
        self.cli.detach_project(project_uid=project_id)
        self.cli.delete_detached_project(project_uid=project_id)
        


def get_cryosparc_env(cm_command: str) -> dict:
    # Execute 'cryosparcm env' and capture the output
    env_output = subprocess.check_output('%s env' % cm_command, shell=True, text=True)
    # Iterate through each line of the output
    result_env = os.environ.copy()
    # TODO - do with regex
    for line in env_output.splitlines():
        if 'export' in line:
            key, value = line.split(' ')[1].split('=')
            # Remove leading and trailing characters (if necessary)
            value = value.strip('"').strip("'")
            key = key.strip('"').strip("'")
            result_env[key] = value
    return result_env



# CLI for the cryosparc engine
if __name__ == "__main__":

    main_parser = argparse.ArgumentParser()
    main_parser.add_argument("-e", "--email", dest="email", help="Email of the cryosparc user", required=True)
    main_parser.add_argument("--cm", dest="cm", help="Path to cryosparccm command to obtain environment from. If not given, it will be assumed that the correct environment is set", required=False)

    subparsers = main_parser.add_subparsers()
    
    setup_parser = subparsers.add_parser('create')
    setup_parser.set_defaults(func="create_project")
    setup_parser.add_argument("-p", "--project-path", dest="session_id", type=pathlib.Path, help="Path to the project, last part of this path will be used as a project name/title", required=True)
    setup_parser.add_argument("-c", "--cluster", dest="cluster", help="Computational cluster", required=True)
    setup_parser.add_argument("-w", "--workflow_file", dest="workflow", help="Path to the file with JSON workflow for the project, - for stdin", required=True, type=argparse.FileType(encoding='utf-8'))

    run_parser = subparsers.add_parser("run")
    run_parser.set_defaults(func="run_project_session")
    run_parser.add_argument("--pid", dest="project_id", help="Project ID", required=True)
    run_parser.add_argument("--sid", dest="session_id", help="Session ID", required=True)

    stop_parser = subparsers.add_parser("stop")
    stop_parser.set_defaults(func="stop_project_session")
    stop_parser.add_argument("--pid", dest="project_id", help="Project ID", required=True)
    stop_parser.add_argument("--sid", dest="session_id", help="Session ID", required=True)

    detach_parser = subparsers.add_parser("detach")
    detach_parser.set_defaults(func="detach_project")
    detach_parser.add_argument("--pid", dest="project_id", help="Project ID", required=True)

    # Parse arguments
    parsed_main = vars(main_parser.parse_args(sys.argv[1:]))
    # Special case for workflow json file - load it and close
    if parsed_main.get("workflow"):
        parsed_main["workflow"] = json.load(parsed_main["workflow"])
        parsed_main["workflow"].close()

    # Should we prepare environment?
    if parsed_main.get("cm"):
        print("Setting enviroment using cryosparcm...")
        env = get_cryosparc_env(parsed_main["cm"])
        for key, value in env.items():
            os.environ[key] = value

    # Cluster given?
    compute_configuration = {}
    if parsed_main.get("cluster"):
        compute_configuration = {"cluster": parsed_main["cluster"]}
        del parsed_main["cluster"]

    # Use cryosparc engine to invoke the subcommand
    cryosparc = CryosparcEngine(parsed_main["email"], sys.stdout, compute_configuration)
    getattr(cryosparc, parsed_main.func)(**vars(parsed_main))



class CryosparcWrapper:
    """ Connects LIMS experiments and their processing with the cryosparc engine"""
    def __init__(self, 
                 exp_engine: experiment.ExperimentStorageEngine, 
                 config: dict): 
        self.exp_engine = exp_engine
        self.config = config
        self.cm_path = config["CmPath"]
        self.projects_dir = config["ProjectsDir"]
        self.project_name = exp_engine.exp.secondary_id
        self.email = config["Email"]
        self.cluster = config["ComputationalCluster"]


class CryosparcProcesingHandler(experiment.ExperimentModuleBase): 

    def provide_experiments(self):
        active_experiments = super().provide_experiments()
        return filter(lambda e: e.processing.engine == "cryosparc" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), active_experiments)

    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        cconf = self.config["CyrosparcConfig"]
        cw = CryosparcWrapper(exp_engine, cconf)
        # cw.cluster.create_project()


    
