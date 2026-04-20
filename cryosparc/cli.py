import sys, os, pathlib, subprocess, json, argparse

class CryosparcEngine:
    def __init__(self, email: str, file_out=sys.stdout, compute_configuration={}) -> None:
        self.file_out = file_out
        self.email = email
        self.compute_configuration = compute_configuration

        from client.api_client import APIClient
        from core import core

        core.startup()

        self.api = APIClient(
            core.settings.api_base_url,
            auth=core.settings.api_admin_auth,
            headers={"license-id": core.settings.license_id},
        )
        self.db = core.db
        # Get user ID by email (assuming similar functionality exists)
        # This may need adjustment based on actual v5 user lookup methods
        self.user_id = None # self._get_user_id_by_email(email)

    def _get_user_id_by_email(self, email: str):
        # This is a placeholder - you may need to adjust based on actual v5 user lookup
        users = self.db.users.find({"email": email})
        if users:
            return users[0]["_id"]
        raise ValueError(f"User with email {email} not found")

    def _configure_project(self, project_uid, session_uid, workflow: dict):

        # Set compute parameters using new v5 API
        cluster = self.compute_configuration["cluster"]
        live_compute_resources = {
            "phase_one_lane": cluster,
            "phase_one_gpus": self.compute_configuration.get("phase_one_gpus", 1),
            "phase_two_lane": cluster,
            "phase_two_ssd": self.compute_configuration.get("phase_two_ssd", True),
            "auxiliary_lane": cluster,
            "auxiliary_ssd": self.compute_configuration.get("auxiliary_ssd", True)
        }

        self.api.sessions.update_compute_configuration(project_uid, session_uid, live_compute_resources)

        # Set exposure group parameters using new v5 API
        exposure_group_update = {}
        for expkey, expval in workflow["exposure"].items():
            if expval is not None:
                exposure_group_update[expkey] = expval

        self.api.sessions.update_exposure_group(project_uid, session_uid, 1, exposure_group_update)

        # Del exposure, only update session params
        del workflow["exposure"]
        self.api.sessions.update_session_params(project_uid, session_uid, workflow)

    def create_project(self, project_path: pathlib.Path, project_name: str, workflow):

        # Create project using new v5 API
        project = self.api.projects.create(parent_dir=str(project_path), title=project_name)
        project_uid = project.uid
        project_dirname = pathlib.Path(project.project_dir).name

        # Create a new Live session using new v5 API
        session = self.api.sessions.create(project_uid, title=f"{project_name} Live Session")
        session_uid = session.session_uid

        # Configure project
        self._configure_project(project_uid, session_uid, workflow)
        
        # Write the project and session id to the output
        print(f"{project_uid}/{session_uid}/{project_dirname}", file=self.file_out)
 
    def run_project_session(self, session_id, project_id):
        self.api.sessions.start(project_id, session_id)

    def stop_project_session(self, session_id, project_id):
        # Export exposures and particles, then pause session
        self.api.sessions.create_and_enqueue_export_exposures(project_id, session_id)
        self.api.sessions.pause(project_id, session_id)

    def detach_project(self, project_id):
        # In v5, project deletion may work differently - this is a placeholder
        # You may need to adjust based on actual v5 project management methods
        try:
            self.api.projects.delete(project_id)
        except:
            # Fallback if direct deletion doesn't work
            pass



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
    setup_parser.add_argument("-p", "--project-path", dest="project_path", type=pathlib.Path, help="Path to the project parent dir", required=True)
    setup_parser.add_argument("-n", "--project-name", dest="project_name", type=str, help="Title of the project", required=True)
    setup_parser.add_argument("-c", "--cluster", dest="cluster", help="Computational cluster", required=True)
    setup_parser.add_argument("-g", "--gpus", dest="gpu_count", help="Number of GPUs", type=int, default=1)
    setup_parser.add_argument("-w", "--workflow-file", dest="workflow", help="Path to the file with JSON workflow for the project, - for stdin", required=True, type=argparse.FileType(encoding='utf-8'))

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
    wf = parsed_main.get("workflow")
    if wf:
        parsed_main["workflow"] = json.load(wf)
        wf.close()

    # Should we prepare environment?
    if parsed_main["cm"]:
        env = get_cryosparc_env(parsed_main["cm"])
        for key, value in env.items():
            os.environ[key] = value
            # Need to set python path explicityl
            if key == "PYTHONPATH":
                sys.path.append(value)
    del parsed_main["cm"]

    # Cluster given?
    compute_configuration = {}
    if parsed_main.get("cluster"):
        compute_configuration = {"cluster": parsed_main["cluster"]}
        del parsed_main["cluster"]
    # GPU count given?
    if parsed_main.get("gpu_count"):
        compute_configuration["phase_one_gpus"] = parsed_main["gpu_count"]
        del parsed_main["gpu_count"]

    # Use cryosparc engine to invoke the subcommand
    cryosparc = CryosparcEngine(parsed_main["email"], sys.stdout, compute_configuration)
    del parsed_main["email"]
    func = getattr(cryosparc, parsed_main["func"])
    del parsed_main["func"]
    func(**parsed_main)
