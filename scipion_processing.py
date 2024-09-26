# =================== Tools for interaction between LIMS and scipion =====================

import datetime
import logging, subprocess, pathlib, json, shutil
import re
from data_tools import DataRule, FileLogSnifferSource, FileWatcher, LogSniffer, LogSnifferCompositeSource, FilesWatcher, DataRulesWrapper
import processing_tools, os
from common import StateObj, exec_state
from logger_db_api import wrap_logger_origin
from experiment import ExperimentModuleBase, ExperimentStorageEngine, ExperimentsApi, JobState, ProcessingState
import tempfile
import common

class ScipionWrapper:
    """ Wrapper for scipion project 
        Supports managing the scipion project, configuration, locations, invoking scipion scripts and more,
        all in the context of LIMS needs
    """

    _DEFAULT_SCIPION_DIRECTORY = pathlib.Path("~/ScipionUserData").expanduser()
    _SCIPION_HOME_CONFIG_DIR = pathlib.Path("~/.config/scipion").expanduser()

    def __init__(self, scipion_config: dict, logger: logging.Logger, project_name: str, project_location: pathlib.Path=None) -> None:
        self.scipion_config = scipion_config
        self.project_name = project_name
        # Determine scipion home directory (where it is installed)
        self.scipion_home = pathlib.Path(scipion_config.get("scipion_home", False) or os.environ["SCIPION_HOME"])
        self.scipion_exec = self.scipion_home / "scipion3"

        # Last thing to determine is ScipionUserData (working directory)
        self.scipion_workspace_directory = pathlib.Path(self._extract_scipion_config_var("SCIPION_USER_DATA") or self._DEFAULT_SCIPION_DIRECTORY).expanduser()
        self.scipion_projects_directory = self.scipion_workspace_directory / "projects"
        self.scipion_project_directory = self.scipion_projects_directory / self.project_name
        self.project_directory = project_location / project_name or self.scipion_project_directory
        self.wf_template_path = pathlib.Path(tempfile.gettempdir()) / f"template_{self.project_name}.json" # TODO maybe use tempfile
        self.logger = logger

        # scipion workspace directory must exists and be writable in order to run scipion
        self.scipion_projects_directory.mkdir(exist_ok=True, parents=True)

        # Files for running in queue
        self.stop_signal_file = self.project_directory / "_request_stop"
        self.submit_script_file = self.project_directory / "queue_schedule_submit.sh"

    def _get_scipion_config_dir(self):
        """ Get directory that scipion considers as config directory """
        # First try user config 
        user_conf =  self._SCIPION_HOME_CONFIG_DIR / "scipion.conf"
        if user_conf.exists():
            return self.user_conf.parent
        # Then it is global config
        return self.scipion_home / "config"
        
    def _extract_scipion_config_var(self, key: str):
        re_pattern = r'\b{} *=(.*)'.format(re.escape(key))
        # Config files
        files = [self._SCIPION_HOME_CONFIG_DIR / "scipion.conf", 
                 self.scipion_home / "config" / "scipion.conf"]

        for f in files:
            if f.exists():
                match = re.search(re_pattern, f.read_text())
                if match:
                    return match.group(1).strip(" '\"")
                
        # 3) Try environment variable
        return { **os.environ, **self.scipion_config.get("env", {}) }.get(key, None)

    def _prepare_summary_template(self):
        # If we have a template, move it to correct destination so ProtocolMonitorSummary consumes it https://github.com/scipion-em/scipion-em-facilities/blob/devel/emfacilities/protocols/report_html.py
        summary_templ = self.scipion_config.get("summary_template", None)
        if summary_templ: 
            target_file = self._get_scipion_config_dir() / "execution.summary.html"
            try:
                shutil.copyfile(summary_templ, target_file)
            except Exception as e:
                self.logger.warn(f"Failed to copy summary template to configuration: {target_file}, {e}")

    def prepare_protocol_log_watcher(self, consumer):
        # Get protocol directories
        protocol_dirs = self.project_directory.glob("Runs/*")
        watchers = []
        for protdir in protocol_dirs:
            logfiles = list(protdir.glob("logs/*.log")) + list(protdir.glob("logs/*.stderr")) + list(protdir.glob("logs/*.stdout"))
            w = [FileWatcher(l, name=protdir.name + " / " + l.name) for l in logfiles]
            watchers.extend(w)

        return FilesWatcher(fileWatchers=watchers, consumer=consumer)


    def prepare_scipion_command(self, append=""):
        command = str(self.scipion_exec) + " " + append
        # Prepare environment - merge current env with configured one
        env = self.scipion_config.get("env", {}).copy()
        # Some of the env values can be variables, use python str % to replace them
        for k, v in env.items():
            env[k] = str(v) % env

        env = { **os.environ, **env }
        return command, env

    def prepare_protocol_data(self, protocols: list):
        """ Processes and adjusts workflow so that the project can be shceduled and run 
            Returns: workflow if ready, None if project is not ready, raises if state is invalid
        """
        raise NotImplementedError()
    
    def project_exists(self):
        return (self.project_directory).exists()
    
    def create_project(self, template: list):
        # Put JSON template to a file because scipion cli requires it
        self.wf_template_path.write_text(json.dumps(template, indent=4))
        self.logger.debug(f"Creating project at {self.project_directory}")

        # Create project with this template
        cmd, env = self.prepare_scipion_command(f'python -m pyworkflow.project.scripts.create "{self.project_name}" "{self.wf_template_path}" "{self.project_directory.parent}"')
        self.logger.info(f"Invoking: {cmd}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, env=env)
        self.logger.info(f"Exit {result.returncode}")
        self.logger.info(f"Stdout {result.stdout.strip()}")
        self.logger.info(f"Stderr {result.stderr.strip()}")

        if result.returncode:
            self.purge_project()
            raise RuntimeError("Error during project creation - purged")
        
        # Move the template file to the project directory
        shutil.move(self.wf_template_path, self.project_directory / "workflow.json")

        # DEL not necessary actually done by scipion project creation script
        # Move project to desired location if desired and symlink it 
        # if self.scipion_project_directory != self.project_directory:
            
        #     shutil.copytree(self.scipion_project_directory, self.project_directory)
        #     shutil.rmtree(self.scipion_project_directory)
        #     self.scipion_project_directory.symlink_to(self.project_directory, target_is_directory=True)
        #     self.logger.debug(f"Moved project and symlinked: {self.scipion_project_directory} -> {self.project_directory}")



    def ensure_project(self, template: list, purge_existing=False):
        if self.project_exists() and not purge_existing:
            self.logger.debug(f"Skipping project creation, already exists: {self.project_directory}")
            return
        
        if self.project_exists() and purge_existing:
            self.logger.debug("Project exists, lets purge it before proceeding...")
            self.purge_project()

        return self.create_project(template)
        

    def purge_project(self): 
        self.logger.info(f"Purging project... {self.scipion_project_directory}, {self.project_directory}")
        if self.scipion_project_directory.is_symlink():
            self.scipion_project_directory.unlink()
        else:
            shutil.rmtree(self.scipion_project_directory, ignore_errors=True)

        shutil.rmtree(self.project_directory, ignore_errors=True)

    def schedule(self):
        self._prepare_summary_template()
        # Queue or command?
        if "Queue" in self.scipion_config:
            return self._schedule_queue()
        else:
            return self._schedule_command()
        

    def _schedule_queue(self):
        # Continue here on carolina
        que_conf = self.scipion_config["Queue"]
        submit_cmd_template = que_conf["SubmitCommandTemplate"]

        # We need to temporarily save submit script template so we can pass it as path
        submit_script_template = que_conf["SubmitScriptTemplate"]

        context = { 
            "submit_script": str(self.submit_script_file),
            "project_name": self.project_name,
            "wf_template_path": str(self.wf_template_path),
            "stop_signal_path": str(self.stop_signal_file),
            "scipion_exec": str(self.scipion_exec)
        }

        submit_cmd = submit_cmd_template % context
        submit_script = submit_script_template % context
        self.submit_script_file.write_text(submit_script)

        # Now we are ready to submit the queue job 
        self.logger.info(f"Executing: {submit_cmd}")
        result = subprocess.run(submit_cmd, shell=True, check=True)
        self.logger.info(f"Submitted queue job for the project schedule: {self.project_name}, exited with {result}")
        return 9999 # TODO - pid

    def _schedule_command(self):
        cmd, env = self.prepare_scipion_command(f'python -m pyworkflow.project.scripts.schedule "{self.project_name}"')
        # self.logger.debug(f"Invoking: {cmd}")
        # result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
        # self.logger.debug(f"Exit {result.returncode}")
        # self.logger.debug(f"Stdout {result.stdout.strip()}")
        # self.logger.debug(f"Stderr {result.stderr.strip()}")

        # We need to fire and forget the process using Popen, because otherwise it will block the thread
        proc = subprocess.Popen(cmd, shell=True, env=env, start_new_session=True)
        self.logger.info(f"Running scipion scheduler on the project, pid is {proc.pid}")
        return proc.pid
    
    def stop(self):
        if "Queue" in self.scipion_config:
            return self._stop_queue()
        else:
            return self._stop_command()

    def _stop_command(self):
        cmd, env = self.prepare_scipion_command(f'python -m pyworkflow.project.scripts.stop "{self.project_name}"')
        self.logger.info(f"Invoking: {cmd}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, env=env)
        self.logger.info(f"Exit {result.returncode}")
        self.logger.info(f"Stdout {result.stdout.strip()}")
        self.logger.info(f"Stderr {result.stderr.strip()}")

    def _stop_queue(self):
        # Stopping in queue is signaled through the file - the script running in the queue should be waiting for it.
        self.stop_signal_file.touch()
        pass

    @property
    def summary_results_directory(self):
        globbed = next((self.project_directory / "Runs").glob("*MonitorSummary*"), None)
        return globbed / "extra" if globbed else None
    
    def get_summary_results_file(self):
        result = next((self.project_directory / "Runs").glob("*MonitorSummary/**/index.html"), None)
        return result

    def get_summary_results_file_watcher(self):
        """ Return summary result, only if it was updated """
        index_file = self.get_summary_results_file()
        if not index_file:
            return None
        return FileWatcher(index_file)
    

class ScipionExpWrapper(ScipionWrapper, StateObj):
    """
    Extends ScipionWrapper with a context of LIMS experiment and it's storage
    """
    def __init__(self, storage_engine : ExperimentStorageEngine, scipion_config: dict, logger: logging.Logger, em_tools: processing_tools.EmMoviesHandler) -> None:
        self.storage_engine = storage_engine
        self.exp = storage_engine.exp


        project_name = f"scipion_{self.exp.secondary_id}"
        storage_loc = storage_engine.resolve_target_location()
        target_loc = storage_loc 
        self.raw_data_dir = storage_loc
        self.em_handler = em_tools

        if not storage_loc:
            # If we dont have storage location, we need to put the project to the configured location (and do data transfering later)
            target_loc = pathlib.Path(scipion_config["projects_dir"])
            self.raw_data_dir = target_loc / f"raw_scipion_{self.exp.secondary_id}"

        super().__init__(scipion_config, logger, project_name, target_loc)

    def get_state(self):
        return self.exp.processing.state

    def prepare_protocol_data(self, protocols: list):
        """ Execute several adjustments to the protocol data so it is compatible with the scipion 
            The returned new workflow should be ready to be scheduled and run
            If None is returned, the processing is not yet ready to be scheduled (i.e. the first movie has not arrived yet to the storage so we dont know the movie format and suffix)
        """
        # Copy protocols 
        protocols = json.loads(json.dumps(protocols)) # Deep copy

        movie_info = self.em_handler.set_importmovie_info(protocols, self.raw_data_dir)
        if not movie_info: # Not ready
            return None
            
        # Make empty strings null - TODO this is bad behavior of IConfigurationSection on LIMS side - quick dirty fix
        for prot in protocols:
            for k, v in prot.items():
                if isinstance(v, str) and len(v) == 0:
                    prot[k] = None

        # Ensure some essential key mappings
        key_mapping = {
            "TYPE": "object.className",
            "ID": "object.id",
            "NAME": "object.label",
            "DESCRIPTION": "object.comment"
        }

        for prot in protocols:
            for k, v in key_mapping.items():
                prot[v] = prot[k]
            
        # Take configured static values and ensure no others for their keys are set in the workflow protocols
        replace_dict: dict = self.scipion_config["ModelOverwrites"] if "ModelOverwrites" in self.scipion_config else None
        if replace_dict:
            for prot in protocols:
                for k, v in filter(lambda x: x[0] in prot, replace_dict.items()):
                    prot[k] = v

        # Make protocol identifiers from int to string
                    
        # Another dirty changes to the protocol - scipion needs IDs to be strings even when they are given as numbers
        for prot in protocols:
            for k, v in prot.items():
                if k in ["ID", "object.id"] and isinstance(v, int):
                    prot[k] = str(v)
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], int):
                    prot[k] = list(map(str, v))
       
        
        return protocols


class ScipionProcessingHandler(ExperimentModuleBase):

    def provide_experiments(self):
        exps = ExperimentsApi(self._api_session).get_experiments_by_states(
            processing_state=[
                ProcessingState.UNINITIALIZED, 
                       ProcessingState.READY, 
                       ProcessingState.RUNNING,
                       ProcessingState.FINALIZING]
            )
        

        return filter(lambda e: e.processing.engine == "scipion" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), exps)

    def step_experiment(self, exp_engine: ExperimentStorageEngine):
        exp = exp_engine.exp
        cconf = self.module_config["ScipionConfig"]
        iconf = self.module_config.get("imod")
        sciw = ScipionExpWrapper(exp_engine, cconf, wrap_logger_origin(exp_engine.logger, "scipion"), processing_tools.EmMoviesHandler(exp_engine, iconf))

        def _filter_relevant_upload_results(up_result: list):
            pattern = re.compile(r'\.(log|stdin|stdout|stderr|sqlite|html)$')
            up_result = [f for f in up_result if not pattern.search(f[0])]
            return up_result

        # Define what will periodically happend to the project in each of the states
        def state_project_not_exists():
            workflow = sciw.prepare_protocol_data(exp.processing.workflow)
            if workflow: # If we get workflow, project is ready to be created and scheduled
                sciw.ensure_project(workflow, purge_existing=True)
                exp.processing.state = ProcessingState.READY

        def state_project_ready():
            pid = sciw.schedule()
            if pid:
                exp.processing.pid = str(pid)
                exp.processing.state = ProcessingState.RUNNING

        
        def state_project_running():
            # Download new raw data to the processing source directory
            dw_result, errs = exp_engine.download_raw(sciw.raw_data_dir)

            # Return new data from processing project to storage
            up_result, errs = exp_engine.upload_processed(sciw.project_directory, sciw.project_directory.name)
            print("RUN UP", up_result)

            # Sniff for new logs and send them to the LIMS as documents to the report
            new_data = []
            def log_consumer(watcher: FileWatcher, data: str):
                new_data.append((watcher.name, data, "text/plain"))    
            sciw.prepare_protocol_log_watcher(log_consumer).consume_new_data()
            exp.processing.log_document.upload_files(new_data, append=True)

            # Sniff for new results and upload them to the LIMS
            result_sniffer = sciw.get_summary_results_file_watcher()
            if result_sniffer and result_sniffer.has_changed_since_last_mark():
                with result_sniffer.file_path.open("rb") as stream:
                    exp.processing.result_document.upload_files(("Scipion results", stream, "text/html"))
                    result_sniffer.mark_processed()


            # Check if the processing is done
            # Some log files can change even though nothing reasonable is happening, ignore them
            up_result = _filter_relevant_upload_results(up_result)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if up_result or not exp_engine.exp.processing.last_update:
                # Update last processing change time 
                exp_engine.exp.processing.last_update = now_utc

            # Check if we should complete the processing
            timeout_delta = common.parse_timedelta(cconf.get("processing_timeout", "00:10:00.0"))
            change_delta = now_utc - exp_engine.exp.processing.last_update
            is_still_active = exp_engine.exp.state == JobState.ACTIVE
            print("RUN Check", up_result, str(now_utc), timeout_delta.seconds, change_delta.seconds, is_still_active,  exp_engine.exp.processing.last_update)
            if not is_still_active and timeout_delta < change_delta:
                # Last change is older than configured timeout - finish
                exp_engine.exp.processing.last_update = now_utc
                exp_engine.exp.processing.state = ProcessingState.STOP_REQUESTED

        def state_stop_requested():
            sciw.stop()
            exp.processing.state = ProcessingState.FINALIZING

        def state_project_finalizing():
            up_result, errs = exp_engine.upload_processed(sciw.project_directory, sciw.project_directory.name)
            up_result = _filter_relevant_upload_results(up_result)
            print("FIn UP", up_result)
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            if up_result or not exp_engine.exp.processing.last_update:
                # Update last processing change time 
                exp_engine.exp.processing.last_update = now_utc
            timeout_delta = common.parse_timedelta(cconf.get("finalizing_timeout", "00:5:00.0"))
            change_delta = now_utc - exp_engine.exp.processing.last_update
            print("FIN Check", str(now_utc), timeout_delta.seconds, change_delta.seconds,  exp_engine.exp.processing.last_update)
            if timeout_delta < change_delta:
                exp_engine.exp.processing.state = ProcessingState.COMPLETED
  

            
        exec_state(sciw,
            {
                ProcessingState.UNINITIALIZED: state_project_not_exists,
                ProcessingState.READY: state_project_ready,
                ProcessingState.RUNNING: state_project_running,
                ProcessingState.STOP_REQUESTED: state_stop_requested,
                ProcessingState.FINALIZING: state_project_finalizing,
                ProcessingState.COMPLETED: lambda: None,
                ProcessingState.DISABLED: lambda: None,
            }
        )