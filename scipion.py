# =================== Tools for interaction between LIMS and scipion =====================

import logging, psutil, subprocess, configparser, configuration, pathlib, json, shutil
import re
from data_tools import FileLogSnifferSource, FileWatcher, LogSniffer, LogSnifferCompositeSource, FilesWatcher
import experiment, processing_tools, os
from common import StateObj, exec_state, multiglob
from logger_db_api import wrap_logger_origin


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
        self.scipion_home = pathlib.Path(scipion_config.get("ScipionHome", False) or os.environ["SCIPION_HOME"])
        self.scipion_exec = self.scipion_home / "scipion3"

        # Last thing to determine is ScipionUserData (working directory)
        self.scipion_workspace_directory = pathlib.Path(self._extract_scipion_config_var("SCIPION_USER_DATA") or self._DEFAULT_SCIPION_DIRECTORY).expanduser()
        self.scipion_projects_directory = self.scipion_workspace_directory / "projects"
        self.scipion_project_directory = self.scipion_projects_directory / self.project_name
        self.project_directory = project_location or self.scipion_project_directory
        self.wf_template_path = self.scipion_workspace_directory / f"template_{self.project_name}.json"
        self.logger = logger

        # scipion workspace directory must exists and be writable in order to run scipion
        self.scipion_projects_directory.mkdir(exist_ok=True, parents=True)

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
        return { **os.environ, **self.scipion_config.get("Env", {}) }.get(key, None)

    def _prepare_summary_template(self):
        # If we have a template, move it to correct destination so ProtocolMonitorSummary consumes it https://github.com/scipion-em/scipion-em-facilities/blob/devel/emfacilities/protocols/report_html.py
        summary_templ = self.scipion_config.get("SummaryTemplate", None)
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
            logfiles = list(protdir.glob("logs/*.log")) + list(protdir.glob("logs/*.stderr")) + list(protdir.glob("logs/*.stdout"))  # STDOUT is just too much
            w = [FileWatcher(l, name=protdir.name + " / " + l.name) for l in logfiles]
            watchers.extend(w)

        return FilesWatcher(fileWatchers=watchers, consumer=consumer)


    def prepare_scipion_command(self, append=""):
        command = str(self.scipion_exec) + " " + append
        # Prepare environment - merge current env with configured one
        env = self.scipion_config.get("Env", {}).copy()
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

    def ensure_project(self, template: list, purge_existing=False):
        if self.project_exists() and not purge_existing:
            self.logger.debug(f"Skipping project creation, already exists: {self.project_directory}")
            return
        
        if self.project_exists() and purge_existing:
            self.logger.debug("Project exists, lets purge it before proceeding...")
            self.purge_project()
        
        # Put JSON template to a file because scipion cli requires it
        self.wf_template_path.write_text(json.dumps(template, indent=4))
        self.logger.debug(f"Ensuring project at {self.project_directory}")

        # Create project with this template
        cmd, env = self.prepare_scipion_command(f'python -m pyworkflow.project.scripts.create "{self.project_name}" "{self.wf_template_path}"')
        self.logger.info(f"Invoking: {cmd}")
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True, env=env)
        self.logger.info(f"Exit {result.returncode}")
        self.logger.info(f"Stdout {result.stdout.strip()}")
        self.logger.info(f"Stderr {result.stderr.strip()}")

        if result.returncode:
            self.purge_project()
            raise RuntimeError("Error during project creation - purged")
        
        # Move project to desired location if desired and symlink it 
        if self.scipion_project_directory != self.project_directory:
            
            shutil.copytree(self.scipion_project_directory, self.project_directory)
            shutil.rmtree(self.scipion_project_directory)
            self.scipion_project_directory.symlink_to(self.project_directory, target_is_directory=True)
            self.logger.debug(f"Moved project and symlinked: {self.scipion_project_directory} -> {self.project_directory}")

        # Also move the template file to the project directory
        shutil.move(self.wf_template_path, self.project_directory / "workflow.json")

    def purge_project(self): 
        self.logger.info(f"Purging projects... {self.scipion_project_directory}, {self.project_directory}")
        if self.scipion_project_directory.is_symlink():
            self.scipion_project_directory.unlink()
        else:
            shutil.rmtree(self.scipion_project_directory, ignore_errors=True)

        shutil.rmtree(self.project_directory, ignore_errors=True)

    def _get_processing_source_path(self): 
        """ Implemented by child """
        raise NotImplementedError()

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
        submit_script_template_file = self.project_directory / f"queue_schedule_submit.sh"

        context = { 
            "submit_script": str(submit_script_template_file),
            "project_name": self.project_name,
            "wf_template_path": str(self.wf_template_path),
            "source_data_root": str(self._get_processing_source_path().parent) # This leads to project collection, but we need moutpoint - the root, therefore "parent"
        }

        submit_cmd = submit_cmd_template % context
        submit_script = submit_script_template % context
        submit_script_template_file.write_text(submit_script)

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
        proc = subprocess.Popen(cmd, shell=True, env=env)
        self.logger.info(f"Running scipion scheduler on the project, pid is {proc.pid}")
        return proc.pid

    @property
    def summary_results_directory(self):
        globbed = next((self.project_directory / "Runs").glob("*MonitorSummary*"), None)
        return globbed / "extra" if globbed else None
    
    def get_summary_results_file(self):
        result = next((self.project_directory / "Runs").glob("*MonitorSummary/**/index.html"), None)
        return result

    
    # def get_summary_results_sniffer(self):
    #     """ Get new summary result files """
    #     if not self.summary_results_directory:
    #         return FilesWatcher([])

    #     sniffer = FilesWatcher(
    #         [FileWatcher(self.summary_results_directory / "index.html")] +
    #         [FileWatcher(p) for p in self.summary_results_directory.glob("*.jpg")]
    #         )

    #     return sniffer

    def get_summary_results_file_watcher(self):
        """ Return summary result, only if it was updated """
        index_file = self.get_summary_results_file()
        print(index_file)
        if not index_file:
            return None
        return FileWatcher(index_file)
    

    
    

class ScipionExpWrapper(ScipionWrapper, StateObj):
    """
    Extends ScipionWrapper with a context of LIMS experiment and it's storage
    """
    def __init__(self, storage_engine : experiment.ExperimentStorageEngine, scipion_config: dict, logger: logging.Logger) -> None:
        self.storage_engine = storage_engine
        self.exp = storage_engine.exp
        project_name = self.exp.secondary_id
        super().__init__(scipion_config, logger, project_name)

    def _get_processing_source_path(self): 
        if "SourceDataRoot" in self.scipion_config and self.scipion_config["SourceDataRoot"]:
            return pathlib.Path(self.scipion_config["SourceDataRoot"]) / self.exp.secondary_id
        else:
            return self.storage_engine.resolve_target_location()
        
    def get_state(self):
        return self.exp.processing.state

    def prepare_protocol_data(self, protocols: list):
        """ Execute several adjustments to the protocol data so it is compatible with the scipion 
            The returned new workflow should be ready to be scheduled and run
            If None is returned, the processing is not yet ready to be scheduled (i.e. the first movie has not arrived yet to the storage so we dont know the movie format and suffix)
        """
        # Copy protocols 
        protocols = json.loads(json.dumps(protocols)) # Deep copy
        movies_handler = processing_tools.EmMoviesHandler(self.storage_engine)

        movie_info = movies_handler.set_importmovie_info(protocols, self._get_processing_source_path())
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


class ScipionProcessingHandler(experiment.ExperimentModuleBase):

    def provide_experiments(self):
        active_experiments = super().provide_experiments()
        # TODO - filter by state
        # TODO - filter by instrument/technique? (splitting jobs between the nodes)
        return filter(lambda e: e.processing.engine == "scipion" and (e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name), active_experiments)

    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        exp = exp_engine.exp
        # Check if process is active, if not, run it
        # self.logger.debug("Stepping scipion processing...")
        sciw = ScipionExpWrapper(exp_engine, self.module_config["ScipionConfig"], wrap_logger_origin(exp_engine.logger, "scipion"))
        # workflow = sciw.prepare_protocol_data(exp.processing.workflow)
        # print(workflow)
        # # sciw.ensure_project(workflow, purge_existing=True)
        # return

        # Define what will periodically happend to the project in each of the states
        def state_project_not_exists():
            workflow = sciw.prepare_protocol_data(exp.processing.workflow)
            if workflow: # If we get workflow, project is ready to be created and scheduled
                sciw.ensure_project(workflow, purge_existing=True)
                exp.processing.state = experiment.ProcessingState.READY

        def state_project_ready():
            pid = sciw.schedule()
            if pid:
                exp.processing.pid = str(pid)
                exp.processing.state = experiment.ProcessingState.RUNNING

        def state_project_running():
            # Sniff for new logs and send them to the LIMS
            new_data = []
            def log_consumer(watcher: FileWatcher, data: str):
                new_data.append((watcher.name, data, "text/plain"))    
            sciw.prepare_protocol_log_watcher(log_consumer).consume_new_data()
            exp.exp_api.upload_document_files(exp.processing.log_document_id, files=new_data, append=True)

            # Sniff for new results and upload them to the LIMS
            result_sniffer = sciw.get_summary_results_file_watcher()
            if result_sniffer and result_sniffer.has_changed_since_last_mark():
                with result_sniffer.file_path.open("rb") as stream:
                    exp.exp_api.upload_document_files(exp.processing.result_document_id, ("Scipion results", stream, "text/html"))
                    result_sniffer.mark_processed()
  
            # Sniff for processing result files and submit them ba0ck to the storage
            # But dont use experiment logger
            exp_engine.sniff_and_transfer(sciw.project_directory, exp_engine.data_rules.with_tags("processed"), keep_source_files=True, logger=logging.getLogger("ProcessingTransfer"))

        def state_project_finished():
            # sciw.logger.info("Scipion project finished")
            # state_project_running()
            pass
            
        exec_state(sciw,
            {
                experiment.ProcessingState.UNINITIALIZED: state_project_not_exists,
                experiment.ProcessingState.READY: state_project_ready,
                experiment.ProcessingState.RUNNING: state_project_running,
                experiment.ProcessingState.COMPLETED: state_project_finished,
                experiment.ProcessingState.DISABLED: lambda: None,
            }
        )
        
        