""" Periodically schedulabe service to clean up source datafolder picked for experiments"""
import experiment
from experiment import ExperimentStorageEngine, ExperimentWrapper
import shutil, common, datetime
import configuration

class DataCleanService(configuration.LimsNodeModule):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments(subpath="with_sourcedir")
    
    def step(self):
        for exp in self.provide_experiments():
            self.step_experiment(exp)

    def step_experiment(self, exp: ExperimentWrapper):
        source_dir = exp.storage.source_directory
        if not source_dir:
            return
        
        if not source_dir.exists():
            exp.storage.source_directory = None
            return
        
        # Check if directory was untouched for configured time by checking file with latest modification time
        latest_file = max(source_dir.rglob('*'), key=lambda f: f.stat().st_mtime, default=None)
        clean_after = common.parse_timedelta(self.module_config.get("clean_after", "7.00:00:00"))
        now = datetime.datetime.now(datetime.timezone.utc)
        if latest_file and (now - datetime.datetime.fromtimestamp(latest_file.stat().st_mtime, tz=datetime.timezone.utc)) < clean_after:
            return
        
        if bool(self.module_config.get("dry_run", True)):
            self.logger.info(f"Would clean up {source_dir}, dry run enabled, skipping.")
            return
        
        errs = []
        def onerror(func, path, exc_info):
            errs.append((func, path, exc_info))

        shutil.rmtree(source_dir, onexc=onerror)


        if errs:
            errs_string = "\n".join([f"{path}: {func} {exc_info}" for func, path, exc_info in errs])
            self.logger.error(f"Errors while cleaning up {source_dir}: \n {errs_string}")

        if not errs or not source_dir.exists():
            self.logger.info(f"Cleaned up {source_dir}")
            exp.storage.source_directory = None