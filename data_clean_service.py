""" Periodically schedulabe service to clean up source datafolder picked for experiments"""
import experiment
from experiment import ExperimentStorageEngine, ExperimentWrapper
import shutil, common, datetime
import configuration
from proxy_transferer import find_proxy_destination_directory_helper

class DataCleanService(configuration.LimsNodeModule):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments(subpath="source_cleanable")
    
    def step(self):
        for exp in self.provide_experiments():
            try:
                self.step_experiment(exp)
            except Exception as e:
                self.logger.error(f"Error while cleaning up experiment {exp.data_model['SecondaryId']}: {e}")
                                                                        
    def step_experiment(self, exp: ExperimentWrapper):
        source_dir = exp.data_source.source_directory
        proxy_source_dir = find_proxy_destination_directory_helper(exp, self.module_config.lims_config)

        if not source_dir:
            return
        
        if not source_dir.exists():
            exp.data_source.mark_cleaned()
            return
        
        # Check if directory was untouched for configured time by checking file with latest modification time
        latest_file = max(source_dir.rglob('*'), key=lambda f: f.stat().st_mtime, default=None)
        now = datetime.datetime.now(datetime.timezone.utc)
        if latest_file and (now - datetime.datetime.fromtimestamp(latest_file.stat().st_mtime, tz=datetime.timezone.utc)) < exp.data_source.clean_after:
            return
        
        if bool(self.module_config.get("dry_run", True)):
            self.logger.info(f"dry run, would clean up: -| {exp.data_model["Operator"]["Fullcontact"]} | {exp.data_model["User"]["Fullcontact"]} | {source_dir} | {proxy_source_dir} |-")
            return
        
        errs = []
        def onerror(func, path, exc_info):
            errs.append((func, path, exc_info))

        shutil.rmtree(source_dir, onexc=onerror)
        if proxy_source_dir:
            shutil.rmtree(proxy_source_dir, onexc=onerror)


        if errs:
            errs_string = "\n".join([f"{path}: {func} {exc_info}" for func, path, exc_info in errs])
            self.logger.error(f"Errors while cleaning up {source_dir}: \n {errs_string}")

        if not errs or not source_dir.exists():
            self.logger.info(f"Cleaned up {source_dir}")
            exp.data_source.mark_cleaned()