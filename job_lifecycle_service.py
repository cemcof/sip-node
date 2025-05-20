import datetime
import experiment, re
from experiment import ExperimentStorageEngine, OperationState


class JobLifecycleService(experiment.ExperimentModuleBase):

    def provide_experiments(self):
        exps = experiment.ExperimentsApi(self._api_session).get_experiments_by_states(
            exp_state=[experiment.JobState.START_REQUESTED, 
                       experiment.JobState.ACTIVE, 
                       experiment.JobState.STOP_REQUESTED]
            )

        return exps


    def step_experiment(self, exp_engine: ExperimentStorageEngine):
        types = self.module_config["experiment_types"]
        exp_filter = next(filter(lambda x: exp_engine.exp.exp_type_matches(x["pattern"]), types), None)
        if not exp_filter: # This experiment should not be handled by this node
            return
        
        # def resolve_source_dir(self):
        #     src_dir_original_path = self.exp.storage.source_directory
        #     src_dir_path = self.config.lims_config.translate_path(src_dir_original_path, self.exp.secondary_id) or src_dir_original_path
        #     return src_dir_path
    
        def exp_start():
            # Called once when experiment is being started
            exp_engine.prepare()
            exp_engine.restore_metadata()

            # Set state to active and transfering, submit storage access information
            patch = {
                "Storage": {
                    **exp_engine.get_access_info(),
                    "State": experiment.StorageState.TRANSFERING.value
                },
                "State": experiment.JobState.ACTIVE.value
            }


            # Set processing node, if necessary 
            proc = exp_engine.exp.processing
            if proc.state != experiment.ProcessingState.DISABLED and (not proc.node_name or proc.node_name == "any"):
                # Do we have the rule? 
                rules = exp_filter.get("assign_processing", None)
                if rules and proc.engine in rules:
                    patch["Processing"] = {"Node": rules[proc.engine]}

            # Send email about the new experiment
            if exp_engine.exp.notify_user:
                email_conf = self.get_experiment_config(exp_engine.exp)["JobStart"]
                exp_engine.exp.exp_api.send_email(email_conf)

            exp_engine.exp.exp_api.patch_experiment(patch)
            exp_running()

        def _handle_auto_stop():
            idle_timeout = self.get_experiment_config(exp_engine.exp).idle_timeout  
            last_update = exp_engine.exp.storage.dt_last_updated
            if idle_timeout and last_update and (datetime.datetime.now(datetime.timezone.utc) - last_update) > idle_timeout:
                exp_engine.exp.state = experiment.JobState.STOP_REQUESTED

        def exp_running():
            exp_data_source = self.module_config.lims_config.translate_path(exp_engine.exp.data_source.source_directory, exp_engine.exp.secondary_id)
            exp_engine.sniff_and_process_metafile(exp_data_source)

            # dt_upload_start = datetime.
            ups, errs = exp_engine.upload_raw(exp_data_source)
            if ups or errs or (exp_engine.exp.storage.dt_last_updated is None): 
                exp_engine.exp.storage.dt_last_updated = datetime.datetime.now(datetime.timezone.utc)

            _handle_auto_stop()


        def exp_finish():
            exp_engine.exp.exp_api.patch_experiment({
                "Storage": {"State": experiment.StorageState.IDLE.value},
                "State": experiment.JobState.FINISHED.value
            })

        action_map = {
            experiment.JobState.START_REQUESTED: exp_start,
            experiment.JobState.ACTIVE: exp_running,
            experiment.JobState.STOP_REQUESTED: exp_finish
        }

        action_map[exp_engine.exp.state]()
