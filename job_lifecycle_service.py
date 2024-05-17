import experiment, re
from experiment import ExperimentStorageEngine

class JobLifecycleService(experiment.ExperimentModuleBase):

    def provide_experiments(self):
        exps = experiment.ExperimentsApi(self._api_session).get_experiments({
            "expState": f"{experiment.JobState.START_REQUESTED.value},{experiment.JobState.ACTIVE.value},{experiment.JobState.STOP_REQUESTED.value}"
        })

        # Select only experiments according to configuration
        types = self.module_config["ExperimentTypes"]
        return filter(lambda e: e.exp_type_matches(types), exps)


    def step_experiment(self, exp_engine: ExperimentStorageEngine):

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

            # If we have archiving and publication, request also draft creation
            if exp_engine.exp.publication.engine and exp_engine.exp.storage.archive:
                patch["Publication"] = {"State": experiment.PublicationState.DRAFT_CREATION_REQUESTED.value}

            exp_engine.exp.exp_api.patch_experiment(patch)
            exp_running()

        def exp_running():
            exp_data_source = self.module_config.lims_config.translate_path(exp_engine.exp.storage.source_directory, exp_engine.exp.secondary_id)
            print(exp_data_source)
            exp_engine.sniff_and_process_metafile(exp_data_source)
            exp_engine.sniff_and_transfer_raw(exp_data_source)


        def exp_finish():
            # Called once when experiment is finishing, send email
            if exp_engine.exp.notify_user:
                exp_engine.exp.exp_api.send_email(exp_engine.e_config["JobFinish"])
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
