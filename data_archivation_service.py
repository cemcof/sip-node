""" Service responsible for cheking whether data should be archived, if so, moves it from one storage to another. """
import pathlib
import experiment, tempfile
from data_tools import TransferAction

class DataArchivationService(experiment.ExperimentModuleBase):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments_by_states(storage_state=[experiment.StorageState.ARCHIVATION_REQUESTED, experiment.StorageState.ARCHIVING])
    
    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        # We have experiment current storage engine
        # Our task is to move data to another storage engine, if desired (configured)

        # DELETING DATA??? 
        exp_target_storage_engine = self.module_config["archivation_storage"]
        if exp_engine.exp.storage.engine == exp_target_storage_engine:
            print("Same storage, already archvied")
            # We are already on the target storage, nothing to do, just mark as archived
            exp_engine.exp.storage.state = experiment.StorageState.ARCHIVED
            return
        
        print(f"ARCHIVE {exp_engine.exp.secondary_id} Temporarily disabled archivation")
        return
    
        exp_config = self.module_config.lims_config.get_experiment_config(exp_engine.exp.instrument, exp_engine.exp.technique)
        exp_target_storage: experiment.ExperimentStorageEngine = self.exp_storage_engine_factory(exp_engine.exp, exp_config, exp_engine.logger, self.module_config, exp_target_storage_engine)

        # Check that both storages are accessible
        if not exp_engine.is_accessible() or not exp_target_storage.is_accessible():
            self.logger.warn("One of the storages is not accessible")
            return
        
        # Set state that we are archiving this experiment 
        exp_engine.exp.storage.state = experiment.StorageState.ARCHIVING

        # Archive (=move) data
        try:
            print("ARCH", exp_engine.exp.secondary_id, exp_engine.irods_collection.collection_path, exp_engine.connection_config)
            archive_data_rules = exp_engine.data_rules.with_tags("archive")
            transfers, errs = exp_engine.transfer_to(exp_target_storage, archive_data_rules, transfer_action=TransferAction.MOVE, session_name=f"archivation_to_{exp_target_storage_engine.replace('/', '_')}")
            if errs:
                raise Exception(f"Errors during archivation: {errs}")
        except Exception as e:
            # Return state back to archivation requested
            exp_engine.exp.storage.state = experiment.StorageState.ARCHIVATION_REQUESTED
            exp_engine.logger.exception(f"Failed to archive data: {e}")
            return
        
        # From now, data is safely transfered to the new storage, we can purge the old one
        exp_engine.purge()

        # Update experiment storage info with new access storage
        exp_engine.exp.exp_api.patch_experiment({
            "Storage": {
                "State": experiment.StorageState.ARCHIVED.value,
                "StorageEngine": exp_target_storage_engine,
                **exp_target_storage.get_access_info()
            }
        })

        # Send notification email that the archivation was done 
        email_conf = self.module_config.lims_config.get_experiment_config(exp_engine.exp.instrument, exp_engine.exp.technique)["DataArchived"]
        exp_engine.exp.exp_api.send_email(email_conf)