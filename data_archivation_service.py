""" Service responsible for cheking whether data should be archived, if so, moves it from one storage to another. """
import pathlib
import experiment, tempfile

class DataArchivationService(experiment.ExperimentModuleBase):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments(
            {"storageState": experiment.StorageState.ARCHIVATION_REQUESTED.value}
        )
    
    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):

        
        # We have experiment current storage engine
        # Our task is to move data to another storage engine, if desired (configured)
        exp_target_storage_engine = self.module_config["archivation_storage"]
        exp_target_storage: experiment.ExperimentStorageEngine = self.exp_storage_engine_factory(exp_engine.exp, exp_engine.e_config, exp_engine.logger, self.module_config, exp_target_storage_engine)

        # Check that both storages are accessible
        if not exp_engine.is_accessible() or not exp_target_storage.is_accessible():
            self.logger.warn("One of the storages is not accessible")
            return
        
        # Set state that we are archiving this experiment 
        exp_engine.exp.exp_api.patch_experiment({
            "Storage": {
                "State": experiment.StorageState.ARCHIVING.value
            }
        })

        # Archive (=move) data
        try:
            # Glob over all files in source storage and move it one by one to target storage for now
            # However, there sure is a better and more optimal way, for now do it through temporary file "buffer"
            tmpfile = tempfile.NamedTemporaryFile()
            tmpfilepath = pathlib.Path(tmpfile.name)
            for file in exp_engine.glob(["**/*"]):
                exp_engine.get_file(file, tmpfilepath)
                exp_target_storage.put_file(file, tmpfilepath)

            # All data is transfered - purge the source storage    
            exp_engine.purge()

        except Exception as e:
            # Return state back to archivation requested
            exp_engine.exp.exp_api.patch_experiment({
                "Storage": {
                    "State": experiment.StorageState.ARCHIVATION_REQUESTED.value
                }
            })
            exp_engine.logger.exception(f"Failed to archive data: {e}")
            return
        finally:
            tmpfile.close()

        # Update experiment storage info with new access storage
        exp_engine.exp.exp_api.patch_experiment({
            "Storage": {
                "State": experiment.StorageState.ARCHIVED.value,
                **exp_target_storage.get_access_info()
            }
        })

        # Send notification email that the archivation was done 
        exp_engine.exp.exp_api.send_email(exp_engine.e_config["DataArchived"])