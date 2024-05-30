""" Periodically schedulabe service to clean up storage experiment data which is expired."""
import experiment
from experiment import ExperimentStorageEngine

class DataExpirationService(experiment.ExperimentModuleBase):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments_by_states(storage_state=experiment.StorageState.EXPIRATION_REQUESTED)
    
    def step_experiment(self, exp_engine: ExperimentStorageEngine):

        if not exp_engine.is_accessible():
            return
        
        # Set state that we are expring this experiment 
        exp_engine.exp.exp_api.patch_experiment({
            "Storage": {
                "State": experiment.StorageState.EXPIRING.value
            }
        })

        # Purge data
        try:
            exp_engine.purge()
        except Exception as e:
            # Return state back to expiration requested
            exp_engine.exp.exp_api.patch_experiment({
                "Storage": {
                    "State": experiment.StorageState.EXPIRATION_REQUESTED.value
                }
            })
            exp_engine.logger.error(f"Failed to purge data: {e}")
            return

    
        # Update experiment storage info to indicate there is no data, change status to expired
        exp_engine.exp.exp_api.patch_experiment({
            "Storage": {
                "State": experiment.StorageState.EXPIRED.value,
                "Target": None,
                "Path": None,
                "Token": None
            }
        })

        # Send notification email that the expiration was done 
        email_conf = self.module_config.lims_config.get_experiment_config(exp_engine.exp.instrument, exp_engine.exp.technique)["DataExpired"]
        exp_engine.exp.exp_api.send_email(email_conf)