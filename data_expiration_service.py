""" Periodically schedulabe service to clean up storage experiment data which is expired."""
import experiment
from experiment import ExperimentStorageEngine

class DataExpirationService(experiment.ExperimentModuleBase):
    def provide_experiments(self):
        return experiment.ExperimentsApi(self._api_session).get_experiments(
            {"storageState": experiment.StorageState.EXPIRATION_REQUESTED.value}
        )
    
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
        exp_engine.exp.exp_api.send_email(exp_engine.e_config["DataExpired"])