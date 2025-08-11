""" Periodically schedulabe service to clean up storage experiment data which is expired."""
import experiment
from experiment import ExperimentStorageEngine, OperationState, Operations


class DataExpirationService(experiment.ExperimentModuleBase):
    def provide_experiments(self):
        return (experiment.ExperimentsApi(self._api_session)
                .get_experiments_by_operation_states([(Operations.EXPIRATION, OperationState.REQUESTED)]))
    
    def step_experiment(self, exp_engine: ExperimentStorageEngine):

        if not exp_engine.is_accessible():
            return
        
        exp_engine.exp.storage.expiration_operation.run_operation(self.module_config.node_name)

        # Purge data
        try:
            exp_engine.purge()
        except Exception as e:
            # Return state back to expiration requested
            exp_engine.logger.error(f"Failed to purge data: {e}")
            exp_engine.exp.storage.expiration_operation.fail_operation(self.module_config.node_name)
            return

    
        # Finish operation
        exp_engine.exp.storage.expiration_operation.finish_operation(self.module_config.node_name)

        # Send notification email that the expiration was done 
        email_conf = self.module_config.lims_config.get_experiment_config(exp_engine.exp.instrument, exp_engine.exp.technique)["DataExpired"]
        exp_engine.exp.exp_api.send_email(email_conf)