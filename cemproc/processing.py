from experiment import ExperimentModuleBase, ProcessingState, ExperimentStorageEngine, ExperimentsApi


class CemprocProcessingHandler(ExperimentModuleBase):

    def provide_experiments(self):
        exps = ExperimentsApi(self._api_session).get_experiments_by_states(
            processing_state=[
                ProcessingState.UNINITIALIZED,
                ProcessingState.READY,
                ProcessingState.RUNNING,
                ProcessingState.STOP_REQUESTED,
                ProcessingState.FINALIZING]
        )

        return filter(lambda e: e.processing.engine == "cemproc" and (
                    e.processing.node_name == "any" or e.processing.node_name == self.module_config.lims_config.node_name),
                      exps)

    def step_experiment(self, exp_engine: ExperimentStorageEngine):
        print("Processing cemproc! ")
        pass