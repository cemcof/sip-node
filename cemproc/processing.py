from cemproc.tomo import TomoSession
from common import LmodEnvProvider, exec_state
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
        iconf = self.module_config.get("imod")
        cconf = self.module_config["working_dir"]
        lmod = LmodEnvProvider(cconf['lmod_path'])

        print("Processing cemproc! ")

        # For now (dirty way) our engine supports only tomo WF
        if not "TOMO" in exp_engine.exp.processing.workflow["Tags"]:
            return

        working_dir = exp_engine.resolve_target_location() or cconf["working_dir"]

        metadata = {}

        tomo_session = TomoSession(source_dir=working_dir, working_dir=working_dir, lmod_env_provider=lmod, **metadata)


        def invalid_state():
            raise Exception("Invalid state")

        def running():
            pass

        def finalizing():
            pass

        exec_state(exp_engine.exp.processing,
           {
               ProcessingState.UNINITIALIZED: tomo_session.create_project,
               ProcessingState.READY: invalid_state,
               ProcessingState.RUNNING: running,
               ProcessingState.STOP_REQUESTED: cw.stop_project,
               ProcessingState.FINALIZING: finalizing,
               ProcessingState.COMPLETED: lambda: None,
               ProcessingState.DISABLED: lambda: None,
           }
           )

        meta = exp_engine.read_metadata()
        wf.
        pass