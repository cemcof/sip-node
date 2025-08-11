from cemproc.tomo import CemcofTomoWorkflow
from common import LmodEnvProvider, exec_state, StateObj, parse_timedelta, DictArgWrapper
from configuration import LimsModuleConfigWrapper
from experiment import ExperimentModuleBase, ProcessingState, ExperimentStorageEngine, ExperimentsApi
from processing_tools import EmMoviesHandler
#
# class Runner:
#     pass
#
# class ThreadRunner(Runner):
#     def __init__(self):
#         pass
#
#
# class PbsRunner(Runner):
#     pass



class CemprocProcessingHandler(ExperimentModuleBase):
    EXEC_CACHE = {}

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
        # iconf = self.module_config.get("imod")
        cconf = self.module_config

        timeout_delta = parse_timedelta(cconf.get("processing_timeout", "00:10:00.0"))

        # For now (dirty way) our engine supports only tomo WF
        if not "tomo" in exp_engine.exp.processing.processing_data["WorkflowRef"]:
            return

        arguments = DictArgWrapper(
            {
                "source_dir": exp_engine.resolve_target_location(),
                "working_dir": exp_engine.resolve_target_location() or cconf["working_dir"],
                "lmod_path": cconf["lmod_path"],
                "movie_patterns": exp_engine.data_rules.get_target_for("raw", "movie"),
                "gain_patterns": exp_engine.data_rules.get_target_for("raw", "gain"),
                "run_mode": "single",
            },
            exp_engine.exp.processing.workflow
        )

        tomo_workflow = CemcofTomoWorkflow(arguments, exp_engine.logger)

        print("Processing cemproc! ")

        def to_run():
            exp_engine.exp.processing.state = ProcessingState.RUNNING

        def running():
            tomo_workflow.run_single()

        def finalizing():
            exp_engine.exp.processing.state = ProcessingState.COMPLETED

        def stop_requested():
            exp_engine.exp.processing.state = ProcessingState.FINALIZING

        exec_state(exp_engine.exp.processing,
       {
           ProcessingState.UNINITIALIZED: to_run,
           ProcessingState.READY: to_run,
           ProcessingState.RUNNING: running,
           ProcessingState.STOP_REQUESTED: stop_requested,
           ProcessingState.FINALIZING: finalizing,
           ProcessingState.COMPLETED: lambda: None,
           ProcessingState.DISABLED: lambda: None,
       }
       )

