from cemproc.tomo import TomoSession, TomoProcessor
from common import LmodEnvProvider, exec_state, StateObj, parse_timedelta
from configuration import LimsModuleConfigWrapper
from experiment import ExperimentModuleBase, ProcessingState, ExperimentStorageEngine, ExperimentsApi
from processing_tools import EmMoviesHandler

class Runner:
    pass

class ThreadRunner(Runner):
    def __init__(self):
        pass


class PbsRunner(Runner):
    pass






class CemprocProcessor(StateObj):
    def __init__(self, exp_engine: ExperimentStorageEngine, config: LimsModuleConfigWrapper, session):
        self.exp_engine = exp_engine
        self.session = session
        self.config = config

    def get_state(self):
        return self.exp_engine.exp.processing.state


    def uninitialized(self):
        pass

    def running(self):
        # Fetch files from storage to preocessing location and upload back new files
        pass

    def finalizing(self):
        pass


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
        iconf = self.module_config.get("imod")
        cconf = self.module_config
        lmod = LmodEnvProvider(cconf['lmod_path'])

        timeout_delta = parse_timedelta(cconf.get("processing_timeout", "00:10:00.0"))

        # For now (dirty way) our engine supports only tomo WF
        if not "tomo" in exp_engine.exp.processing.processing_data["WorkflowRef"]:
            return

        metadata = exp_engine.exp.processing.workflow
        working_dir = exp_engine.resolve_target_location() or cconf["working_dir"]
        tomo_session = TomoSession(
            TomoProcessor(working_dir, exp_engine.logger, lmod, **metadata),
            source_dir=working_dir,
            exp_storage=exp_engine)

        tomo_session.run()

        # processor = CemprocProcessor(exp_engine, cconf, lmod)
        # processor.exec_state()


        def uninitialized():

            exp_engine.exp.processing.state = ProcessingState.READY
        #
        # em_handler = EmMoviesHandler(storage_engine=exp_engine)
        # mov, meta, gain = em_handler.find_movie_information()
        # if not mov:
        #     return
        # if gain:
        #     gain = working_dir / em_handler.convert_gain_reference(gain)


        print("Processing cemproc! ")


        def to_run():
            exp_engine.exp.processing.state = ProcessingState.RUNNING

        def invalid_state():
            raise Exception("Invalid state")

        def running():
            tomo_session.run()

        def finalizing():
            pass

        exec_state(exp_engine.exp.processing,
       {
           ProcessingState.UNINITIALIZED: cw.create_project,
           ProcessingState.READY: cw.run_project,
           ProcessingState.RUNNING: running,
           ProcessingState.STOP_REQUESTED: cw.stop_project,
           ProcessingState.FINALIZING: finalizing,
           ProcessingState.COMPLETED: lambda: None,
           ProcessingState.DISABLED: lambda: None,
       }
       )

