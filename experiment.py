import fnmatch
import logging
import pathlib
import re

import yaml
import common, configuration
import json
import datetime
import requests

import data_tools
import logger_db_api
import datetime
import enum
import uuid
import threading
import inspect, tempfile
from typing import List, Union, Tuple
from data_tools import DataRulesSniffer, DataRulesWrapper, DataRule, MetadataModel, TransferAction, TransferCondition, \
    list_directory, DataAsyncTransferer


class JobState(enum.Enum):
    IDLE = "Idle"
    START_REQUESTED = "StartRequested"
    ACTIVE = "Active"
    STOP_REQUESTED = "StopRequested"
    FINISHED = "Finished"

class ProcessingState(enum.Enum):
    UNINITIALIZED = "Uninitialized"
    READY = "Ready"
    RUNNING = "Running"
    STOP_REQUESTED = "StopRequested"
    FINALIZING = "Finalizing"
    COMPLETED = "Completed"
    DISABLED = "Disabled"

class StorageState(enum.Enum):
    NONE = "None"
    UNINITIALIZED = "Uninitialized"
    IDLE = "Idle"
    TRANSFER_START_REQUESTED = "TransferStartRequested"
    TRANSFERING = "Transfering"
    TRANSFER_STOP_REQUESTED = "TransferStopRequested"
    ARCHIVATION_REQUESTED = "ArchivationRequested"
    ARCHIVING = "Archiving"
    ARCHIVED = "Archived"

class PublicationState(enum.Enum):
    UNPUBLISHED = "Unpublished"
    DRAFT_CREATION_REQUESTED = "DraftCreationRequested"
    DRAFT_CREATED = "DraftCreated"
    DRAFT_REMOVAL_REQUESTED = "DraftRemovalRequested"
    PUBLICATION_REQUESTED = "PublicationRequested"
    PUBLISHED = "Published"

class OperationState(enum.Enum):
    NOT_APPLICABLE = "NotApplicableOperation"
    NOT_SCHEDULED = "NotScheduledOperation"
    SCHEDULED = "ScheduledOperation"
    REQUESTED = "RequestedOperation"
    RUNNING = "RunningOperation"
    FINISHED = "FinishedOperation"

class Operations(enum.Enum):
    EXPIRATION = "ExpirationOperation"
    ARCHIVATION = "ArchivationOperation"
    PUBLICATION = "PublicationOperation"


class ExperimentApi:
    def __init__(self, exp_id, http_session: requests.Session):
        self.exp_id = exp_id
        self._http_session = http_session

    @property
    def session(self):
        return self._http_session

    def exp_url_base(self):
        return f"experiments/{self.exp_id}"
        
    def get_experiment(self):
        result = self._http_session.get(f"experiments/{self.exp_id}")
        return result.json()
    
    def patch_experiment(self, data):
        """ Data is just a dictionary with properties to be replaced to new values, possibly nested,
            convert it to a new data structure in format of "json patch" standard, which will be sent to the server"""
        json_patch = common.dict_to_json_patch(data)
        result = self._http_session.patch(f"experiments/{self.exp_id}", json=json_patch, headers={"Content-Type": "application/json-patch+json"})

    def change_state(self, state):
        state_map = {
            JobState: {"State": state.value},
            ProcessingState: {"Processing": {"State": state.value}},
            StorageState: {"Storage": {"State": state.value}},
            PublicationState: {"Publication": {"State": state.value}}
        }

        # Now patch experiment accrding to state param
        state_patch_data = state_map[type(state)]
        self.patch_experiment(state_patch_data)

    def upload_document_files(self, document_id, files, append=False):
        files = files if isinstance(files, list) else [files]
        # Files is a list of tuples compatible with requests module 
        files_for_request = [("files", f) for f in files]
        self._http_session.post(f"documents/{document_id}/files", files=files_for_request, params={"append": append})

    def send_email(self, email):
        result = self._http_session.post(f"experiments/{self.exp_id}/email", json=email)



class OperationWrapper:
    def __init__(self, op_name: str, data, api: ExperimentApi = None):
        self.data = data
        self.name = op_name
        self.api = api

    def is_in(self, state: OperationState):
        return self.data["$type"] == state.value

    def run_operation(self, node_name: str):
        result = self.api.session.post(f"{self.api.exp_url_base()}/operations/{self.name}/run", params={"node": node_name})
        result.raise_for_status()
        self.data = result.json()

    def finish_operation(self, node_name: str):
        result = self.api.session.post(f"{self.api.exp_url_base()}/operations/{self.name}/finish", params={"node": node_name})
        result.raise_for_status()
        self.data = result.json()

    def fail_operation(self, node_name: str, request_again: bool = True):
        result = self.api.session.post(f"{self.api.exp_url_base()}/operations/{self.name}/fail", params={"node": node_name, "request_again": request_again})
        result.raise_for_status()
        self.data = result.json()

class ExperimentProcessingWrapper(common.StateObj):
    def __init__(self, processing_data, exp_api: ExperimentApi):
        self.processing_data = processing_data
        self.exp_api = exp_api

    def find_protocol_by_name(self, name):
        return next(filter(lambda p: p["TYPE"] == name, self.workflow), None)
    
    @property
    def engine(self):
        return self.processing_data["ProcessingEngine"]
    
    @property
    def pid(self):
        return self.processing_data["Pid"]
    
    @pid.setter
    def pid(self, value):
        self.exp_api.patch_experiment({ "Processing": {"Pid": str(value) }})
        self.processing_data["Pid"] = str(value)

    @property
    def state(self):
        return ProcessingState(self.processing_data["State"])
    
    @state.setter
    def state(self, value: ProcessingState):
        if (self.state != value):
            self.exp_api.patch_experiment({"Processing": {"State": value.value}})
            self.processing_data["State"] = value

    @property
    def last_update(self):
        return common.parse_date(self.processing_data["DtLastUpdate"])

    @last_update.setter
    def last_update(self, value: datetime.datetime):
        strd = common.stringify_date(value)
        self.exp_api.patch_experiment({"Processing": {"DtLastUpdate": strd}})
        self.processing_data["DtLastUpdate"] = strd
    
    @property
    def node_name(self):
        return self.processing_data["Node"]
    
    @node_name.setter
    def node_name(self, value: str):
        if (value != self.node_name):
            self.exp_api.patch_experiment({"Processing": {"Node": value}})
            self.processing_data["Node"] = value

    
    @property
    def workflow(self):
        return self.processing_data["Workflow"]

    @property
    def result_document(self):
        return ExperimentDocumentWrapper(self.processing_data["ResultReport"], self.exp_api)
    
    @property
    def log_document(self):
        return ExperimentDocumentWrapper(self.processing_data["LogReport"], self.exp_api)

    def get_state(self):
        return self.state


# ------------- Experiment storage ----------------
class ExperimentDataSourceWrapper:
    def __init__(self, data: dict, exp_api: ExperimentApi):
        self._data = data
        self.exp_api = exp_api

    @property
    def source_directory(self):
        source_dir_str = self._data["SourceDirectory"]
        return common.path_universal_factory(source_dir_str) if source_dir_str else None
    
    @source_directory.setter
    def source_directory(self, value: pathlib.Path):
        value = str(value) if value else None
        self.exp_api.patch_experiment({"DataSource": {"SourceDirectory": value}})
        self._data["SourceDirectory"] = value

    @property
    def source_patterns(self):
        pattlist = self._data["SourcePatterns"]
        for p in pattlist:
            # Current policy is: if pattern does not contain any / nor *, consider it as a full glob
            # and prepend **/* to it
            yield p if "/" in p or "*" in p else "*" + p
    
    @property
    def clean_after(self):
        return common.parse_timedelta(self._data["CleanAfter"])
    
    @property
    def keep_source_files(self):
        return self._data["KeepSourceFiles"]
    
    def mark_cleaned(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        self.exp_api.patch_experiment({
            "DataSource": {
                "DtCleaned": common.stringify_date(now), 
                }
                })
        
        self._data["DtCleaned"] = now
    
    def get_combined_raw_datarules(self, raw_rules, keep_source_files=False):
        trans_action = TransferAction.MOVE if not keep_source_files else TransferAction.COPY
        raw_rules = DataRulesWrapper(raw_rules.data_rules + [DataRule(p, ["raw"], ".", True, action=trans_action, condition=TransferCondition.IF_MISSING) for p in self.source_patterns])
        # If keeping files on the instrument is requested, use copy action on all, otherwise leave default configured values
        if self.keep_source_files:
            for r in raw_rules:
                r.action = TransferAction.COPY

        return raw_rules

class ExperimentStorageWrapper:
    def __init__(self, exp_data: dict, exp_api: ExperimentApi) -> None:
        self._exp_data = exp_data
        self.exp_api = exp_api

    @property
    def engine(self):
        return self._exp_data["StorageEngine"]
    
    @property 
    def dt_last_updated(self):
        return common.parse_date(self._exp_data["DtLastUpdate"])
    
    @dt_last_updated.setter
    def dt_last_updated(self, value: datetime.datetime):
        strd = common.stringify_date(value)
        self.exp_api.patch_experiment({"Storage": {"DtLastUpdate": strd}})
        self._exp_data["DtLastUpdate"] = strd


    @property
    def expiration_operation(self):
        return OperationWrapper("ExpirationOperation", self._exp_data["ExpirationOperation"], self.exp_api)

    @property
    def target(self):
        return self._exp_data["Target"]
    
    @property
    def path(self):
        return self._exp_data["Path"]
    
    @property
    def subpath(self):
        return self._exp_data["SubPath"]    
    
    @property
    def token(self):
        return self._exp_data["Token"]
    

    @property
    def state(self):
        return StorageState(self._exp_data["State"])
    
    @state.setter
    def state(self, value: StorageState):
        if (self.state != value):
            self.exp_api.patch_experiment({"Storage": {"State": value.value}})
            self._exp_data["State"] = value.value


class ExperimentPublicationWrapper:
    def __init__(self, publication_data, exp_api: ExperimentApi):
        self._publication = publication_data
        self.exp_api = exp_api

    @property
    def engine(self):
        return self._publication["PublicationEngine"]
    
    @property
    def draft_id(self):
        return self._publication["RecordId"] if "RecordId" in self._publication else None
    
    @draft_id.setter
    def draft_id(self, value: str):
        self.exp_api.patch_experiment_publication({"Publication": {"RecordId": value}})
        self._publication["RecordId"] = value

    @property
    def state(self):
        return PublicationState(self._publication["State"])
    
    @state.setter
    def state(self, value: PublicationState):
        if (self.state != value):
            self.exp_api.patch_experiment_publication({"Publication": {"State": str(value)}})
            self.reload()

    @property
    def operation(self):
        return OperationWrapper(self._publication["PublicationOperation"])


class ExperimentWrapper:
    def __init__(self, experiment_api: ExperimentApi, data=None):
        self._data = data
        self.exp_api = experiment_api

        self._lastexpfetch = None
        self._laststatusfetch = None

        if data:
            self._lastexpfetch = datetime.datetime.utcnow()
            self._laststatusfetch = datetime.datetime.utcnow()

    def reload(self):
        self._data = self.exp_api.get_experiment()
        self._lastexpfetch = datetime.datetime.utcnow()
        self._laststatusfetch = datetime.datetime.utcnow()

    @property
    def state(self):
        return JobState(self._data["State"])
    
    @state.setter
    def state(self, value: JobState):
        if (self.state != value):
            self.exp_api.change_state(value)
            self.reload()


    @property
    def data_source(self):
        return ExperimentDataSourceWrapper(self._data["DataSource"], self.exp_api)
    
    @property
    def storage(self):
        return ExperimentStorageWrapper(self._data["Storage"], self.exp_api)

    @property    
    def processing(self):
        return ExperimentProcessingWrapper(self._data["Processing"], self.exp_api)
    
    @property
    def publication(self):
        return ExperimentPublicationWrapper(self._data["Publication"], self.exp_api)

    @property
    def secondary_id(self):
        return self._data["SecondaryId"]
    
    @property
    def user_type(self):
        return self._data["UserType"]
    
    @property
    def dt_created(self):
        return common.parse_date(self._data["DtCreated"]) 

    @property
    def id(self):
        return self._data["Id"]
    
    @property
    def technique(self):
        return self._data["Technique"]
    
    @property
    def instrument(self):
        return self._data["InstrumentName"]
    
    @property
    def exp_type(self):
        return self.instrument + "/" + self.technique 

    @property
    def notify_user(self):
        return self._data["NotifyUser"]
    
    @property
    def data_model(self):
        # TODO - attach flat metadata somehow as well?
        return self._data
    
    def uuid_for(self, target: str):
        """
        This method generates an UUID for this experiment and given "target".
        Should always generate same UUID for particular experiment-target combinations.
        Uses uuid version 5 feature
        This is useful for example for generating a log id for logs that are meant to be replaced (e.g status logs)
        """
        return uuid.uuid5(uuid.UUID(self.id), target)
    
    def exp_type_matches(self, patterns=[]):
        if isinstance(patterns, str):
            patterns = [patterns]
        for p in patterns:
            if re.match(p, self.exp_type):
                return True
        return False

class ExperimentsApi:
    def __init__(self, http_session: requests.Session) -> None:
        self._http_session = http_session

    def get_active_experiments(self):
        return self.get_experiments_by_states(exp_state=JobState.ACTIVE)
        
    def get_experiments(self, queryData=None, subpath=None):
        queryData = queryData or {}
        path = "experiments" if subpath is None else f"experiments/{subpath}"
        result = self._http_session.get(path, params=queryData)
        expData = result.json()
        return [ExperimentWrapper(self.for_experiment(x["Id"]), x) for x in expData]

    def get_experiments_by_operation_states(self, operations: List[Tuple[Operations, OperationState]]):
        qrData = [(x.value, y.value) for x, y in operations]
        exps = self.get_experiments(queryData=qrData, subpath="by_operation")
        return exps

    def get_experiments_by_states(self, exp_state: Union[JobState, List[JobState], None]=None,
                                        storage_state: Union[StorageState, List[StorageState], None]=None,
                                        processing_state: Union[ProcessingState, List[ProcessingState], None]=None,
                                        publication_state: Union[PublicationState, List[PublicationState], None]=None):
        queryData = {}
        if exp_state:
            queryData["expState"] = exp_state.value if isinstance(exp_state, JobState) else ",".join([s.value for s in exp_state])
        if storage_state:
            queryData["storageState"] = storage_state.value if isinstance(storage_state, StorageState) else ",".join([s.value for s in storage_state])
        if processing_state:
            queryData["processingState"] = processing_state.value if isinstance(processing_state, ProcessingState) else ",".join([s.value for s in processing_state])
        if publication_state:
            queryData["publicationState"] = publication_state.value if isinstance(publication_state, PublicationState) else ",".join([s.value for s in publication_state])
        
        return self.get_experiments(queryData)

    def for_experiment(self, id):
        return ExperimentApi(id, self._http_session)

class ExperimentDocumentWrapper:
    def __init__(self, doc_data, exp_api: ExperimentApi):
        self._doc_data = doc_data
        self.exp_api = exp_api

    @property
    def id(self):
        return self._doc_data["Id"]
    
    @property 
    def primary_file_metadata(self):
        files = self._doc_data["FilesInDocuments"]
        # Find file that is primary
        prim_file = next(filter(lambda f: f["DocumentFileType"] == "Primary", files), None)

        if prim_file is None:
            return None
        
        return prim_file["FileMetadata"]
    
    @property
    def primary_file_lastmodified(self):
        """
        Get modification/update datetime of document's primary file, if any
        """
        prim_file = self.primary_file_metadata
        if prim_file is None:
            return None
        
        return common.parse_date(prim_file["DtModified"])
    
    def upload_files(self, data, append=False):
        return self.exp_api.upload_document_files(self.id, data, append=append)

class ExperimentStorageEngine(data_tools.DataTransferSource):
    def __init__(self, 
                 experiment: ExperimentWrapper, 
                 logger: logging.Logger,
                 data_rules: DataRulesWrapper,
                 metadata_model: Union[dict, MetadataModel],

                 metadata_target="experiment.yml"
                 ) -> None:
        self.exp = experiment
        self.logger = logger
        self.metadata_target = pathlib.Path(metadata_target)
        self.data_rules = data_rules
        self.metadata_model = metadata_model if isinstance(metadata_model, MetadataModel) else MetadataModel(metadata_model)

    def is_accessible(self):
        """ Check if the storage is accessible from current node with current configuration """
        raise NotImplementedError()
    
    def prepare(self):
        """ Prepare the storage for the experiment """
        raise NotImplementedError()
    
    def get_access_info(self):
        """ Get data access information for the storage """
        raise NotImplementedError()
    
    def has_same_location(self, other: 'ExperimentStorageEngine'):
        """ Check if the storage has the same location as other storage """
        first_loc = self.resolve_target_location()
        second_loc = self.resolve_target_location()
        return first_loc is not None and second_loc is not None and first_loc == second_loc
    
    def resolve_target_location(self, src_relative: pathlib.Path=None) -> pathlib.Path:
        raise NotImplementedError()
        
    def metadata_exists(self):
        return self.file_exists(self.metadata_target)
    
    def read_metadata(self):
        if not self.metadata_exists():
            return {}
        
        metastr: str = self.read_file(self.metadata_target)
        return yaml.safe_load(
             metastr
        )

    def get_tag_target_dir(self, *tags):
        dr = self.data_rules.with_tags(*tags)
        if not dr: 
            raise ValueError(f"No target configured for tags: {tags}")
        return dr.data_rules[0].target
    
    def file_exists(self, path_relative: pathlib.Path):
        raise NotImplementedError()
    
    def glob(self, data_rules: DataRulesWrapper=None):
        raise NotImplementedError()
    
    def read_file(self, path_relative: pathlib.Path, as_text=True):
        raise NotImplementedError()
    
    def write_file(self, path_relative: pathlib.Path, content):
        raise NotImplementedError()
    

    def put_file(self, path_relative: pathlib.Path, src_file: pathlib.Path, condition: TransferCondition = TransferCondition.IF_MISSING):
        """ Put file from file system to the storage """
        raise NotImplementedError()
    
    def get_file(self, path_relative_src: pathlib.Path, path_dst: pathlib.Path):
        """ Get file from storage to file system """
        raise NotImplementedError()
    
    def del_file(self, path_relative: pathlib.Path):
        """ Delete file from storage """
        raise NotImplementedError()
    
    def purge(self):
        """ Purge all data from the storage """
        raise NotImplementedError()
    
    def upload_raw(self, source_path):
        source_path = pathlib.Path(source_path)
        raw_rules = self.data_rules.with_tags("raw")
        # Add raw files specified by user on the experiment 
        raw_rules = self.exp.data_source.get_combined_raw_datarules(raw_rules)
        return self.upload(source_path, raw_rules, session_name="raw")

    def upload(self, source: pathlib.Path, rules: configuration.DataRulesWrapper, session_name=None):

        transferer = DataAsyncTransferer(data_tools.FsTransferSource(source), self, rules,
                                         f"{session_name}_{self.exp.secondary_id}_{int(self.exp.dt_created.timestamp())}",
                                         self.logger)
        result = transferer.transfer()
        return result

    def download(self, target: pathlib.Path, data_rules: configuration.DataRulesWrapper = None, session_name=None):
        data_rules = data_rules or self.data_rules
        transferer = DataAsyncTransferer(self, data_tools.FsTransferSource(target), data_rules, f"{session_name}_{self.exp.secondary_id}")
        return transferer.transfer()

    def transfer_to(self, target: 'ExperimentStorageEngine', data_rules: configuration.DataRulesWrapper=None, session_name=None, transfer_action: TransferAction=TransferAction.COPY):
        """ Transfer data from this storage to another """
        if data_rules is None:
            data_rules = DataRulesWrapper([DataRule("**/*", ["all"], keep_tree=True, condition=TransferCondition.ALWAYS)])

        transferer = DataAsyncTransferer(self, target, data_rules, f"{session_name}_{self.exp.secondary_id}")
        result = transferer.transfer()
        return result

    def sniff_and_process_metafile(self, source_path):
        source_path = pathlib.Path(source_path)
        meta_rules = self.data_rules.with_tags("metadata")
        def sniff_consumer(source_path: pathlib.Path, data_rule: DataRule):
            self.restore_metadata(yaml.full_load(source_path.read_text()))
            source_path.unlink()

        sniffer = DataRulesSniffer(source_path, meta_rules, sniff_consumer, None, min_nochange_sec=0)
        sniffer.sniff_and_consume()

    def download_raw(self, target: pathlib.Path):
        dr = DataRule('Raw/**/*.*', "raw", keep_tree=True)
        dw_result, errs = self.download(target, DataRulesWrapper([dr]), session_name="cs_raw_download")
        return dw_result, errs
    
    def upload_processed(self, source: pathlib.Path, target: pathlib.Path):
        dr = DataRule('**/*.*', "processed", target=target, keep_tree=True, condition=TransferCondition.IF_NEWER)
        up_result, errs = self.upload(source, DataRulesWrapper([dr]), session_name="cs_processed_upload")
        return up_result, errs
    
    def extract_metadata(self):
        """ Using the configured metadata model, extract metadata from the sources"""
        def from_exp_source(path: str):
            return common.get_dict_val_by_path(self.exp.data_model, path)

        def from_processing_source(key: str):
            return next(common.search_for_key(self.exp.processing.workflow, key), None)

        metad = self.metadata_model.extract_metadata({
            "exp": from_exp_source, 
            "processing": from_processing_source
            })
        return metad

    def restore_metadata(self, metadata={}):
        """ Restore metadata from multiple sources (by given precedence, later overrides earlier):
            - From experiment data
            - From current metadata file (if exists)
            - From given metadata parameter, if any
        """

        # Read metadata from file
        metad = {}
        # Read metadata from experiment
        extracted_meta = self.extract_metadata()
        metad.update({ k: v for k, v, _ in extracted_meta })

        if self.metadata_exists():
            try:
                metad_yaml = self.read_file(self.metadata_target)
                metad_parsed = yaml.safe_load(metad_yaml)
                if not isinstance(metad_parsed, dict):
                    raise Exception("Metadata file does not contain a dictionary")
                metad = metad_parsed # TODO - is this a bug? 
            except:
                self.logger.warning(f"Failed to read metadata from {self.metadata_target}")


        # Read metadata from parameter
        metad.update(metadata)

        # Write metadata
        metad_yaml = yaml.dump(metad)
        # TODO - submit metadata to sip experiment
        self.write_file(self.metadata_target, metad_yaml)

        return metad

    

class ExperimentModuleBase(configuration.LimsNodeModule):
    def __init__(self, name, logger, lims_logger, config: configuration.LimsModuleConfigWrapper, api_session, exp_storage_engine_factory):
        super().__init__(name, logger, lims_logger, config, api_session)
        self.exp_storage_engine_factory = exp_storage_engine_factory
        self.exec_state = {}

    def _get_experiment_storage_engine(self, e: ExperimentWrapper):
        exp_logger = logger_db_api.experiment_logger_adapter(self._lims_logger, e.id)
        exp_config = self.module_config.lims_config.get_experiment_config(e.instrument, e.technique)
         
        return self.exp_storage_engine_factory(e, exp_config, exp_logger, self.module_config)

    @property
    def parallel(self):
        str_val = self.module_config.get("parallel", 0)
        return int(str_val)

    @property
    def is_parallel(self):
        return self.parallel > 0 or self.parallel == -1
    
    def _clean_finished_threads(self):
        for exp_id, thrd in list(self.exec_state.items()):
            if not thrd.is_alive():
                self.logger.debug(f"Finished thread for experiment {exp_id}")
                del self.exec_state[exp_id]
    
    def step(self):
        self._clean_finished_threads()
        
        experiments = self.provide_experiments()

        def step_exp_helper(exp_engine):
            try:
                self.step_experiment(exp_engine)
            except Exception as e:
                self.logger.exception(e)

        def wrap_step_parallel(exp_engine):
            if len(self.exec_state) >= self.parallel != -1:
                return

            if exp_engine.exp.id in self.exec_state:
                return

            thread = threading.Thread(target=step_exp_helper, args=(exp_engine,))
            thread.start()
            self.exec_state[exp_engine.exp.id] = thread

        for e in experiments:
            try:
                exp_engine = self._get_experiment_storage_engine(e)
            except Exception as e:
                self.logger.error("Could not create storage engine for experiment, skipping")
                self.logger.exception(e)
                continue
            
            if self.is_parallel:
                wrap_step_parallel(exp_engine)
            else:
                step_exp_helper(exp_engine)
                
    def step_experiment(self, exp_engine: ExperimentStorageEngine):
        pass

    def provide_experiments(self):
        return ExperimentsApi(self._api_session).get_active_experiments()
    
    def get_experiment_config(self, exp: ExperimentWrapper):
        return self.module_config.lims_config.get_experiment_config(exp.instrument, exp.technique)
        

class ExpFileBrowser(configuration.LimsNodeModule):
   
    def step(self):
        path_requests: dict = self._api_session.get("fs").json()
        result = {} 
        for reqid, reqinfo in path_requests.items():
            path, scope = reqinfo["Path"], reqinfo["Scope"]
            if not scope:
                continue

            if scope == "autopicking":
                roots = [{"Path": self.module_config["AutopickingModelsPath"],
                          "Name": "Autopicking models"}]
                result_path = list_directory(path, roots, self.logger)
            else:
                inst, job = scope.split("/")
                drives = self.get_available_drives(inst, job, self.module_config)
                roots = [{"Path": d["Path"], "Name": d["Label"]} for d in drives]
                result_path = list_directory(path, roots, self.logger)
                
            result[reqid] = result_path

        if result:
            self._api_session.post("fs", json=result)

    @staticmethod
    def get_available_drives(instrument: str, job: str, conf):
        instrument = next(filter(lambda x: x["Name"] == instrument, conf["Instruments"]), None)
        if instrument:
            inst_drives = instrument["Drives"]
        else:
            # TODO - not this logger
            logging.warning(f"Attempting to get drives for unknown instrument {instrument}")
            return []

        drives = [d for d in inst_drives]
        return drives

