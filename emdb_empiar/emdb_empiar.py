import json
import pathlib
import tempfile
from dataclasses import dataclass

import jsonschema
import requests

import experiment
from experiment import Operations
from processing_tools import EmMoviesHandler, MovieFormat, VoxelType

try:
    from . import empiar_depositor_custom
except ImportError:
    import empiar_depositor_custom

STATE_FILE_DEFAULT = ".empiar_deposition.state.json"
STATE_VERSION = 1
STEP_BUILD_DEPOSITION = "build_deposition"
STEP_VALIDATE_GLOBUS = "validate_globus"
STEP_CREATE_DEPOSITION = "create_deposition"
STEP_UPLOAD_THUMBNAIL = "upload_thumbnail"
STEP_SHARE_UPLOAD = "share_upload_directory"
STEP_GLOBUS_UPLOAD = "globus_upload"
STEP_GLOBUS_UPLOAD_SUBMIT = "globus_upload_submit"
STEP_GLOBUS_UPLOAD_WAIT = "globus_upload_wait"
STEP_ACK_UPLOAD = "acknowledge_upload"
STEP_SUBMIT = "submit"
STEP_FINISH = "finish_operation"


@dataclass
class DepositionState:
    version: int = STATE_VERSION
    next_step: str = STEP_BUILD_DEPOSITION
    entry_id: str | None = None
    entry_directory: str | None = None
    globus_task_id: str | None = None
    empiar_accession: str | None = None

    def to_dict(self):
        return {
            "version": self.version,
            "next_step": self.next_step,
            "entry_id": self.entry_id,
            "entry_directory": self.entry_directory,
            "globus_task_id": self.globus_task_id,
            "empiar_accession": self.empiar_accession,
        }

    @staticmethod
    def from_dict(data: dict):
        return DepositionState(
            version=data.get("version", STATE_VERSION),
            next_step=data.get("next_step", STEP_BUILD_DEPOSITION),
            entry_id=data.get("entry_id"),
            entry_directory=data.get("entry_directory"),
            globus_task_id=data.get("globus_task_id"),
            empiar_accession=data.get("empiar_accession"),
        )

def experiment_type_selector(instrument, technique):
    # 3 - default
    # 4, 5 hydra?
    # 9 - diffraction tecnique
    if instrument.startswith('Hydra'):
        return 4
    return 3

def imageset_info(em_handler: EmMoviesHandler):
    # We start from raw data rule
    # Then check first movie - what do we have?
    # Error if not compatible data
    movie_info = em_handler.find_movie_information()
    if not movie_info:
        raise ValueError("No movie files found for this experiment")

    mov, met, _gain = movie_info
    if met is None:
        raise ValueError("Movie metadata file is required for EMPIAR deposition")

    mov_meta = em_handler.movie_metadata(mov, met)

    voxel_map = {
        VoxelType.UNSIGNED_BYTE: "('T1', '')",
        VoxelType.SIGNED_INT32: "('T6', '')"
    }

    format_map = {
        MovieFormat.TIFF: "('T3', '')",
        MovieFormat.MRC: "('T1', '')",
        MovieFormat.EER: "('T9', '')",
        MovieFormat.MRCS: "('T1', '')",
    }

    def category_selector():
        return "('T1', '')" if mov_meta.frame_count > 1 else "('T2', '')"

    return {
        "directory": "data/Movies", # TODO - this actually folder name?
        "category": category_selector(),
        "header_format": format_map[mov_meta.movie_format],
        "data_format": format_map[mov_meta.movie_format],
        "num_images_or_tilt_series": em_handler.count_movies(),
        "frames_per_image": mov_meta.frame_count,
        "voxel_type": voxel_map[mov_meta.voxel_type]
    }

def build_empiar_deposition_data(metadata, imageset, publication: experiment.ExperimentPublicationWrapper):
    draft_id = publication.draft_id
    if draft_id is None or not str(draft_id).strip():
        raise ValueError("Missing required EMPIAR cross reference draft_id")
    draft_id = str(draft_id).strip()

    user_simple = {
        "name": f"('{metadata['PI_last_name']}', '{''.join(part[0].upper() for part in metadata['PI_first_name'].split())}')",  # "('Chang', 'YW')"
        "order_id": 0,
        "author_orcid":  metadata.get("PI_orcid"),
    }

    user_complex = {
        "author_orcid": user_simple["author_orcid"],
        "first_name": metadata["PI_first_name"],
        "last_name": metadata["PI_last_name"],
        "email": metadata["PI_email"],
        "organization": metadata["PI_affiliation"],
        "country": metadata["PI_affiliation_country"]
    }

    proj_sam_tech = f"{metadata['SAMPLE_project_name']} - {metadata['SAMPLE_name']} - {metadata['DATA_experiment_type']}"

    imageset["name"] = proj_sam_tech
    imagesets = [imageset]

    empiar_deposition = {
        "title": metadata.get("SAMPLE_project_name") or proj_sam_tech,
        "release_date": "HP",  # or HO
        "experiment_type": experiment_type_selector(
            metadata["DATA_emMicroscopeId"], metadata["DATA_experiment_type"]
        ),
        "cross_references": [{"name": draft_id}],
        "biostudies_references": [] if not metadata.get("SAMPLE_reference") else [{"name": metadata["SAMPLE_reference"]}],
        "authors": [user_simple],
        "corresponding_author": user_complex,
        "principal_investigator": [user_complex],
        "imagesets": imagesets,
        "citation": [{
            "authors": [user_simple],
            "published": False,
            "j_or_nj_citation": True,
            "title": publication.name
        }]
    }

    # TODO - experiment type selector - jeste probrat s Jirkou
    # TODO - moznost archivace Transfer zrusit?
    # TODO - imagesets
    # TODO - workflowhub reference
    # TODO - workflow file v pripade scipionu
    # TODO - user middle name

    schema_path = pathlib.Path(__file__).parent / "empiar-schema.json"
    with schema_path.open("r", encoding="utf-8") as f:
        jsonschema.validate(instance=empiar_deposition, schema=json.load(f))

    return empiar_deposition


class EmdbEmpiarPublicationService(experiment.ExperimentModuleBase):

    def provide_experiments(self):
        valid_states = [
            (Operations.PUBLICATION, experiment.OperationState.REQUESTED),
            (Operations.PUBLICATION, experiment.OperationState.RUNNING)
        ]

        exps = (experiment.ExperimentsApi(self._api_session)
                .get_experiments_by_operation_states(valid_states))

        return filter(lambda e: e.publications.publication("empiar-emdb"), exps)

    def _state_file(self) -> pathlib.Path:
        return pathlib.Path(self.module_config.get("state_file", STATE_FILE_DEFAULT))

    def _load_state(self, exp_engine: experiment.ExperimentStorageEngine) -> DepositionState:
        state_file = self._state_file()
        if not exp_engine.file_exists(state_file):
            return DepositionState()

        state_raw = exp_engine.read_file(state_file)
        return DepositionState.from_dict(json.loads(state_raw))

    def _save_state(self, exp_engine: experiment.ExperimentStorageEngine, state: DepositionState):
        exp_engine.write_file(self._state_file(), json.dumps(state.to_dict(), indent=2))

    def _clear_state(self, exp_engine: experiment.ExperimentStorageEngine):
        state_file = self._state_file()
        if exp_engine.file_exists(state_file):
            exp_engine.del_file(state_file)

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        if isinstance(exc, empiar_depositor_custom.CliError):
            cli_exc: empiar_depositor_custom.CliError = exc
            transient_prefixes = ("E_API_", "E_GLOBUS_", "E_ACK_COMPLETION")
            return cli_exc.code.startswith(transient_prefixes)

        return isinstance(exc, (requests.RequestException, TimeoutError))

    def _data_path_for_upload(self, exp_engine: experiment.ExperimentStorageEngine) -> str:
        data_root = exp_engine.resolve_target_location()
        if data_root is None:
            raise ValueError("Storage engine does not provide local data path for Globus upload")
        return str(data_root)

    def _execute_step(
        self,
        step_name: str,
        state: DepositionState,
        exp_engine: experiment.ExperimentStorageEngine,
        publication_info: experiment.ExperimentPublicationWrapper,
        metadata: dict,
        em_handler: EmMoviesHandler,
    ):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = pathlib.Path(tmp_dir)
            json_input_path = tmp_dir_path / "empiar_deposition.json"
            thumb_path = tmp_dir_path / "thumbnail.png"

            deposit = build_empiar_deposition_data(metadata, imageset_info(em_handler), publication_info)
            json_input_path.write_text(json.dumps(deposit, indent=2), encoding="utf-8")

            globus_helper = empiar_depositor_custom.GlobusHelper(logger=exp_engine.logger)

            if step_name == STEP_VALIDATE_GLOBUS:
                globus_helper.validate_globus_details(
                    endpoint_search=self.module_config["globus_source_endpoint_id"],
                    endpoint_path=self._data_path_for_upload(exp_engine),
                )
                state.next_step = STEP_CREATE_DEPOSITION
                return

            environment = "production" if bool(self.module_config.get("production", False)) else "development"
            server_root = self.module_config.get(
                "server_root", empiar_depositor_custom.SETTINGS[environment]["server_root"]
            )
            destination_endpoint_id = self.module_config.get(
                "destination_endpoint_id", empiar_depositor_custom.SETTINGS[environment]["destination_endpoint_id"]
            )

            globus_helper.validate_globus_details(
                endpoint_search=self.module_config["globus_source_endpoint_id"],
                endpoint_path=self._data_path_for_upload(exp_engine),
            )
            em_handler.build_thumbnail(thumb_path)

            depositor = empiar_depositor_custom.EmpiarDepositor(
                empiar_auth_value=self.module_config["empiar_api_token"],
                json_input=str(json_input_path),
                server_root=server_root,
                data=self._data_path_for_upload(exp_engine),
                globus_source_endpoint=globus_helper.endpoint_id,
                ignore_certificate=bool(self.module_config.get("ignore_certificate", True)),
                entry_thumbnail=str(thumb_path),
                entry_id=state.entry_id,
                entry_directory=state.entry_directory,
                stop_submit=False,
                password=None,
                grant_rights_usernames=self.module_config.get("grant_rights_usernames"),
                grant_rights_emails=self.module_config.get("grant_rights_emails"),
                grant_rights_orcids=self.module_config.get("grant_rights_orcids"),
                globus_local_username=globus_helper.user_identity,
                log=exp_engine.logger,
            )

            if step_name == STEP_CREATE_DEPOSITION:
                if state.entry_id and state.entry_directory:
                    depositor.redeposit()
                else:
                    depositor.create_new_deposition()
                state.entry_id = depositor.entry_id
                state.entry_directory = depositor.entry_directory
                state.next_step = STEP_UPLOAD_THUMBNAIL
                return

            depositor.entry_id = state.entry_id
            depositor.entry_directory = state.entry_directory

            if step_name == STEP_UPLOAD_THUMBNAIL:
                depositor.thumbnail_upload()
                state.next_step = STEP_SHARE_UPLOAD
                return

            if step_name == STEP_SHARE_UPLOAD:
                depositor.share_upload_directory()
                state.next_step = STEP_GLOBUS_UPLOAD
                return

            if step_name == STEP_GLOBUS_UPLOAD_SUBMIT:
                state.globus_task_id = globus_helper.globus_upload_submit(
                    destination_directory=state.entry_directory,
                    destination_endpoint_id=destination_endpoint_id,
                    entry_reference=state.entry_id,
                )
                state.next_step = STEP_GLOBUS_UPLOAD_WAIT
                return

            if step_name == STEP_GLOBUS_UPLOAD_WAIT:
                if not state.globus_task_id:
                    raise ValueError("Missing Globus task id in state, cannot wait for upload")
                globus_helper.globus_upload_wait(state.globus_task_id)
                state.globus_task_id = None
                state.next_step = STEP_ACK_UPLOAD
                return

            if step_name == STEP_ACK_UPLOAD:
                if not depositor.acknowledge_completion():
                    raise empiar_depositor_custom.CliError(
                        code="E_ACK_COMPLETION",
                        step="empiar.acknowledge_upload",
                        message="Upload completion acknowledgement failed",
                    )
                state.next_step = STEP_SUBMIT
                return

            if step_name == STEP_SUBMIT:
                depositor.submit_deposition()
                state.empiar_accession = depositor.empiar_accession
                state.next_step = STEP_FINISH
                return

            raise ValueError(f"Unknown EMPIAR deposition step: {step_name}")


    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        if not exp_engine.is_accessible():
            exp_engine.logger.warning("Experiment storage is not accessible, postponing EMPIAR deposition")
            raise RuntimeError("Storage not accessible")

        publication_info = exp_engine.exp.publications.publication("empiar-emdb")
        operation = publication_info.operation

        if operation.is_in(experiment.OperationState.REQUESTED):
            operation.run_operation(self.module_config.node_name)

        metadata = exp_engine.read_metadata()
        state = self._load_state(exp_engine)
        em_handler = EmMoviesHandler(exp_engine)

        steps = [
            STEP_BUILD_DEPOSITION,
            STEP_VALIDATE_GLOBUS,
            STEP_CREATE_DEPOSITION,
            STEP_UPLOAD_THUMBNAIL,
            STEP_SHARE_UPLOAD,
            STEP_GLOBUS_UPLOAD_SUBMIT,
            STEP_GLOBUS_UPLOAD_WAIT,
            STEP_ACK_UPLOAD,
            STEP_SUBMIT,
            STEP_FINISH,
        ]

        try:
            # Backward compatibility with older state files.
            if state.next_step == STEP_GLOBUS_UPLOAD:
                state.next_step = STEP_GLOBUS_UPLOAD_WAIT if state.globus_task_id else STEP_GLOBUS_UPLOAD_SUBMIT

            while state.next_step in steps:
                if state.next_step == STEP_BUILD_DEPOSITION:
                    # Build and validate deposition payload before touching remote services.
                    build_empiar_deposition_data(metadata, imageset_info(em_handler), publication_info)
                    state.next_step = STEP_VALIDATE_GLOBUS
                    self._save_state(exp_engine, state)
                    continue

                if state.next_step == STEP_FINISH:
                    operation.finish_operation(
                        self.module_config.node_name,
                        {
                            "EntryId": state.entry_id,
                            "EntryDirectory": state.entry_directory,
                            "EmpiarAccession": state.empiar_accession,
                        },
                    )
                    self._clear_state(exp_engine)
                    return

                self._execute_step(
                    state.next_step,
                    state,
                    exp_engine,
                    publication_info,
                    metadata,
                    em_handler,
                )
                self._save_state(exp_engine, state)

            raise ValueError(f"Invalid state file step value: {state.next_step}")
        except Exception as exc:
            if self._is_transient_error(exc):
                exp_engine.logger.warning(
                    "Transient EMPIAR deposition error for %s (%s): %s",
                    exp_engine.exp.secondary_id,
                    type(exc).__name__,
                    exc,
                )
                raise

            exp_engine.logger.error(
                "Fatal EMPIAR deposition error for %s (%s): %s",
                exp_engine.exp.secondary_id,
                type(exc).__name__,
                exc,
            )
            operation.fail_operation(self.module_config.node_name, request_again=False)
            self._clear_state(exp_engine)
