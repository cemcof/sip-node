# AGENTS Guide for `sip-node`

## Big picture (read this first)
- Runtime entrypoint is `main.py`: it continuously syncs config with LIMS, then dynamically imports and schedules module classes listed under config `LimsNodes.<node>.Modules[*].target`.
- Modules must subclass either `configuration.LimsNodeModule` (node-level services) or `experiment.ExperimentModuleBase` (experiment-level services with storage engines and runners).
- Core domain model is in `experiment.py`: wrappers map LIMS JSON to Python objects and mutate state via JSON Patch (`ExperimentApi.patch_experiment`, `common.dict_to_json_patch`).
- Storage abstraction lives in `ExperimentStorageEngine` (`experiment.py`), with concrete engines in `fs_storage_engine.py` and `irods_storage_engine.py` selected by storage engine prefix in `main.py`.
- Data movement logic is centralized in `data_tools.py` (`DataRule`, `DataRulesWrapper`, `DataAsyncTransferer`), and many services build behavior by composing tags (`raw`, `processed`, `archive`, `metadata`).

## Service boundaries and flow
- Job lifecycle: `job_lifecycle_service.py` drives `START_REQUESTED -> ACTIVE -> STOP_REQUESTED -> FINISHED`, prepares storage, uploads raw data, and updates LIMS states.
- Processing services (`scipion_processing.py`, `cryosparc/processing.py`) consume raw data, run external engines, stream logs/results back to LIMS documents, and manage `ProcessingState` transitions.
- Archival/expiration/cleanup services (`data_archivation_service.py`, `data_expiration_service.py`, `data_clean_service.py`) are separate periodic modules keyed by storage/operation states.
- Proxy replication is independent (`proxy_transferer.py`) and resolves destination nodes by inspecting config for `job_lifecycle_service.JobLifecycleService` ownership.
- Publication path (`b2share.py`) is state-driven and assumes archived data before publishing.

## Project-specific conventions
- Config lookup is layered (`LimsModuleConfigWrapper.__getitem__`): module -> node -> global (`configuration.py`). Prefer `module_config.get(...)` when keys are optional.
- Experiment module concurrency is config-driven (`parallel` key): `ExperimentModuleBase` picks `SequentialRunner` vs `ParallelRunner` (`experiment.py`).
- API session is shared and thread-serialized (`common.BaseUrlSession` has internal request lock).
- LIMS log shipping is async: `logger_db_api.LimsApiLoggerHandler.keep_flushing()` runs on a daemon thread from `main.py`.
- File pattern policy is intentionally fnmatch-style (not arbitrary regex from model for source patterns); see `ExperimentDataSourceWrapper.source_patterns` and `FnMatchPattern`.

## External integrations to respect
- LIMS HTTP API (all state/config/doc updates) via `configuration.create_lims_session`.
- iRODS via `python-irodsclient` in `irods_storage_engine.py` (tickets, metadata, collection lifecycle).
- Processing engines: Scipion (`scipion_processing.py`) and CryoSPARC (`cryosparc/processing.py`, `cryosparc/cli.py`).
- B2SHARE publication in `b2share.py` (draft lifecycle + DOI submission).

## Developer workflows (discovered here)
- Install deps:
  - `python -m pip install -r requirements.txt`
- Run the node:
  - `python main.py --organization-name <org> --sip-api-url <url> --sip-api-key <key> --config-file <config.yml>`
- Run tests (unittest style):
  - `python -m unittest discover -s tests -v`
  - `python -m unittest tests.test_parallel_runner -v`

## Agent tips for making safe changes
- Prefer implementing behavior inside `step_experiment(...)` and keep `provide_experiments()` filters explicit by enum states.
- When adding transfers, express scope through `DataRulesWrapper.with_tags(...)` rather than hardcoded glob scans.
- Keep state transitions explicit and idempotent; most services rely on periodic re-entry, not one-shot execution.
- Preserve module target importability (`<module>.<Class>` strings from config) when renaming files/classes.
- Be careful with source cleanup: `data_clean_service.py` defaults to `dry_run=True` unless config overrides it.
