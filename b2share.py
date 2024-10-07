import json, yaml
import logging
import requests
import common
import configuration
import experiment

class B2ShareDraft:
    def __init__(self, bs_http_session: requests.Session, community_id: str, bs_draft_id=None) -> None:
        self.http_session = bs_http_session
        self.draft_id = bs_draft_id
        self.community_id = community_id

    def draft_url(self):
        return self.record_url() + "/draft"
    
    def exists(self):
        return bool(self.draft_id)
    
    def get_record(self):
        response = self.http_session.get(self.record_url())
        response.raise_for_status()
        return response.json()
    
    def get_draft(self):
        response = self.http_session.get(self.draft_url())
        response.raise_for_status()
        resjson = response.json()
        return resjson
    
    def record_page_url(self):
        if self.draft_id:
            return self.http_session.base_url + f"records/{self.draft_id}"
        else:
            raise ValueError("Draft does not exist")

    def record_url(self, absolute=False):
        if self.draft_id:
            return f"api/records/{self.draft_id}" if not absolute else self.http_session.base_url + f"api/records/{self.draft_id}"
        else:
            raise ValueError("Draft does not exist")
        
    def publish_draft(self):
        headers = {"Content-Type":"application/json-patch+json"}
        
        response = self.http_session.patch(url=self.draft_url(), headers=headers, json=[{
            "op":"add", 
            "path":"/publication_state", 
            "value":"submitted"
        }])

        # print(response.content)
        response.raise_for_status()
        published_draft = response.json()
        return published_draft["metadata"]["DOI"]
    
    def update_dataset_access_info(self, path: str, target: str, token: str=None):
        draft = self.get_draft()
        res = { 
            "resource_type_general": "Dataset",
            "resource_type_description": f"path={path} ticket={token}"
        }

        # Check if dataset resource exists and find its index
        resource_index = None
        for i, r in enumerate(draft["metadata"].get("resource_types", [])):
            if r["resource_type_general"] == "Dataset":
                resource_index = i
                break

        # If resource exists, update it
        if resource_index is not None:
            patch_data = [{
                "op":"replace",
                "path": f"/resource_types/{resource_index}",
                "value": res
            }]
        else:
            # Otherwise add it
            patch_data = [{
                "op":"add",
                "path": "/resource_types",
                "value": [res]
            }]


        headers = {"Content-Type":"application/json-patch+json"}
        response = self.http_session.patch(url=self.draft_url(), headers=headers, json=patch_data)
        print(response.content, patch_data)
        response.raise_for_status()

    def get_doi(self):
        record = self.get_record()
        return record["metadata"]["DOI"]
    
    def is_published(self):
        record = self.get_draft()
        return record["metadata"]["publication_state"] == "published"
    
    def prepare_draft_metadata(self, title, metadata):
        title = f"{metadata['DATA_facility_name']} {metadata['PI_last_name']} {metadata['LIMS_ID'][-7:]}"

        # Convert metadata to b2share format
        meta =  {
            "titles": [{ "title": title }],
            "creators": [{"creator_name": metadata["PI_first_name"] + " " + metadata["PI_last_name"]}],
            "community_specific": {} # This seems to be necessary otherwise publication attempt will fail
        }

        # Temporary solution - just attach the metadata to the description field until proper schema and community is defined in b2share
        meta["descriptions"] = [{ "description": yaml.dump(metadata), "description_type": "TechnicalInfo"}]
        return meta
    
    def create_draft(self, metadata):
        if self.draft_id: 
            return # For now we just consider record is actually there
        
        if not "community" in metadata:
            metadata["community"] = self.community_id
        if not "open_access" in metadata:
            metadata["open_access"] = True

        # No draft id - create record
        response = self.http_session.post("api/records/", json=metadata)
        new_record = response.json()
        self.draft_id = new_record["id"]

    def delete_draft(self, not_exists_ok=True):
        response = self.http_session.delete(self.draft_url())
        if response.status_code == 404 and not_exists_ok:
            return
        
        response.raise_for_status()
        self.draft_id = None
    

def b2share_session_factory(base_url: str, api_key: str, timeout=5):
    sess = common.BaseUrlSession(base_url, timeout)
    sess.params = { "access_token": api_key }
    return sess

def b2Share_draft_factory(bs_conf: dict, draft_id=None):
    return B2ShareDraft(
        b2share_session_factory(bs_conf["base_url"], 
        bs_conf["api_key"]), 
        bs_conf["community_id"], 
        draft_id
        )

class B2SharePublicationService(experiment.ExperimentModuleBase):

    # This override gets us experiments ready for publishing instead of the active ones
    def provide_experiments(self):
        publication_requested_exps = experiment.ExperimentsApi(self._api_session).get_experiments_by_states(
            publication_state=[experiment.PublicationState.PUBLICATION_REQUESTED, experiment.PublicationState.DRAFT_CREATION_REQUESTED, experiment.PublicationState.DRAFT_REMOVAL_REQUESTED]
            )
        # Only these for b2share
        return filter(lambda e: e.publication.engine == "b2share" and e.storage.archive, publication_requested_exps)
        
    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine): 

        def publish():
            exp = exp_engine.exp
            # Experiment must first be archived 
            if not exp.storage.state == experiment.StorageState.ARCHIVED:
                # Not error - experiment might be just waiting for the archivation to compltet
                self.logger.info(f"Not publishing experiment {exp.secondary_id} - must be archived first and state is {exp.storage.state}")
                return
            
            b2_draft = b2Share_draft_factory(self.module_config["B2ShareConnection"], exp.publication.draft_id)
            if not b2_draft.exists():
                self.logger.error("Draft must exist")
                return
            
            # print(json.dumps(b2_draft.get_draft(), indent=2))
            # Before publication, attach access info to the draft
            b2_draft.update_dataset_access_info(
                path=exp_engine.exp.storage.path,
                target=exp_engine.exp.storage.target,
                token=exp_engine.exp.storage.token
                )
            
            # Draft publication
            doi = b2_draft.publish_draft() if not b2_draft.is_published() else b2_draft.get_doi()
            exp.exp_api.patch_experiment({"Publication":{
            # Submit publication success to LIMS
                "Doi": doi,
                "TargetUrl": b2_draft.record_page_url(),
                "State": experiment.PublicationState.PUBLISHED.value
            }})

            # Notify by email
            email_conf = self.module_config.lims_config.get_experiment_config(exp_engine.exp.instrument, exp_engine.exp.technique)["JobPublished"]
            exp.exp_api.send_email(email_conf)
            
        def create_draft():
            b2share_draft = b2Share_draft_factory(self.module_config["B2ShareConnection"], draft_id=None)
            # We need to get the metadata 
            # USE ONLY NOW FOR TESTING
            # Draft for the experiment must exist
            print(f"Temporarily disabled {exp_engine.exp.secondary_id}")
            return 
            exp_engine.restore_metadata(exp_engine.extract_metadata())
            metadata = exp_engine.read_metadata()
            metadata = b2share_draft.prepare_draft_metadata(exp_engine.exp.secondary_id, metadata)
            # Prepare file access tickets
            # TODO
            # Create draft using this metadata
            b2share_draft.create_draft(metadata)
            # Save draft ID to the lims and move state
            exp_engine.exp.exp_api.patch_experiment({"Publication": {"RecordId": b2share_draft.draft_id, "State": experiment.PublicationState.DRAFT_CREATED.value}})
            exp_engine.logger.info("Successfully created b2share draft.")

        def remove_draft():
            b2_draft = b2Share_draft_factory(self.module_config["B2ShareConnection"], exp_engine.exp.publication.draft_id)
            b2_draft.delete_draft(not_exists_ok=True)
            exp_engine.exp.exp_api.patch_experiment({"Publication": {"State": experiment.PublicationState.UNPUBLISHED.value, "RecordId": None}})

        action_map = {
            experiment.PublicationState.PUBLICATION_REQUESTED: publish,
            experiment.PublicationState.DRAFT_CREATION_REQUESTED: create_draft,
            experiment.PublicationState.DRAFT_REMOVAL_REQUESTED: remove_draft
        }

        action_map[exp_engine.exp.publication.state]()
            

        


