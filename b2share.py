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
        
    def record_url(self, absolute=False):
        if self.draft_id:
            return f"records/{self.draft_id}" if not absolute else self.http_session.base_url + f"records/{self.draft_id}"
        else:
            raise ValueError("Draft does not exist")
        
    def publish_draft(self):
        headers = {"Content-Type":"application/json-patch+json"}
        
        response = self.http_session.patch(url=self.draft_url(), headers=headers, json=[{
            "op":"add", 
            "path":"/publication_state", 
            "value":"submitted"
        }])

        response.raise_for_status()
        published_draft = response.json()
        return published_draft["metadata"]["DOI"]
    
    def is_published(self):
        record = self.get_record()
        return record["metadata"]["publication_state"] == "published"
    
    def prepare_draft_metadata(self, title, metadata):
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
        response = self.http_session.post("records/", json=metadata)
        new_record = response.json()
        self.draft_id = new_record["id"]
    

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
        publication_requested_exps = experiment.ExperimentsApi(self._api_session).get_experiments({"publicationState": f"{experiment.PublicationState.PUBLICATION_REQUESTED.value},{experiment.PublicationState.DRAFT_CREATION_REQUESTED.value}"})
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
            
            # Draft for the experiment must exist
            b2_draft = b2Share_draft_factory(self.module_config["B2ShareConnection"], exp.publication.draft_id)
            if not b2_draft.exists():
                self.logger.error("Draft must exist")
                return
            
            # Draft publication
            if not b2_draft.is_published():
                doi = b2_draft.publish_draft()

            # Submit publication success to LIMS
            exp.exp_api.patch_experiment({"Publication":{
                "Doi": doi,
                "TargetUrl": b2_draft.record_url(absolute=True),
                "State": experiment.PublicationState.PUBLISHED.value
            }})

            # Notify by email
            exp.exp_api.send_email(exp_engine.e_config["JobPublished"])
            
        def create_draft():
            b2share_draft = b2Share_draft_factory(self.module_config["B2ShareConnection"], draft_id=None)
            # We need to get the metadata 
            metadata = exp_engine.read_metadata()
            metadata = b2share_draft.prepare_draft_metadata(exp_engine.exp.secondary_id, metadata)
            # Prepare file access tickets
            # TODO
            # Create draft using this metadata
            b2share_draft.create_draft(metadata)
            # Save draft ID to the lims and move state
            exp_engine.exp.exp_api.patch_experiment({"Publication": {"RecordId": b2share_draft.draft_id, "State": experiment.PublicationState.DRAFT_CREATED.value}})
            exp_engine.logger.info("Successfully created b2share draft.")

        action_map = {
            experiment.PublicationState.PUBLICATION_REQUESTED: publish,
            experiment.PublicationState.DRAFT_CREATION_REQUESTED: create_draft
        }

        action_map[exp_engine.exp.publication.state]()
            

        


