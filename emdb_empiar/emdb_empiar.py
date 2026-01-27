import tempfile

import experiment
from experiment import Operations
import jsonschema, json

from processing_tools import EmMoviesHandler, VoxelType, MovieFormat
import empiar_depositor.empiar_depositor

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
    mov, met, gain = em_handler.find_movie_information()
    mov_meta = em_handler.movie_metadata(mov, met)

    voxel_map = {
        VoxelType.UNSIGNED_BYTE: "('T1', '')",
        VoxelType.SIGNED_INT32: "('T6', '')"
    }

    format_map = {
        MovieFormat.TIFF: "('T3', '')",
        MovieFormat.MRC: "('T1', '')",
        MovieFormat.EER: "('T9', '')"
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
        "title": metadata["SAMPLE_project_name"] or
                 f'{metadata["SAMPLE_project_name"]} - {metadata["SAMPLE_name"]} - {metadata["DATA_experiment_type"]}',
        "release_date": "HP",  # or HO
        "experiment_type": experiment_type_selector(
            metadata["DATA_emMicroscopeId"], metadata["DATA_experiment_type"]
        ),
        "cross_references": [{"name": publication.draft_id}],
        "biostudies_references": [] if not metadata["SAMPLE_reference"] else [{"name": metadata["SAMPLE_reference"]}],
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

    with open("emdb_empiar/empiar-schema.json") as f:
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

    def step_experiment(self, exp_engine: experiment.ExperimentStorageEngine):
        # exp_engine.data_rules.
        metadata = exp_engine.read_metadata()
        em_handler = EmMoviesHandler(exp_engine)
        publication_info = exp_engine.exp.publications.publication("empiar-emdb")
        json_tmp_input = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)

        try:
            imgset_info = imageset_info(em_handler)
            deposit = build_empiar_deposition_data(metadata, imgset_info, exp_engine.exp.publications.publication("empiar-emdb"))
            # TODO - dump to json_tmp_input
            print(f"We have experiment empiar deposit json! {exp_engine.exp.secondary_id} \n{deposit}")

            # Now prepare empiar depositor for deposition and globus transfer submission
            data = None # Path to directory with data to deposit
            globus_data = {
                'is_dir': '-r',
                'obj_name': None # TODO is last path part of data path, or more like experiment sec id
            }

            depositor = empiar_depositor.empiar_depositor.EmpiarDepositor(
                empiar_token=self.module_config["empiar_api_token"], # Obtained from empiar website
                json_input=json_tmp_input.name, # TODO - this expects file path,
                data=data,
                globus_data=globus_data,
                globus=self.module_config["globus_source_endpoint_id"], # TODO - globus endpoint id

                # Entry information is obtained and set automatically after deposit submission
                # It is needed only for redeposition
                entry_id = None,
                entry_directory=None


            )
            depositor.deposit_data()

        except Exception as e:
            print(e)
            pass
        finally:
            json_tmp_input.close() # TODO remove file
        pass
