import experiment
from experiment import Operations
import jsonschema, json

from processing_tools import EmMoviesHandler
import empiar_depositor.empiar_depositor

def experiment_type_selector(instrument, technique):
    # 3 - default
    # 4, 5 hydra?
    # 9 - diffraction tecnique
    if instrument.startswith('Hydra'):
        return 4
    return 3

def imageset_info():

    return {
        "category": None,
        "header_format": None,
        "data_format": None,
        "num_images_or_tilt_series": None,
        "frames_per_image": None,
    }

def build_empiar_deposition_data(metadata):

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

    imagesets = {}

    empiar_deposition = {
        "title": metadata["SAMPLE_project_name"] or
                 f'{metadata["SAMPLE_project_name"]} - {metadata["SAMPLE_name"]} - {metadata["DATA_experiment_type"]}',
        "release_date": "HP",  # or HO
        "experiment_type": experiment_type_selector(
            metadata["DATA_emMicroscopeId"], metadata["DATA_experiment_type"]
        ),
        "cross_references": [{"name": "TODO "}],
        "biostudies_references": [] if not metadata["SAMPLE_reference"] else [{"name": metadata["SAMPLE_reference"]}],
        "authors": [user_simple],
        "corresponding_author": user_complex,
        "principal_investigator": [user_complex],
        "imagesets": imagesets,
        "citation": [{
            "authors": [user_simple],
            "published": False,
            "j_or_nj_citation": True,
            "title": metadata["TODO"]
        }]
    }

    # TODO - experiment type selector - jeste probrat s Jirkou
    # TODO - moznost archivace Transfer zrusit?
    # TODO - country
    # TODO - imagesets
    # TODO - workflowhub reference
    # TODO - workflow file v pripade scipionu
    # TODO - user middle name

    jsonschema.validate(instance=empiar_deposition, schema=json.loads(empiar_schema))
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
        mov, met,  gain = em_handler.find_movie_information()



        try:
            deposit = build_empiar_deposition_data(metadata)
            print(f"We have experiment empiar deposit json! {exp_engine.exp.secondary_id} \n{deposit}")
        except Exception as e:
            print(e)
            pass

        pass


empiar_schema = """
{
  "$id": "https://empiar.org/empiar_deposition.schema.json#",
  "type": "object",
  "definitions": {},
  "$schema": "http://json-schema.org/schema#",
  "properties": {
    "title": {
      "$id": "/properties/title",
      "type": "string",
      "title": "The Title Schema ",
      "minLength": 10,
      "maxLength": 255,
      "examples": [
        "Volta phase plate data collection facilitates image processing and cryo-EM structure determination"
      ]
    },
    "release_date": {
      "$id": "/properties/release_date",
      "type": "string",
      "title": "The Release_date Schema ",
      "enum": [
        "RE",
        "EP",
        "HP",
        "HO"
      ],
      "default": "RE",
      "description": "Options for releasing entry to the public: RE - directly after the submission has been processed, EP - after the related EMDB entry has been released, HP - after the related primary citation has been published and HO - delay release of entry by one year from the date of deposition.",
      "examples": [
        "RE"
      ]
    },
    "experiment_type": {
      "$id": "/properties/experiment_type",
      "type": "integer",
      "title": "The Experiment_type Schema ",
      "enum": [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        11,
        12,
        13
      ],
      "default": 3,
      "description": "Type of the EMPIAR entry: 1 - image data collected using soft x-ray tomography, 2 - simulated data, for instance, created using InSilicoTEM (note: we only accept simulated data in special circumstances such as test/training sets for validation challenges: you need to ask for and be granted permission PRIOR to deposition otherwise the dataset will be rejected), 3 - raw image data relating to structures deposited to the Electron Microscopy Data Bank, 4 - image data collected using serial block-face scanning electron microscopy (like the Gatan 3View system), 5 - image data collected using focused ion beam scanning electron microscopy, 6 - integrative hybrid modelling data, 7 - correlative light-electron microscopy, 8 - correlative light X-ray microscopy, 9 - microcrystal electron diffraction, 11 - ATUM-SEM, 12 - Hard X-ray/X-ray microCT, 13 - ssET",
      "examples": [
        3
      ]
    },
    "scale": {
      "$id": "/properties/scale",
      "type": "string",
      "title": "The Scale Schema ",
      "pattern": "^[1-5]$",
      "default": 3,
      "description": "The scale of the experiment: 1 - molecule, 2 - virus, 3 - cell, 4 - tissue, 5 - organism",
      "examples": [
        3
      ]
    },
    "cross_references": {
      "$id": "/properties/cross_references",
      "type": "array",
      "items": {
        "$id": "/properties/cross_references/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/cross_references/items/properties/name",
            "type": "string",
            "pattern": "^(EMD)-[0-9]{4,}$",
            "title": "The Name Schema ",
            "examples": [
              "EMD-8000"
            ]
          }
        }
      },
      "default": [],
      "description": "Array of references to EMDB"
    },
    "biostudies_references": {
      "$id": "/properties/biostudies_references",
      "type": "array",
      "items": {
        "$id": "/properties/biostudies_references/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/biostudies_references/items/properties/name",
            "type": "string",
            "pattern": "^S-.*$",
            "title": "The Name Schema ",
            "examples": [
              "S-SCDT-EMBOR-2018-46196V1"
            ]
          }
        }
      },
      "default": [],
      "description": "Array of references to BioStudies"
    },
    "idr_references": {
      "$id": "/properties/idr_references",
      "type": "array",
      "items": {
        "$id": "/properties/idr_references/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/idr_references/items/properties/name",
            "type": "string",
            "pattern": "^idr.*$",
            "title": "The Name Schema ",
            "examples": [
              "idr1040"
            ]
          }
        }
      },
      "default": [],
      "description": "Array of references to IDR"
    },
    "empiar_references": {
      "$id": "/properties/empiar_references",
      "type": "array",
      "items": {
        "$id": "/properties/empiar_references/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/empiar_references/items/properties/name",
            "type": "string",
            "pattern": "^EMPIAR-\\d+$",
            "title": "The Name Schema ",
            "examples": [
              "EMPIAR-10002"
            ]
          }
        }
      },
      "default": [],
      "description": "Array of references to EMPIAR"
    },
    "workflows": {
      "$id": "/properties/workflows",
      "type": "array",
      "items": {
        "$id": "/properties/workflows/items",
        "type": "object",
        "properties": {
          "url": {
            "$id": "/properties/workflows/items/properties/url",
            "type": "string",
            "pattern": "^https:\/\/.+$",
            "title": "The Url Schema ",
            "examples": [
              "https://doi.org/10.48546/workflowhub.workflow.201.1"
            ]
          },
          "type": {
            "$id": "/properties/workflows/items/properties/type",
            "type": "integer",
            "title": "The Type Schema ",
            "description": "Currently supported value: 1 - WorkflowHub.",
            "enum": [
              1
            ]
          }
        }
      },
      "default": [],
      "description": "Array of workflows"
    },
    "authors": {
      "$id": "/properties/authors",
      "type": "array",
      "minItems": 1,
      "items": {
        "$id": "/properties/authors/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/authors/items/properties/name",
            "type": "string",
            "pattern": "^\\('[^0-9!\"£$%^&*()]+', '[^0-9!\"£$%^&*()]+'\\)$",
            "title": "The Name Schema ",
            "description": "Author's surname and given name represented in specified format. Given name is initials as capital letters only. Max length of surname is 60 characters, max length of given name - 20.",
            "examples": [
              "('Chang', 'YW')"
            ]
          },
          "order_id": {
            "$id": "/properties/authors/items/properties/order_id",
            "type": "integer",
            "title": "The Order_id Schema ",
            "default": 0,
            "description": "Author's order ID in the author array. Starts with zero.",
            "examples": [
              0
            ]
          },
          "author_orcid": {
            "$id": "/properties/authors/items/properties/author_orcid",
            "type": [
              "null",
              "string"
            ],
            "pattern": "^([0-9]{4}-){3}[0-9]{3}([0-9]|X)$",
            "title": "The Author_orcid Schema ",
            "default": null,
            "description": "ORCID identifier of the author. Apart from complying with regex it has to satisfy the checksum: https://support.orcid.org/knowledgebase/articles/116780-structure-of-the-orcid-identifier",
            "examples": [
              "0000-0003-2391-473X"
            ]
          }
        },
        "required": [
          "name",
          "order_id"
        ]
      },
      "description": "Array of entry authors. Has to contain all the relevant authors, even if one of them is filled into the corresponding_author or principal_investigator. Can be different from the citation author array.",
      "examples": [
        [
          "('Chang', 'YW')",
          "('Smith', 'JD')"
        ]
      ]
    },
    "corresponding_author": {
      "$id": "/properties/corresponding_author",
      "type": "object",
      "properties": {
        "author_orcid": {
          "$id": "/properties/corresponding_author/properties/author_orcid",
          "type": [
            "null",
            "string"
          ],
          "pattern": "^([0-9]{4}-){3}[0-9]{3}([0-9]|X)$",
          "title": "The Author_orcid Schema ",
          "description": "ORCID identifier of the author. Apart from complying with regex it has to satisfy the checksum: https://support.orcid.org/knowledgebase/articles/116780-structure-of-the-orcid-identifier",
          "examples": [
            "0000-0003-2391-473X"
          ]
        },
        "first_name": {
          "$id": "/properties/corresponding_author/properties/first_name",
          "type": "string",
          "pattern": "^[^0-9!\"£$%^&*()]+$",
          "title": "The First_name Schema ",
          "maxLength": 60,
          "examples": [
            "Yi-Wei"
          ]
        },
        "middle_name": {
          "$id": "/properties/corresponding_author/properties/middle_name",
          "type": [
            "null",
            "string"
          ],
          "pattern": "^[^0-9!\"£$%^&*()]+$",
          "title": "The Middle_name Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            "John"
          ]
        },
        "last_name": {
          "$id": "/properties/corresponding_author/properties/last_name",
          "type": "string",
          "pattern": "^[^0-9!\"£$%^&*()]+$",
          "title": "The Last_name Schema ",
          "maxLength": 60,
          "examples": [
            "Chang"
          ]
        },
        "organization": {
          "$id": "/properties/corresponding_author/properties/organization",
          "type": "string",
          "title": "The Organization Schema ",
          "maxLength": 255,
          "examples": [
            "Division of Biology, California Institute of Technology"
          ]
        },
        "street": {
          "$id": "/properties/corresponding_author/properties/street",
          "type": [
            "null",
            "string"
          ],
          "title": "The Street Schema ",
          "maxLength": 255,
          "default": null,
          "examples": [
            "1200 E California Blvd"
          ]
        },
        "town_or_city": {
          "$id": "/properties/corresponding_author/properties/town_or_city",
          "type": "string",
          "title": "The Town_or_city Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            "Pasadena"
          ]
        },
        "state_or_province": {
          "$id": "/properties/corresponding_author/properties/state_or_province",
          "type": [
            "null",
            "string"
          ],
          "title": "The State_or_province Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            "California"
          ]
        },
        "post_or_zip": {
          "$id": "/properties/corresponding_author/properties/post_or_zip",
          "type": "string",
          "title": "The Post_or_zip Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            "91125"
          ]
        },
        "telephone": {
          "$id": "/properties/corresponding_author/properties/telephone",
          "type": [
            "null",
            "string"
          ],
          "title": "The Telephone Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            null
          ]
        },
        "fax": {
          "$id": "/properties/corresponding_author/properties/fax",
          "type": [
            "null",
            "string"
          ],
          "title": "The Fax Schema ",
          "maxLength": 60,
          "default": null,
          "examples": [
            null
          ]
        },
        "email": {
          "$id": "/properties/corresponding_author/properties/email",
          "type": "string",
          "format": "email",
          "title": "The Email Schema ",
          "examples": [
            "test@example.com"
          ]
        },
        "country": {
          "$id": "/properties/corresponding_author/properties/country",
          "type": "string",
          "title": "The Country Schema ",
          "enum": [
            "AD",
            "AE",
            "AF",
            "AG",
            "AI",
            "AL",
            "AM",
            "AO",
            "AQ",
            "AR",
            "AS",
            "AT",
            "AU",
            "AW",
            "AZ",
            "BA",
            "BB",
            "BD",
            "BE",
            "BF",
            "BG",
            "BH",
            "BI",
            "BJ",
            "BL",
            "BM",
            "BN",
            "BO",
            "BR",
            "BS",
            "BT",
            "BV",
            "BW",
            "BY",
            "BZ",
            "CA",
            "CC",
            "CD",
            "CF",
            "CG",
            "CH",
            "CI",
            "CK",
            "CL",
            "CM",
            "CN",
            "CO",
            "CR",
            "CU",
            "CV",
            "CW",
            "CX",
            "CY",
            "CZ",
            "DE",
            "DJ",
            "DK",
            "DM",
            "DO",
            "DZ",
            "EC",
            "EE",
            "EG",
            "EH",
            "ER",
            "ES",
            "ET",
            "FI",
            "FJ",
            "FK",
            "FM",
            "FO",
            "FR",
            "FX",
            "GA",
            "GB",
            "GD",
            "GE",
            "GF",
            "GG",
            "GH",
            "GI",
            "GL",
            "GM",
            "GN",
            "GP",
            "GQ",
            "GR",
            "GS",
            "GT",
            "GU",
            "GW",
            "GY",
            "HK",
            "HM",
            "HN",
            "HR",
            "HT",
            "HU",
            "ID",
            "IE",
            "IL",
            "IM",
            "IN",
            "IO",
            "IQ",
            "IR",
            "IS",
            "IT",
            "JE",
            "JM",
            "JO",
            "JP",
            "KE",
            "KG",
            "KH",
            "KI",
            "KM",
            "KN",
            "KP",
            "KR",
            "KW",
            "KY",
            "KZ",
            "LA",
            "LB",
            "LC",
            "LI",
            "LK",
            "LR",
            "LS",
            "LT",
            "LU",
            "LV",
            "LY",
            "MA",
            "MC",
            "MD",
            "ME",
            "MF",
            "MG",
            "MH",
            "MK",
            "ML",
            "MM",
            "MN",
            "MO",
            "MP",
            "MQ",
            "MR",
            "MS",
            "MT",
            "MU",
            "MV",
            "MW",
            "MX",
            "MY",
            "MZ",
            "NA",
            "NC",
            "NE",
            "NF",
            "NG",
            "NI",
            "NL",
            "NO",
            "NP",
            "NR",
            "NU",
            "NZ",
            "OM",
            "PA",
            "PE",
            "PF",
            "PG",
            "PH",
            "PK",
            "PL",
            "PM",
            "PN",
            "PR",
            "PS",
            "PT",
            "PW",
            "PY",
            "QA",
            "RE",
            "RO",
            "RS",
            "RU",
            "RW",
            "SA",
            "SB",
            "SC",
            "SD",
            "SE",
            "SG",
            "SH",
            "SI",
            "SJ",
            "SK",
            "SL",
            "SM",
            "SN",
            "SO",
            "SR",
            "SS",
            "ST",
            "SV",
            "SX",
            "SY",
            "SZ",
            "TC",
            "TD",
            "TF",
            "TG",
            "TH",
            "TJ",
            "TK",
            "TL",
            "TM",
            "TN",
            "TO",
            "TR",
            "TT",
            "TV",
            "TW",
            "TZ",
            "UA",
            "UG",
            "UM",
            "US",
            "UY",
            "UZ",
            "VA",
            "VC",
            "VE",
            "VG",
            "VI",
            "VN",
            "VU",
            "WF",
            "WS",
            "XK",
            "YE",
            "YT",
            "ZA",
            "ZM",
            "ZW"
          ],
          "description": "Country according to ISO 3166-1 alpha-2 standard",
          "examples": [
            "UK"
          ]
        }
      },
      "required": [
        "first_name",
        "last_name",
        "organization",
        "email",
        "country"
      ]
    },
    "principal_investigator": {
      "$id": "/properties/principal_investigator",
      "type": "array",
      "minItems": 1,
      "items": {
        "$id": "/properties/principal_investigator/items",
        "type": "object",
        "properties": {
          "author_orcid": {
            "$id": "/properties/principal_investigator/items/properties/author_orcid",
            "type": [
              "null",
              "string"
            ],
            "pattern": "^([0-9]{4}-){3}[0-9]{3}([0-9]|X)$",
            "title": "The Author_orcid Schema ",
            "description": "ORCID identifier of the author. Apart from complying with regex it has to satisfy the checksum: https://support.orcid.org/knowledgebase/articles/116780-structure-of-the-orcid-identifier",
            "examples": [
              "0000-0003-2391-473X"
            ]
          },
          "first_name": {
            "$id": "/properties/principal_investigator/items/properties/first_name",
            "type": "string",
            "pattern": "^[^0-9!\"£$%^&*()]+$",
            "title": "The First_name Schema ",
            "maxLength": 60,
            "examples": [
              "Yi-Wei"
            ]
          },
          "middle_name": {
            "$id": "/properties/principal_investigator/items/properties/middle_name",
            "type": [
              "null",
              "string"
            ],
            "pattern": "^[^0-9!\"£$%^&*()]+$",
            "title": "The Middle_name Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              "John"
            ]
          },
          "last_name": {
            "$id": "/properties/principal_investigator/items/properties/last_name",
            "type": "string",
            "pattern": "^[^0-9!\"£$%^&*()]+$",
            "title": "The Last_name Schema ",
            "maxLength": 60,
            "examples": [
              "Chang"
            ]
          },
          "organization": {
            "$id": "/properties/principal_investigator/items/properties/organization",
            "type": "string",
            "title": "The Organization Schema ",
            "maxLength": 255,
            "examples": [
              "Division of Biology, California Institute of Technology"
            ]
          },
          "street": {
            "$id": "/properties/principal_investigator/items/properties/street",
            "type": [
              "null",
              "string"
            ],
            "title": "The Street Schema ",
            "maxLength": 255,
            "default": null,
            "examples": [
              "1200 E California Blvd"
            ]
          },
          "town_or_city": {
            "$id": "/properties/principal_investigator/items/properties/town_or_city",
            "type": "string",
            "title": "The Town_or_city Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              "Pasadena"
            ]
          },
          "state_or_province": {
            "$id": "/properties/principal_investigator/items/properties/state_or_province",
            "type": [
              "null",
              "string"
            ],
            "title": "The State_or_province Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              "California"
            ]
          },
          "post_or_zip": {
            "$id": "/properties/principal_investigator/items/properties/post_or_zip",
            "type": "string",
            "title": "The Post_or_zip Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              "91125"
            ]
          },
          "telephone": {
            "$id": "/properties/principal_investigator/items/properties/telephone",
            "type": [
              "null",
              "string"
            ],
            "title": "The Telephone Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              null
            ]
          },
          "fax": {
            "$id": "/properties/principal_investigator/items/properties/fax",
            "type": [
              "null",
              "string"
            ],
            "title": "The Fax Schema ",
            "maxLength": 60,
            "default": null,
            "examples": [
              null
            ]
          },
          "email": {
            "$id": "/properties/principal_investigator/items/properties/email",
            "type": "string",
            "format": "email",
            "title": "The Email Schema ",
            "examples": [
              "test@example.com"
            ]
          },
          "country": {
            "$id": "/properties/principal_investigator/items/properties/country",
            "type": "string",
            "title": "The Country Schema ",
            "enum": [
              "AD",
              "AE",
              "AF",
              "AG",
              "AI",
              "AL",
              "AM",
              "AO",
              "AQ",
              "AR",
              "AS",
              "AT",
              "AU",
              "AW",
              "AZ",
              "BA",
              "BB",
              "BD",
              "BE",
              "BF",
              "BG",
              "BH",
              "BI",
              "BJ",
              "BL",
              "BM",
              "BN",
              "BO",
              "BR",
              "BS",
              "BT",
              "BV",
              "BW",
              "BY",
              "BZ",
              "CA",
              "CC",
              "CD",
              "CF",
              "CG",
              "CH",
              "CI",
              "CK",
              "CL",
              "CM",
              "CN",
              "CO",
              "CR",
              "CU",
              "CV",
              "CW",
              "CX",
              "CY",
              "CZ",
              "DE",
              "DJ",
              "DK",
              "DM",
              "DO",
              "DZ",
              "EC",
              "EE",
              "EG",
              "EH",
              "ER",
              "ES",
              "ET",
              "FI",
              "FJ",
              "FK",
              "FM",
              "FO",
              "FR",
              "FX",
              "GA",
              "GB",
              "GD",
              "GE",
              "GF",
              "GG",
              "GH",
              "GI",
              "GL",
              "GM",
              "GN",
              "GP",
              "GQ",
              "GR",
              "GS",
              "GT",
              "GU",
              "GW",
              "GY",
              "HK",
              "HM",
              "HN",
              "HR",
              "HT",
              "HU",
              "ID",
              "IE",
              "IL",
              "IM",
              "IN",
              "IO",
              "IQ",
              "IR",
              "IS",
              "IT",
              "JE",
              "JM",
              "JO",
              "JP",
              "KE",
              "KG",
              "KH",
              "KI",
              "KM",
              "KN",
              "KP",
              "KR",
              "KW",
              "KY",
              "KZ",
              "LA",
              "LB",
              "LC",
              "LI",
              "LK",
              "LR",
              "LS",
              "LT",
              "LU",
              "LV",
              "LY",
              "MA",
              "MC",
              "MD",
              "ME",
              "MF",
              "MG",
              "MH",
              "MK",
              "ML",
              "MM",
              "MN",
              "MO",
              "MP",
              "MQ",
              "MR",
              "MS",
              "MT",
              "MU",
              "MV",
              "MW",
              "MX",
              "MY",
              "MZ",
              "NA",
              "NC",
              "NE",
              "NF",
              "NG",
              "NI",
              "NL",
              "NO",
              "NP",
              "NR",
              "NU",
              "NZ",
              "OM",
              "PA",
              "PE",
              "PF",
              "PG",
              "PH",
              "PK",
              "PL",
              "PM",
              "PN",
              "PR",
              "PS",
              "PT",
              "PW",
              "PY",
              "QA",
              "RE",
              "RO",
              "RS",
              "RU",
              "RW",
              "SA",
              "SB",
              "SC",
              "SD",
              "SE",
              "SG",
              "SH",
              "SI",
              "SJ",
              "SK",
              "SL",
              "SM",
              "SN",
              "SO",
              "SR",
              "SS",
              "ST",
              "SV",
              "SX",
              "SY",
              "SZ",
              "TC",
              "TD",
              "TF",
              "TG",
              "TH",
              "TJ",
              "TK",
              "TL",
              "TM",
              "TN",
              "TO",
              "TR",
              "TT",
              "TV",
              "TW",
              "TZ",
              "UA",
              "UG",
              "UM",
              "US",
              "UY",
              "UZ",
              "VA",
              "VC",
              "VE",
              "VG",
              "VI",
              "VN",
              "VU",
              "WF",
              "WS",
              "XK",
              "YE",
              "YT",
              "ZA",
              "ZM",
              "ZW"
            ],
            "description": "Country according to ISO 3166-1 alpha-2 standard",
            "examples": [
              "UK"
            ]
          }
        },
        "required": [
          "first_name",
          "last_name",
          "organization",
          "email",
          "country"
        ]
      },
      "description": "Array of principal investigators."
    },
    "workflow_file": {
      "$id": "/properties/workflow_file",
      "type": "object",
      "properties": {
        "path": {
          "$id": "/properties/workflow_file/items/properties/path",
          "type": [
            "null",
            "string"
          ],
          "title": "The relative path to the workflow file",
          "default": null,
          "maxLength": 200,
          "description": "A workflow file, for example, a Scipion (http://scipion.i2pc.es) workflow provides a great way to reproduce previous processing steps and is particularly useful to repeat steps for similar samples or to share knowledge between users.",
          "examples": [
            "data/workflow.json"
          ]
        }
      },
      "required": [
        "path"
      ]
    },
    "imagesets": {
      "$id": "/properties/imagesets",
      "type": "array",
      "minItems": 1,
      "items": {
        "$id": "/properties/imagesets/items",
        "type": "object",
        "properties": {
          "name": {
            "$id": "/properties/imagesets/items/properties/name",
            "type": "string",
            "title": "The Name Schema ",
            "maxLength": 200,
            "description": "Descriptive name for data-set. This name will be used to identify the data-set on the EMPIAR website and to distinguish it from other sets belonging to the same entry.",
            "examples": [
              "Different tilt series for the Vibrio cholerae toxin-coregulated pilus machine revealed by electron cryotomography"
            ]
          },
          "directory": {
            "$id": "/properties/imagesets/items/properties/directory",
            "type": "string",
            "pattern": "^[^\\0]+$",
            "title": "The Directory Schema ",
            "maxLength": 200,
            "description": "Path to the directory that would contain all image set files. The top-level directory 'data' is always reserved for the sake of conformity. The uploaded image sets will be placed in it as they are.",
            "examples": [
              "data/micrographs"
            ]
          },
          "category": {
            "$id": "/properties/imagesets/items/properties/category",
            "type": "string",
            "pattern": "^(\\('T(?:1[0-4]|[1-9])', ''\\))|\\('OT', '[^\\0]+'\\)$",
            "title": "The Category Schema ",
            "description": "'T1' corresponds to 'micrographs - single frame', 'T2' - 'micrographs - multiframe', 'T3' - 'micrographs - focal pairs - unprocessed', 'T4' - 'micrographs - focal pairs - contrast inverted', 'T5' - 'picked particles - single frame - unprocessed', 'T6' - 'picked particles - multiframe - unprocessed', 'T7' - 'picked particles - single frame - processed', 'T8' - 'picked particles - multiframe - processed', 'T9' - 'tilt series', 'T10' - 'class averages', T11' - 'stitched maps', 'T12' - 'diffraction images', 'T13' - 'reconstructed volumes', 'T14' - 'subtomograms', 'OT' - other, in this case please specify the category in the second element.",
            "examples": [
              "('T9', '')",
              "('OT', 'new category name')"
            ]
          },
          "header_format": {
            "$id": "/properties/imagesets/items/properties/header_format",
            "type": "string",
            "pattern": "^(\\('T(?:1[0-4]|[1-9])', ''\\))|\\('OT', '[A-Z0-9 ]+'\\)$",
            "title": "The Header_format Schema ",
            "description": "'T1' corresponds to 'MRC', 'T2' - 'MRCS', 'T3' - 'TIFF', 'T4' - 'IMAGIC', 'T5' - 'DM3', 'T6' - 'DM4', 'T7' - 'SPIDER', 'T8' - 'XML', 'T9' - 'EER', 'T10' - 'PNG', 'T11' - 'JPEG', 'T12' - 'SMV', 'T13' - 'EM', 'T14' - 'TPX3', 'OT' - other, in this case please specify the header format in the second element in capital letters.",
            "examples": [
              "('T1', '')",
              "('OT', 'NEW HEADER FORMAT')"
            ]
          },
          "data_format": {
            "$id": "/properties/imagesets/items/properties/data_format",
            "type": "string",
            "pattern": "^(\\('T(?:1[0-4]|[1-9])', ''\\))|\\('OT', '[A-Z0-9 ]+'\\)$",
            "title": "The Data_format Schema ",
            "description": "'T1' corresponds to 'MRC', 'T2' - 'MRCS', 'T3' - 'TIFF', 'T4' - 'IMAGIC', 'T5' - 'DM3', 'T6' - 'DM4','T7' - 'SPIDER', 'T8' - 'BIG DATA VIEWER HDF5', 'T9' - 'EER', 'T10' - 'PNG', 'T11' - 'JPEG', 'T12' - 'SMV', 'T13' - 'EM', 'T14' - 'TPX3', 'OT' - other, in this case please specify the header format in the second element in capital letters.",
            "examples": [
              "('T1', '')",
              "('OT', 'NEW DATA FORMAT')"
            ]
          },
          "num_images_or_tilt_series": {
            "$id": "/properties/imagesets/items/properties/num_images_or_tilt_series",
            "type": "integer",
            "title": "The Num_images_or_tilt_series Schema ",
            "examples": [
              16
            ]
          },
          "frames_per_image": {
            "$id": "/properties/imagesets/items/properties/frames_per_image",
            "type": "integer",
            "title": "The Frames_per_image Schema ",
            "examples": [
              8
            ]
          },
          "frame_range_min": {
            "$id": "/properties/imagesets/items/properties/frame_range_min",
            "type": [
              "null",
              "integer"
            ],
            "title": "The Frame_range_min Schema ",
            "default": null,
            "examples": [
              2
            ]
          },
          "frame_range_max": {
            "$id": "/properties/imagesets/items/properties/frame_range_max",
            "type": [
              "null",
              "integer"
            ],
            "title": "The Frame_range_max Schema ",
            "default": null,
            "examples": [
              6
            ]
          },
          "voxel_type": {
            "$id": "/properties/imagesets/items/properties/voxel_type",
            "type": "string",
            "pattern": "^(\\('T([1-9])', ''\\))|\\('OT', '[A-Z0-9 ]+'\\)$",
            "title": "The Voxel_type Schema ",
            "description": "'T1' corresponds to 'UNSIGNED BYTE', 'T2' - 'SIGNED BYTE', 'T3' - 'UNSIGNED 16 BIT INTEGER', 'T4' - 'SIGNED 16 BIT INTEGER', 'T5' - 'UNSIGNED 32 BIT INTEGER', 'T6' - 'SIGNED 32 BIT INTEGER', 'T7' - '32 BIT FLOAT', 'T8' - 'BIT', 'T9' - '4 BIT INTEGER', 'OT' - other, in this case please specify the header format in the second element in capital letters.",
            "examples": [
              "('T5', '')",
              "('OT', 'NEW VOXEL TYPE')"
            ]
          },
          "pixel_width": {
            "$id": "/properties/imagesets/items/properties/pixel_width",
            "type": [
              "null",
              "number"
            ],
            "title": "The Pixel_width Schema ",
            "default": null,
            "examples": [
              1.6
            ]
          },
          "pixel_height": {
            "$id": "/properties/imagesets/items/properties/pixel_height",
            "type": [
              "null",
              "number"
            ],
            "title": "The Pixel_height Schema ",
            "default": null,
            "examples": [
              1.6
            ]
          },
          "details": {
            "$id": "/properties/imagesets/items/properties/details",
            "type": [
              "null",
              "string"
            ],
            "title": "The Details Schema ",
            "default": null,
            "examples": [
              "Three-dimensional in situ structure of a T4bP machine in its piliated and non-piliated states constructed from its tilt series."
            ]
          },
          "micrographs_file_pattern": {
            "$id": "/properties/imagesets/items/properties/micrographs_file_pattern",
            "type": [
              "null",
              "string"
            ],
            "title": "The Micrographs_file_pattern Schema ",
            "default": null,
            "maxLength": 200,
            "description": "The pattern for the micrographs that correspond to the picked particles.",
            "examples": [
              "data/micrographs/mc-*-05-2018-*.mrc"
            ]
          },
          "picked_particles_file_pattern": {
            "$id": "/properties/imagesets/items/properties/picked_particles_file_pattern",
            "type": [
              "null",
              "string"
            ],
            "title": "The Picked_particles_file_pattern Schema ",
            "default": null,
            "maxLength": 200,
            "description": "The pattern for the picked particle description files or the path to such a file if there is just one.",
            "examples": [
              "data/picked_particles.star"
            ]
          },
          "picked_particles_directory": {
            "$id": "/properties/imagesets/items/properties/picked_particles_directory",
            "type": [
              "null",
              "string"
            ],
            "title": "The Picked_particles_directory Schema ",
            "default": null,
            "maxLength": 200,
            "description": "The directory that contains the related picked particles.",
            "examples": [
              "data/picked_particles1"
            ]
          },
          "image_width": {
            "$id": "/properties/imagesets/items/properties/image_width",
            "type": [
              "null",
              "integer"
            ],
            "title": "The Image_width Schema ",
            "default": null,
            "examples": [
              3838
            ]
          },
          "image_height": {
            "$id": "/properties/imagesets/items/properties/image_height",
            "type": [
              "null",
              "integer"
            ],
            "title": "The Image_height Schema ",
            "default": null,
            "examples": [
              3710
            ]
          }
        },
        "required": [
          "name",
          "directory",
          "category",
          "header_format",
          "data_format",
          "num_images_or_tilt_series",
          "frames_per_image",
          "voxel_type"
        ]
      }
    },
    "citation": {
      "$id": "/properties/citation",
      "type": "array",
      "minItems": 1,
      "items": {
        "$id": "/properties/citation/items",
        "type": "object",
        "properties": {
          "authors": {
            "$id": "/properties/citation/items/properties/authors",
            "type": "array",
            "minItems": 1,
            "items": {
              "$id": "/properties/citation/items/properties/authors/items",
              "type": "object",
              "properties": {
                "name": {
                  "$id": "/properties/citation/items/properties/authors/items/properties/name",
                  "type": "string",
                  "pattern": "^\\('[^0-9!\"£$%^&*()]+', '[^0-9!\"£$%^&*()]+'\\)$",
                  "title": "The Name Schema ",
                  "description": "Author's surname and given name represented in specified format. Given name is initials as capital letters only. Max length of surname is 60 characters, max length of given name - 20.",
                  "examples": [
                    "('Chang', 'YW')"
                  ]
                },
                "order_id": {
                  "$id": "/properties/citation/items/properties/authors/items/properties/order_id",
                  "type": "integer",
                  "title": "The Order_id Schema ",
                  "default": 0,
                  "description": "Author's order ID in the author array. Starts with zero.",
                  "examples": [
                    0
                  ]
                },
                "author_orcid": {
                  "$id": "/properties/citation/items/properties/authors/items/properties/author_orcid",
                  "type": [
                    "null",
                    "string"
                  ],
                  "pattern": "^([0-9]{4}-){3}[0-9]{3}([0-9]|X)$",
                  "title": "The Author_orcid Schema ",
                  "default": null,
                  "description": "ORCID identifier of the author. Apart from complying with regex it has to satisfy the checksum: https://support.orcid.org/knowledgebase/articles/116780-structure-of-the-orcid-identifier",
                  "examples": [
                    "0000-0003-2391-473X"
                  ]
                }
              },
              "required": [
                "name",
                "order_id"
              ]
            }
          },
          "editors": {
            "$id": "/properties/citation/items/properties/editors",
            "type": "array",
            "items": {
              "$id": "/properties/citation/items/properties/editors/items",
              "type": "object",
              "properties": {
                "name": {
                  "$id": "/properties/citation/items/properties/editors/items/properties/name",
                  "type": "string",
                  "pattern": "^\\('[^0-9!\"£$%^&*()]+', '[^0-9!\"£$%^&*()]+'\\)$",
                  "title": "The Name Schema ",
                  "description": "Editor's surname and given name represented in specified format. Given name is initials as capital letters only. Max length of surname is 60 characters, max length of given name - 20.",
                  "examples": [
                    "('Chang', 'YW')"
                  ]
                },
                "order_id": {
                  "$id": "/properties/citation/items/properties/editors/items/properties/order_id",
                  "type": "integer",
                  "title": "The Order_id Schema ",
                  "default": 0,
                  "description": "Editor's order ID in the editor array. Starts with zero.",
                  "examples": [
                    0
                  ]
                },
                "author_orcid": {
                  "$id": "/properties/citation/items/properties/editors/items/properties/author_orcid",
                  "type": [
                    "null",
                    "string"
                  ],
                  "pattern": "^([0-9]{4}-){3}[0-9]{3}([0-9]|X)$",
                  "title": "The Editor_orcid Schema ",
                  "default": null,
                  "description": "ORCID identifier of the editor. Apart from complying with regex it has to satisfy the checksum: https://support.orcid.org/knowledgebase/articles/116780-structure-of-the-orcid-identifier",
                  "examples": [
                    "0000-0003-2391-473X"
                  ]
                }
              },
              "required": [
                "name",
                "order_id"
              ]
            }
          },
          "published": {
            "$id": "/properties/citation/items/properties/published",
            "type": "boolean",
            "title": "The Published Schema ",
            "default": false,
            "description": "Set to true if citation has been published, false otherwise.",
            "examples": [
              true
            ]
          },
          "published": {
            "$id": "/properties/citation/items/properties/preprint",
            "type": "boolean",
            "title": "The Preprint Schema ",
            "default": false,
            "description": "Set to true if citation is a preprint, false otherwise.",
            "examples": [
              true
            ]
          },
          "j_or_nj_citation": {
            "$id": "/properties/citation/items/properties/j_or_nj_citation",
            "type": "boolean",
            "title": "The J_or_nj_citation Schema ",
            "default": true,
            "description": "Journal (true) or non-journal (false) publication.",
            "examples": [
              true
            ]
          },
          "title": {
            "$id": "/properties/citation/items/properties/title",
            "type": "string",
            "title": "The Title Schema ",
            "maxLength": 255,
            "description": "The title of the citation (article, book, thesis, etc.). It will remain suppressed until the deposition has been released.",
            "examples": [
              "Architecture of the Vibrio cholerae toxin-coregulated pilus machine revealed by electron cryotomography"
            ]
          },
          "volume": {
            "$id": "/properties/citation/items/properties/volume",
            "type": [
              "null",
              "string"
            ],
            "title": "The Volume Schema ",
            "maxLength": 8,
            "default": null,
            "description": "The volume.",
            "examples": [
              "2"
            ]
          },
          "country": {
            "$id": "/properties/citation/items/properties/country",
            "type": [
              "null",
              "string"
            ],
            "title": "The Country Schema ",
            "enum": [
              null,
              "AD",
              "AE",
              "AF",
              "AG",
              "AI",
              "AL",
              "AM",
              "AO",
              "AQ",
              "AR",
              "AS",
              "AT",
              "AU",
              "AW",
              "AZ",
              "BA",
              "BB",
              "BD",
              "BE",
              "BF",
              "BG",
              "BH",
              "BI",
              "BJ",
              "BL",
              "BM",
              "BN",
              "BO",
              "BR",
              "BS",
              "BT",
              "BV",
              "BW",
              "BY",
              "BZ",
              "CA",
              "CC",
              "CD",
              "CF",
              "CG",
              "CH",
              "CI",
              "CK",
              "CL",
              "CM",
              "CN",
              "CO",
              "CR",
              "CU",
              "CV",
              "CW",
              "CX",
              "CY",
              "CZ",
              "DE",
              "DJ",
              "DK",
              "DM",
              "DO",
              "DZ",
              "EC",
              "EE",
              "EG",
              "EH",
              "ER",
              "ES",
              "ET",
              "FI",
              "FJ",
              "FK",
              "FM",
              "FO",
              "FR",
              "FX",
              "GA",
              "GB",
              "GD",
              "GE",
              "GF",
              "GG",
              "GH",
              "GI",
              "GL",
              "GM",
              "GN",
              "GP",
              "GQ",
              "GR",
              "GS",
              "GT",
              "GU",
              "GW",
              "GY",
              "HK",
              "HM",
              "HN",
              "HR",
              "HT",
              "HU",
              "ID",
              "IE",
              "IL",
              "IM",
              "IN",
              "IO",
              "IQ",
              "IR",
              "IS",
              "IT",
              "JE",
              "JM",
              "JO",
              "JP",
              "KE",
              "KG",
              "KH",
              "KI",
              "KM",
              "KN",
              "KP",
              "KR",
              "KW",
              "KY",
              "KZ",
              "LA",
              "LB",
              "LC",
              "LI",
              "LK",
              "LR",
              "LS",
              "LT",
              "LU",
              "LV",
              "LY",
              "MA",
              "MC",
              "MD",
              "ME",
              "MF",
              "MG",
              "MH",
              "MK",
              "ML",
              "MM",
              "MN",
              "MO",
              "MP",
              "MQ",
              "MR",
              "MS",
              "MT",
              "MU",
              "MV",
              "MW",
              "MX",
              "MY",
              "MZ",
              "NA",
              "NC",
              "NE",
              "NF",
              "NG",
              "NI",
              "NL",
              "NO",
              "NP",
              "NR",
              "NU",
              "NZ",
              "OM",
              "PA",
              "PE",
              "PF",
              "PG",
              "PH",
              "PK",
              "PL",
              "PM",
              "PN",
              "PR",
              "PS",
              "PT",
              "PW",
              "PY",
              "QA",
              "RE",
              "RO",
              "RS",
              "RU",
              "RW",
              "SA",
              "SB",
              "SC",
              "SD",
              "SE",
              "SG",
              "SH",
              "SI",
              "SJ",
              "SK",
              "SL",
              "SM",
              "SN",
              "SO",
              "SR",
              "SS",
              "ST",
              "SV",
              "SX",
              "SY",
              "SZ",
              "TC",
              "TD",
              "TF",
              "TG",
              "TH",
              "TJ",
              "TK",
              "TL",
              "TM",
              "TN",
              "TO",
              "TR",
              "TT",
              "TV",
              "TW",
              "TZ",
              "UA",
              "UG",
              "UM",
              "US",
              "UY",
              "UZ",
              "VA",
              "VC",
              "VE",
              "VG",
              "VI",
              "VN",
              "VU",
              "WF",
              "WS",
              "XK",
              "YE",
              "YT",
              "ZA",
              "ZM",
              "ZW"
            ],
            "description": "Country according to ISO 3166-1 alpha-2 standard",
            "examples": [
              "UK"
            ]
          },
          "first_page": {
            "$id": "/properties/citation/items/properties/first_page",
            "type": [
              "null",
              "string"
            ],
            "title": "The First_page Schema ",
            "maxLength": 8,
            "default": null,
            "examples": [
              "16269"
            ]
          },
          "last_page": {
            "$id": "/properties/citation/items/properties/last_page",
            "type": [
              "null",
              "string"
            ],
            "title": "The Last_page Schema ",
            "maxLength": 8,
            "default": null,
            "examples": [
              "16269"
            ]
          },
          "year": {
            "$id": "/properties/citation/items/properties/year",
            "type": [
              "null",
              "integer"
            ],
            "title": "The Year Schema ",
            "minimum": 1000,
            "maximum": 9999,
            "default": null,
            "description": "The year of the publication",
            "examples": [
              2017
            ]
          },
          "language": {
            "$id": "/properties/citation/items/properties/language",
            "type": [
              "null",
              "string"
            ],
            "title": "The Language Schema ",
            "maxLength": 40,
            "default": null,
            "examples": [
              "English"
            ]
          },
          "doi": {
            "$id": "/properties/citation/items/properties/doi",
            "type": [
              "null",
              "string"
            ],
            "pattern": "^10.[0-9]{4,9}/[-._;()/:A-z0-9]+$",
            "title": "The Doi Schema ",
            "maxLength": 80,
            "default": null,
            "description": "The digital object identifier",
            "examples": [
              "10.1038/nmicrobiol.2016.269"
            ]
          },
          "pubmedid": {
            "$id": "/properties/citation/items/properties/pubmedid",
            "type": [
              "null",
              "string"
            ],
            "pattern": "^[0-9]{1,8}((.[0-9]+)?)$",
            "title": "The Pubmedid Schema ",
            "maxLength": 20,
            "default": null,
            "description": "The PubMed unique identifier",
            "examples": [
              "28165453"
            ]
          },
          "details": {
            "$id": "/properties/citation/items/properties/details",
            "type": [
              "null",
              "string"
            ],
            "title": "The Details Schema ",
            "maxLength": 65535,
            "default": null,
            "examples": [
              "Three-dimensional in situ structure of a T4bP machine in its piliated and non-piliated states constructed from its tilt series."
            ]
          },
          "book_chapter_title": {
            "$id": "/properties/citation/items/properties/book_chapter_title",
            "type": [
              "null",
              "string"
            ],
            "title": "The Book_chapter_title Schema ",
            "maxLength": 80,
            "default": null,
            "description": "This field should be filled in only for books. Otherwise, set it to null",
            "examples": [
              null
            ]
          },
          "publisher": {
            "$id": "/properties/citation/items/properties/publisher",
            "type": [
              "null",
              "string"
            ],
            "title": "The Publisher Schema ",
            "maxLength": 80,
            "default": null,
            "description": "This field should be filled in only for books. Otherwise, set it to null",
            "examples": [
              null
            ]
          },
          "publication_location": {
            "$id": "/properties/citation/items/properties/publication_location",
            "type": [
              "null",
              "string"
            ],
            "title": "The Publication_location Schema ",
            "maxLength": 80,
            "default": null,
            "description": "This field should be filled in only for books. Otherwise, set it to null",
            "examples": [
              null
            ]
          },
          "journal": {
            "$id": "/properties/citation/items/properties/journal",
            "type": [
              "null",
              "string"
            ],
            "title": "The Journal Schema ",
            "default": null,
            "description": "This field should be filled in only for journal publications. Otherwise, set it to null",
            "examples": [
              "Nature microbiology"
            ]
          },
          "journal_abbreviation": {
            "$id": "/properties/citation/items/properties/journal_abbreviation",
            "type": [
              "null",
              "string"
            ],
            "title": "The Journal_abbreviation Schema ",
            "default": null,
            "description": "This field should be filled in only for journal publications. Otherwise, set it to null",
            "examples": [
              "Nat Microbiol"
            ]
          },
          "issue": {
            "$id": "/properties/citation/items/properties/issue",
            "type": [
              "null",
              "string"
            ],
            "title": "The Issue Schema ",
            "default": null,
            "description": "This field should be filled in only for journal publications. Otherwise, set it to null",
            "examples": [
              null
            ]
          }
        },
        "required": [
          "authors",
          "published",
          "j_or_nj_citation",
          "title"
        ]
      }
    }
  },
  "required": [
    "title",
    "release_date",
    "experiment_type",
    "authors",
    "corresponding_author",
    "principal_investigator",
    "imagesets",
    "citation"
  ]
}"""