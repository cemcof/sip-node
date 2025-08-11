import json

import jsonschema

d = {
  "title" : "None - sam - EM-TOMO",
  "release_date" : "HP",
  "experiment_type" : 3,
  "cross_references" : [ {
    "name" : "EMD-1001"
  } ],
  "biostudies_references" : [ ],
  "authors" : [ {
    "name" : "('Veverka', 'R')",
    "order_id" : 0,
    "author_orcid" : "0000-0003-4081-655X"
  } ],
  "corresponding_author" : {
    "author_orcid" : "0000-0003-4081-655X",
    "first_name" : "Radek",
    "last_name" : "Veverka",
    "email" : "radek.veverka@icloud.com",
    "organization" : "CEITEC",
    "country" : "CZ"
  },
  "principal_investigator" : [ {
    "author_orcid" : "0000-0003-4081-655X",
    "first_name" : "Radek",
    "last_name" : "Veverka",
    "email" : "radek.veverka@icloud.com",
    "organization" : "CEITEC",
    "country" : "CZ"
  } ],
  "imagesets" : [ {
    "directory" : "data/micrographs",
    "category" : "('T1', '')",
    "header_format" : "('T3', '')",
    "data_format" : "('T3', '')",
    "num_images_or_tilt_series" : 2307,
    "frames_per_image" : 4,
    "voxel_type" : "('T1', '')",
    "name" : "None - sam - EM-TOMO"
  } ],
  "citation" : [ {
    "authors" : [ {
      "name" : "('Veverka', 'R')",
      "order_id" : 0,
      "author_orcid" : "0000-0003-4081-655X"
    } ],
    "published" : False,
    "j_or_nj_citation" : True,
    "title" : "this is pub name"
  } ]
}

with open("empiar-schema.json") as f:
    jsonschema.validate(d, json.load(f))